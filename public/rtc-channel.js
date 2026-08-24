/**
 * rtc-channel.js - WebRTC 通道與信令核心 (負責監聽、握手、心跳與斷線重連)
 */
import { state, getWorkerUrl } from './state.js';
import { showToast, updateStatus, generateQRCode, triggerAppleGlow, acquireWakeLock, releaseWakeLock } from './ui.js';
import { handleIncomingData, createOPFSWorker, cleanupOPFSWorker, updateRecvUi, resetTransferState, setPeerDisconnectHandler } from './webrtc.js';

let localCandidates = [];
let pendingCandidates = [];

// 心跳與看門狗
let heartbeatTimer = null;
let watchdogTimer = null;
let lastActivityTime = Date.now();
let isDisconnectHandled = false;
let transportMonitorTimer = null;

// 斷線重連區
let isReconnecting = false;
let reconnectTimer = null;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 15;

/**
 * 統一的斷線處理器 (支援短暫中斷重連與致命斷線區分)
 */
export function handlePeerDisconnection(reason = '連線已斷開', isFatal = false) {
    if (isDisconnectHandled && !isFatal) return;

    console.warn(`[連線中斷觸發]: ${reason} (isFatal=${isFatal})`);
    stopHeartbeatAndWatchdog();
    stopTransportMonitor();

    // 釋放 DataChannel 與 PeerConnection
    if (state.controlChannel) {
        try { state.controlChannel.close(); } catch (e) { }
        state.controlChannel = null;
    }
    if (state.fileChannel) {
        try { state.fileChannel.close(); } catch (e) { }
        state.fileChannel = null;
    }
    if (state.dataChannel) {
        try { state.dataChannel.close(); } catch (e) { }
        state.dataChannel = null;
    }

    // 重置傳輸內部狀態
    resetTransferState();

    // 僅在有未完成傳輸任務時啟動自動重連
    const isReceivingIncomplete = state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size;
    const isSendingIncomplete = state.selectedFile && state.receivedSize < state.selectedFile.size;
    const hasActiveSession = Boolean(state.roomId && (isReceivingIncomplete || isSendingIncomplete));

    if (!isFatal && hasActiveSession) {
        cleanupOPFSWorker(false, false);
        isReconnecting = false;
        attemptReconnection(reason);
        return;
    }

    // 致命斷線或無傳輸任務，徹底清理
    isDisconnectHandled = true;
    releaseWakeLock();
    cleanupOPFSWorker(false, true);

    updateStatus('連線已中斷', 'red');
    showToast(reason, 'error');

    // 重設 UI
    document.getElementById('transfer-panel')?.classList.add('hidden');
    document.getElementById('signaling-box')?.classList.add('hidden');
    document.getElementById('host-panel')?.classList.add('hidden');
    document.getElementById('joiner-panel')?.classList.add('hidden');
    document.getElementById('setup-view')?.classList.remove('hidden');

    const btnSend = document.getElementById('btn-send');
    if (btnSend) {
        btnSend.disabled = false;
        btnSend.classList.remove('opacity-40', 'cursor-not-allowed');
    }

    // 重設狀態
    state.roomId = null;
    state.incomingFileInfo = null;
    state.receivedSize = 0;
    isReconnecting = false;
}

setPeerDisconnectHandler(handlePeerDisconnection);

/**
 * 自動重連控制器
 */
async function attemptReconnection(reason) {
    if (isReconnecting) return;
    isReconnecting = true;
    reconnectAttempts++;

    if (reconnectAttempts > MAX_RECONNECT_ATTEMPTS) {
        console.error('[自動重連] 超過最大重試次數，宣告連線失敗');
        handlePeerDisconnection('連線逾時且無法自動恢復', true);
        return;
    }

    const backoffDelay = Math.min(1000 * Math.pow(1.3, reconnectAttempts - 1), 5000);
    console.log(`[自動重連] 第 ${reconnectAttempts} 次嘗試，${(backoffDelay / 1000).toFixed(1)} 秒後重敲信令伺服器... 原因: ${reason}`);

    updateStatus(`網路波動，正在自動重連 (第 ${reconnectAttempts} 次)...`, 'yellow');
    showToast(`網路中斷，正在嘗試續傳 (${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})...`, 'warning');

    if (reconnectTimer) clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(async () => {
        try {
            await restartSignalingAndPeerConnection();
        } catch (err) {
            console.warn('[自動重連] 本次握手失敗，排程下次重試:', err);
            isReconnecting = false;
            attemptReconnection(err.message);
        }
    }, backoffDelay);
}

/**
 * 非阻塞式背景 IPv4 探針
 */
export async function probeIPv4() {
    const endpoints = [
        { url: 'https://api4.ipify.org?format=json', type: 'json', key: 'ip' },
        { url: 'https://ipv4.icanhazip.com', type: 'text' }
    ];

    const controllers = [];

    const fetchEndpoint = async (ep) => {
        const controller = new AbortController();
        controllers.push(controller);
        const timeoutId = setTimeout(() => controller.abort(), 2000);

        try {
            const res = await fetch(ep.url, { signal: controller.signal });
            clearTimeout(timeoutId);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);

            let ip = '';
            if (ep.type === 'json') {
                const data = await res.json();
                ip = data[ep.key];
            } else {
                const text = await res.text();
                ip = text.trim();
            }

            if (ip) return ip;
            throw new Error('解析出空 IP');
        } catch (err) {
            clearTimeout(timeoutId);
            throw err;
        }
    };

    try {
        const fastestIp = await Promise.any(endpoints.map(ep => fetchEndpoint(ep)));
        state.localIPv4 = fastestIp;
    } catch (err) {
        console.warn('[IPv4 探針] 所有 API 均無回應或連線失敗');
    } finally {
        controllers.forEach(c => c.abort());
    }
}

/**
 * 發起者流程 (Host)
 */
export async function initHost() {
    localCandidates = [];
    pendingCandidates = [];
    state.isHost = true;
    isDisconnectHandled = false;

    const numPin = Math.floor(100000 + Math.random() * 900000).toString();
    state.roomId = numPin;

    document.getElementById('setup-view').classList.add('hidden');
    document.getElementById('signaling-box').classList.remove('hidden');
    document.getElementById('host-panel').classList.remove('hidden');

    const formattedPin = `${numPin.slice(0, 3)} ${numPin.slice(3)}`;
    const pinDisplay = document.getElementById('host-pin-display');
    if (pinDisplay) {
        pinDisplay.innerText = formattedPin;
    }

    const githubPagesOrigin = window.location.origin + window.location.pathname;
    const joinUrl = `${githubPagesOrigin}?room=${state.roomId}`;
    generateQRCode('qr-canvas', joinUrl);

    try {
        state.peerConnection = new RTCPeerConnection(state.rtcConfig);
        setupPeerConnectionListeners(state.peerConnection);

        // 建立雙並列 P2P 通道
        // 1. 控制通道 (專門處理 Ping/ACK/復原對齊)
        state.controlChannel = state.peerConnection.createDataChannel('controlChannel', { ordered: true });
        setupControlChannelListeners();

        // 2. 檔案通道 (專門處理二進制高頻數據)
        state.fileChannel = state.peerConnection.createDataChannel('fileTransfer', { ordered: true });
        setupFileChannelListeners();

        state.peerConnection.onicecandidate = (event) => {
            if (event.candidate) {
                const payload = {
                    type: 'candidate',
                    candidate: event.candidate,
                    roomId: state.roomId
                };
                if (state.ws && state.ws.readyState === WebSocket.OPEN) {
                    state.ws.send(JSON.stringify(payload));
                } else {
                    localCandidates.push(payload);
                }
            }
        };

        connectSignalingWS();

        const offer = await state.peerConnection.createOffer();
        await state.peerConnection.setLocalDescription(offer);

        const ipKey = state.localIPv4 ? `ipv4:${state.localIPv4}` : null;
        const offerPayload = {
            type: 'offer',
            offer: state.peerConnection.localDescription,
            roomId: state.roomId,
            ipKey: ipKey
        };

        if (state.ws && state.ws.readyState === WebSocket.OPEN) {
            state.ws.send(JSON.stringify(offerPayload));
            updateStatus('信令通道已就緒，等待連線...', 'yellow');
        } else {
            state.pendingOffer = offerPayload;
        }

    } catch (e) {
        console.error(e);
        showToast('初始化失敗: ' + e.message, 'error');
    }
}

/**
 * 加入者流程 (Joiner)
 */
export async function initJoinerWithRoom(roomId) {
    if (roomId) state.roomId = roomId;
    localCandidates = [];
    pendingCandidates = [];
    isDisconnectHandled = false;

    document.getElementById('setup-view').classList.add('hidden');
    document.getElementById('signaling-box').classList.remove('hidden');
    document.getElementById('joiner-panel').classList.remove('hidden');
    updateStatus('正在連接 DO 信令房間...', 'yellow');
    try {
        state.peerConnection = new RTCPeerConnection(state.rtcConfig);
        setupPeerConnectionListeners(state.peerConnection);

        // 依據 channel.label 分流綁定
        state.peerConnection.ondatachannel = (event) => {
            const channel = event.channel;
            if (channel.label === 'controlChannel') {
                state.controlChannel = channel;
                setupControlChannelListeners();
            } else if (channel.label === 'fileTransfer') {
                state.fileChannel = channel;
                setupFileChannelListeners();
            }
        };

        state.peerConnection.onicecandidate = (event) => {
            if (event.candidate) {
                const payload = {
                    type: 'candidate',
                    candidate: event.candidate,
                    roomId: state.roomId
                };
                if (state.ws && state.ws.readyState === WebSocket.OPEN) {
                    state.ws.send(JSON.stringify(payload));
                } else {
                    localCandidates.push(payload);
                }
            }
        };
        connectSignalingWS();
    } catch (e) {
        showToast(e.message, 'error');
        updateStatus('連線建立中斷', 'red');
    }
}

export function connectSignalingWS() {
    const workerUrl = getWorkerUrl();
    const wsUrl = workerUrl.replace(/^http/, 'ws') + `/ws/${state.roomId}`;
    state.ws = new WebSocket(wsUrl);

    state.ws.onopen = () => {
        if (state.pendingOffer) {
            state.ws.send(JSON.stringify(state.pendingOffer));
            state.pendingOffer = null;
            updateStatus('信令通道已就緒，等待連線...', 'yellow');
        }

        while (localCandidates.length > 0) {
            const candMsg = localCandidates.shift();
            state.ws.send(JSON.stringify(candMsg));
        }
    };

    state.ws.onmessage = async (event) => {
        try {
            const data = JSON.parse(event.data);

            if (data.type === 'offer' && !state.isHost) {
                updateStatus('已接收 Offer 信令，正在回應 Answer...', 'yellow');
                await state.peerConnection.setRemoteDescription(new RTCSessionDescription(data.offer));
                await processPendingCandidates();

                const answer = await state.peerConnection.createAnswer();
                await state.peerConnection.setLocalDescription(answer);

                if (state.ws && state.ws.readyState === WebSocket.OPEN) {
                    state.ws.send(JSON.stringify({ type: 'answer', answer, roomId: state.roomId }));
                }
            }
            else if (data.type === 'answer' && state.isHost) {
                updateStatus('成功對齊傳輸描述，建立直連中！', 'yellow');
                await state.peerConnection.setRemoteDescription(new RTCSessionDescription(data.answer));
                await processPendingCandidates();
                showToast('對齊成功，極速握手中...', 'success');
            }
            else if (data.type === 'candidate' && state.peerConnection) {
                if (state.peerConnection.remoteDescription) {
                    await state.peerConnection.addIceCandidate(new RTCIceCandidate(data.candidate));
                } else {
                    pendingCandidates.push(data.candidate);
                }
            }
        } catch (err) {
            console.error("[DO WebSocket 訊息解析失敗]:", err);
        }
    };

    state.ws.onclose = () => {
        console.log("[信令 WebSocket 關閉]");
    };
}

async function processPendingCandidates() {
    while (pendingCandidates.length > 0) {
        const candidate = pendingCandidates.shift();
        try {
            await state.peerConnection.addIceCandidate(new RTCIceCandidate(candidate));
        } catch (e) {
            console.warn("[ICE Candidate Add Error]:", e);
        }
    }
}

/**
 * 重建信令與 WebRTC 連線通道
 */
export async function restartSignalingAndPeerConnection() {
    localCandidates = [];
    pendingCandidates = [];

    state.peerConnection = new RTCPeerConnection(state.rtcConfig);
    setupPeerConnectionListeners(state.peerConnection);

    state.peerConnection.onicecandidate = (event) => {
        if (event.candidate) {
            const payload = {
                type: 'candidate',
                candidate: event.candidate,
                roomId: state.roomId
            };
            if (state.ws && state.ws.readyState === WebSocket.OPEN) {
                state.ws.send(JSON.stringify(payload));
            } else {
                localCandidates.push(payload);
            }
        }
    };

    if (state.isHost) {
        state.controlChannel = state.peerConnection.createDataChannel('controlChannel', { ordered: true });
        setupControlChannelListeners();

        state.fileChannel = state.peerConnection.createDataChannel('fileTransfer', { ordered: true });
        setupFileChannelListeners();

        connectSignalingWS();

        const offer = await state.peerConnection.createOffer();
        await state.peerConnection.setLocalDescription(offer);

        const ipKey = state.localIPv4 ? `ipv4:${state.localIPv4}` : null;
        const offerPayload = {
            type: 'offer',
            offer: state.peerConnection.localDescription,
            roomId: state.roomId,
            ipKey: ipKey
        };

        if (state.ws && state.ws.readyState === WebSocket.OPEN) {
            state.ws.send(JSON.stringify(offerPayload));
        } else {
            state.pendingOffer = offerPayload;
        }
    } else {
        state.peerConnection.ondatachannel = (event) => {
            const channel = event.channel;
            if (channel.label === 'controlChannel') {
                state.controlChannel = channel;
                setupControlChannelListeners();
            } else if (channel.label === 'fileTransfer') {
                state.fileChannel = channel;
                setupFileChannelListeners();
            }
        };
        connectSignalingWS();
    }
}

/**
 * 設定控制通道 (controlChannel) 監聽
 */
export function setupControlChannelListeners() {
    if (!state.controlChannel) return;

    state.controlChannel.onopen = () => {
        console.log('[P2P 控制通道] 已開啟');
        isDisconnectHandled = false;
        isReconnecting = false;
        reconnectAttempts = 0;
        lastActivityTime = Date.now();

        updateStatus('P2P 控制與傳輸通道已開通', 'green');
        showToast('P2P 直連通道就緒', 'success');
        triggerAppleGlow();

        startHeartbeatAndWatchdog();
        startTransportMonitor();

        // 發送裝置資訊
        state.controlChannel.send(JSON.stringify({
            type: 'device-info',
            isMobile: state.localIsMobile
        }));

        // 關閉信令 WS 節省資源
        if (state.ws) {
            state.ws.close();
            state.ws = null;
        }

        document.getElementById('setup-view')?.classList.add('hidden');
        document.getElementById('signaling-box')?.classList.add('hidden');
        const tfPanel = document.getElementById('transfer-panel');
        if (tfPanel) {
            tfPanel.classList.remove('hidden');
            tfPanel.style.opacity = '1';
        }

        // 若有續傳任務，經由 controlChannel 協商
        if (state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size) {
            document.getElementById('incoming-file-box')?.classList.remove('hidden');
            const recvFileName = document.getElementById('recv-file-name');
            if (recvFileName) recvFileName.innerText = state.incomingFileInfo.name;
            updateRecvUi();

            createOPFSWorker(true);
            state.controlChannel.send(JSON.stringify({
                type: 'resume-request',
                name: state.incomingFileInfo.name,
                receivedOffset: state.receivedSize
            }));
        }
    };

    state.controlChannel.onmessage = (event) => {
        lastActivityTime = Date.now();
        // 控制訊息傳入 webrtc.js 處理
        handleIncomingData(event.data, 'control');
    };

    state.controlChannel.onclose = () => {
        console.warn('[P2P 控制通道] 關閉');
        handlePeerDisconnection('控制通道中斷', false);
    };

    state.controlChannel.onerror = (err) => {
        console.error('[P2P 控制通道 異常]:', err);
    };
}

/**
 * 設定檔案傳輸通道 (fileChannel) 監聽
 */
export function setupFileChannelListeners() {
    if (!state.fileChannel) return;

    state.fileChannel.binaryType = 'arraybuffer';
    state.fileChannel.onmessage = (event) => {
        handleIncomingData(event.data, 'file');
    };

    state.fileChannel.onclose = () => {
        console.warn('[P2P 檔案通道] 關閉');
    };

    state.fileChannel.onerror = (err) => {
        console.error('[P2P 檔案通道 異常]:', err);
    };
}

/**
 * 優先經由 P2P 控制線進行修復，避免直接對 DO 信令發起連線
 */
export function triggerFileChannelRecovery(reason) {
    if (state.controlChannel && state.controlChannel.readyState === 'open') {
        console.log(`[P2P 內生修復] 理由: ${reason}。正在透過控制通道請求對端重新對齊與續傳...`);
        showToast('檔案通道波動，正在自動無感修復...', 'warning');
        
        // 通知對端重新發送/續傳
        state.controlChannel.send(JSON.stringify({
            type: 'file-resync-request',
            receivedOffset: state.receivedSize || 0
        }));
    } else {
        // 連控制線都斷了才觸發完整重連 (打 DO 信令)
        handlePeerDisconnection(reason, false);
    }
}

function setupPeerConnectionListeners(pc) {
    if (!pc) return;

    pc.onconnectionstatechange = () => {
        console.log(`[WebRTC 連線狀態變更]: ${pc.connectionState}`);

        if (pc.connectionState === 'disconnected') {
            console.warn('[WebRTC 狀態] 連線短暫中斷 (disconnected)，等待底層 ICE 復原...');
            updateStatus('網路連線波動中，等待恢復...', 'yellow');
        } else if (pc.connectionState === 'failed') {
            handlePeerDisconnection('對端連線狀態失敗 (failed)', false);
        } else if (pc.connectionState === 'closed') {
            handlePeerDisconnection('對端連線已關閉 (closed)', false);
        } else if (pc.connectionState === 'connected') {
            updateStatus('P2P 直連已開通 (加密)', 'green');
        }
    };

    pc.oniceconnectionstatechange = () => {
        console.log(`[ICE 狀態變更]: ${pc.iceConnectionState}`);

        if (pc.iceConnectionState === 'disconnected') {
            console.warn('[ICE 狀態] ICE 暫時中斷，等待重連...');
        } else if (pc.iceConnectionState === 'failed') {
            handlePeerDisconnection('ICE 連線失敗 (failed)', false);
        } else if (pc.iceConnectionState === 'closed') {
            handlePeerDisconnection('ICE 連線已關閉 (closed)', false);
        }
    };
}

function startTransportMonitor() {
    stopTransportMonitor();

    transportMonitorTimer = setInterval(async () => {
        const pc = state.peerConnection;
        const dc = state.fileChannel;
        if (!pc) return;

        try {
            await logWebRTCStats(pc, dc);
        } catch (err) {
            console.warn('[WebRTC Stats 監控失敗]', err);
        }
    }, 1000);
}

function stopTransportMonitor() {
    if (transportMonitorTimer) {
        clearInterval(transportMonitorTimer);
        transportMonitorTimer = null;
    }
}

function startHeartbeatAndWatchdog() {
    stopHeartbeatAndWatchdog();

    isDisconnectHandled = false;
    lastActivityTime = Date.now();

    let lastStatsBytesSent = 0;
    let lastStatsBytesReceived = 0;

    heartbeatTimer = setInterval(() => {
        if (state.controlChannel && state.controlChannel.readyState === 'open') {
            const isTransferring =
                (state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size) ||
                (state.selectedFile && state.receivedSize < state.selectedFile.size);

            const idleTime = Date.now() - lastActivityTime;

            if (!isTransferring || idleTime > 3000) {
                try {
                    state.controlChannel.send(JSON.stringify({ type: 'ping' }));
                } catch (e) {
                    console.warn('[Heartbeat] Ping 發送失敗:', e);
                }
            }
        }
    }, 2000);

    watchdogTimer = setInterval(async () => {
        const now = Date.now();
        const isReceiving = state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size;
        const isSending = state.selectedFile && state.receivedSize < state.selectedFile.size;

        if (!isReceiving && !isSending) return;

        // 檢查控制通道
        if (!state.controlChannel || state.controlChannel.readyState !== 'open') return;

        try {
            const stats = await state.peerConnection?.getStats();
            let currentBytesSent = 0;
            let currentBytesReceived = 0;
            stats?.forEach(report => {
                if (report.type === 'candidate-pair' && report.state === 'succeeded') {
                    currentBytesSent = Math.max(currentBytesSent, report.bytesSent || 0);
                    currentBytesReceived = Math.max(currentBytesReceived, report.bytesReceived || 0);
                }
            });
            const bytesSentChanged = currentBytesSent > lastStatsBytesSent;
            const bytesReceivedChanged = currentBytesReceived > lastStatsBytesReceived;
            if (bytesSentChanged || bytesReceivedChanged) {
                lastActivityTime = now;
            }
            lastStatsBytesSent = currentBytesSent;
            lastStatsBytesReceived = currentBytesReceived;
        } catch (e) { }

        const effectiveIdleTime = Date.now() - lastActivityTime;

        if (effectiveIdleTime > 15000) {
            console.error(`[看門狗逾時] 傳輸中超過 ${(effectiveIdleTime / 1000).toFixed(1)} 秒無回應，優先嘗試 P2P 控制通道內生恢復...`);
            triggerFileChannelRecovery('傳輸逾時無回應');
        }
    }, 1000);
}

function stopHeartbeatAndWatchdog() {
    if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
    }
    if (watchdogTimer) {
        clearInterval(watchdogTimer);
        watchdogTimer = null;
    }
}

async function logWebRTCStats(pc, dc) {
    if (!pc) return;

    const stats = await pc.getStats();

    let transport = null;
    let candidatePair = null;
    let dataChannelStats = null;

    stats.forEach(report => {
        if (report.type === 'transport') transport = report;
        if (report.type === 'candidate-pair' && report.state === 'succeeded') candidatePair = report;
        if (report.type === 'data-channel') dataChannelStats = report;
    });

    console.log('[WebRTC Stats]', {
        dcState: dc?.readyState,
        bufferedKB: dc ? (dc.bufferedAmount / 1024).toFixed(1) : 'N/A',
        pcState: pc.connectionState,
        iceState: pc.iceConnectionState,
        pairState: candidatePair?.state,
        pairBytesSent: candidatePair?.bytesSent,
        pairBytesReceived: candidatePair?.bytesReceived,
        dcBytesSent: dataChannelStats?.bytesSent,
        dcBytesReceived: dataChannelStats?.bytesReceived,
        dcMessagesSent: dataChannelStats?.messagesSent,
        dcMessagesReceived: dataChannelStats?.messagesReceived
    });
}

// 視窗卸載離線告警
window.addEventListener('beforeunload', () => {
    if (state.controlChannel && state.controlChannel.readyState === 'open') {
        try {
            state.controlChannel.send(JSON.stringify({ type: 'peer-leave' }));
        } catch (e) { }
    }
});

window.addEventListener('pagehide', () => {
    if (state.controlChannel && state.controlChannel.readyState === 'open') {
        try {
            state.controlChannel.send(JSON.stringify({ type: 'peer-leave' }));
        } catch (e) { }
    }
});