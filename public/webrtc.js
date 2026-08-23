/**
 * webrtc.js - WebRTC 直連通道與信令核心 (OPFS Web Worker 重構版 - 支援超大檔案 2GB+ 與累積確認 Cumulative ACK)
 */
import { state, getWorkerUrl } from './state.js';
import { showToast, updateStatus, generateQRCode, triggerAutoDownload, triggerAppleGlow, acquireWakeLock, releaseWakeLock } from './ui.js';

let currentDownloadId = null;
let currentTempFilename = null; // 記錄當前正在接收的暫存檔名
let opfsWorker = null;
let currentOffset = 0;

let localCandidates = [];
let pendingCandidates = [];

let recvQueue = [];
let isWorkerReady = false;
let fileEndPending = false;
let inFlightBytes = 0;
const MAX_IN_FLIGHT_BYTES = 16 * 1024 * 1024; // 16MB 接收並發窗口

let totalSent = 0;
let bufferLowResolver = null;
const BUFFERED_LOW_THRESHOLD = 8 * 1024 * 1024; // 8MB 累積緩存低水位
const BUFFERED_HIGH_THRESHOLD = 32 * 1024 * 1024; // 32MB 累積緩存高水位

// 接收端累積確認 (Cumulative ACK) 機制
let lastAckedOffset = 0;
let ackFlushTimer = null;
const BATCH_ACK_INTERVAL = 2 * 1024 * 1024; // 每累積寫入 2MB 即發送一次確認

// 心跳與看門狗
let heartbeatTimer = null;
let watchdogTimer = null;
let lastActivityTime = Date.now();
let isDisconnectHandled = false;

// log監控區
let recvPacketCount = 0;
let workerAckCount = 0;
let transportMonitorTimer = null;

let isFinishSent = false; // 防止重複觸發 finish

// 斷線重連區
let isReconnecting = false;
let reconnectTimer = null;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 15;

let isSendingInProgress = false; // 防止發送端重複進入發送迴圈
let lastConfirmedAckOffset = 0;  // 發送端記錄對端已寫入落盤的確認點
let currentTransferSessionId = 0; // 用於杜絕多個發送迴圈並行競爭

// 獨立輔助函式：由主線程直接刪除指定 OPFS 檔案
async function removeOPFSFile(filename) {
    if (!filename || !navigator.storage || !navigator.storage.getDirectory) return;
    try {
        const rootDir = await navigator.storage.getDirectory();
        await rootDir.removeEntry(filename);
        console.log(`[OPFS 清理] 暫存檔已安全刪除: ${filename}`);
    } catch (e) {
        // 檔案若已刪除或不存在則忽略
    }
}

// 接收端發送累積確認 (Cumulative ACK)
function flushCumulativeAck() {
    if (state.receivedSize > lastAckedOffset && state.dataChannel && state.dataChannel.readyState === 'open') {
        try {
            state.dataChannel.send(JSON.stringify({
                type: 'ack',
                offset: state.receivedSize
            }));
            lastAckedOffset = state.receivedSize;
        } catch (e) { }
    }
}

/**
 * 統一的斷線處理器 (支援短暫中斷重連與致命斷線區分)
 * @param {string} reason 斷線原因
 * @param {boolean} isFatal 是否為致命斷線 (如對端主動離線/使用者手動取消)
 */
export function handlePeerDisconnection(reason = '連線已斷開', isFatal = false) {
    if (isDisconnectHandled && !isFatal) return;

    console.warn(`[連線中斷觸發]: ${reason} (isFatal=${isFatal})`);
    stopHeartbeatAndWatchdog();
    stopTransportMonitor();

    // 釋放 DataChannel 與 PeerConnection
    if (state.dataChannel) {
        try { state.dataChannel.close(); } catch (e) { }
        state.dataChannel = null;
    }
    if (state.peerConnection) {
        try { state.peerConnection.close(); } catch (e) { }
        state.peerConnection = null;
    }
    if (state.ws) {
        try { state.ws.close(); } catch (e) { }
        state.ws = null;
    }

    // 中斷當前所有正在等待 buffer 的 promise
    if (bufferLowResolver) {
        const resolve = bufferLowResolver;
        bufferLowResolver = null;
        resolve();
    }

    // 中斷發送迴圈
    currentTransferSessionId++;
    isSendingInProgress = false;

    // 清空接收端尚未寫入的隊列，防止續傳時舊 chunk 錯位寫入
    recvQueue = [];
    inFlightBytes = 0;

    // 僅在有未完成傳輸任務時啟動自動重連，避免傳輸完成後因對端離線觸發無效重試
    const isReceivingIncomplete = state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size;
    const isSendingIncomplete = state.selectedFile && totalSent < state.selectedFile.size;
    const hasActiveSession = Boolean(state.roomId && (isReceivingIncomplete || isSendingIncomplete));

    if (!isFatal && hasActiveSession) {
        cleanupOPFSWorker(false, false); // 非致命中斷：終止 Worker，但保留暫存檔案 (Bug 5)
        isReconnecting = false; // 重設標記以允許進入重試邏輯
        attemptReconnection(reason);
        return;
    }

    // 致命斷線或無傳輸任務，徹底清理
    isDisconnectHandled = true;
    releaseWakeLock();
    cleanupOPFSWorker(false, true); // 致命斷線：徹底刪除暫存檔案

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
    totalSent = 0;
    isReconnecting = false;
}

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
 * 建立並初始化 OPFS Worker (支援全新寫入與斷點續傳)
 * @param {boolean} isResume 是否為續傳模式
 */
function createOPFSWorker(isResume = false) {
    if (opfsWorker) {
        try { opfsWorker.terminate(); } catch (e) { }
        opfsWorker = null;
    }

    isWorkerReady = false;
    isFinishSent = false;
    fileEndPending = false;

    try {
        opfsWorker = new Worker('./opfs-worker.js');
    } catch (err) {
        console.error('[接收端] 建立 OPFS Worker 失敗:', err);
        showToast('無法建立 OPFS Worker: ' + err.message, 'error');
        releaseWakeLock();
        return false;
    }

    opfsWorker.onmessage = (e) => {
        const msg = e.data;
        if (!msg) return;

        if (msg.type === 'init-success') {
            console.log(`[接收端] OPFS Worker 就緒 (${isResume ? '續傳' : '全新'})`);
            isWorkerReady = true;
            drainRecvQueue();
        }
        else if (msg.type === 'write-ack') {
            workerAckCount++;
            const writtenSize = msg.size || 0;
            inFlightBytes = Math.max(0, inFlightBytes - writtenSize);
            state.receivedSize += writtenSize;
            lastActivityTime = Date.now();

            // 超過累積限制立即觸發 ACK 發送
            if (state.receivedSize - lastAckedOffset >= BATCH_ACK_INTERVAL) {
                flushCumulativeAck();
            }

            updateRecvUi();
            drainRecvQueue();
        }
        else if (msg.type === 'finish-success') {
            releaseWakeLock();
            flushCumulativeAck();

            const { file, filename, tempFilename } = msg;
            const fileToDelete = tempFilename || currentTempFilename;
            const blobUrl = URL.createObjectURL(file);

            console.log(`[接收端 傳輸完成] 檔案大小: ${(file.size / (1024 * 1024)).toFixed(2)} MB`);

            cleanupOPFSWorker(true, false);

            const actualFileSize = state.incomingFileInfo ? state.incomingFileInfo.size : (file ? file.size : 0);
            if (typeof triggerAutoDownload === 'function') {
                triggerAutoDownload(blobUrl, filename, actualFileSize);
            }

            const recvPercent = document.getElementById('recv-percent');
            const recvProgressBar = document.getElementById('recv-progress-bar');
            if (recvPercent) recvPercent.innerText = '100% (完成)';
            if (recvProgressBar) recvProgressBar.style.width = '100%';
            showToast(`「${state.incomingFileInfo ? state.incomingFileInfo.name : filename}」傳輸成功！`, 'success');

            setTimeout(async () => {
                URL.revokeObjectURL(blobUrl);
                if (fileToDelete) {
                    await removeOPFSFile(fileToDelete);
                }
            }, 15000);
        }
        else if (msg.type === 'error') {
            releaseWakeLock();
            console.error('[接收端] Worker 錯誤:', msg.message);
            handlePeerDisconnection('接收端寫入錯誤: ' + msg.message, true);
        }
    };

    opfsWorker.onerror = (err) => {
        releaseWakeLock();
        console.error('[接收端 OPFS Worker 異常]:', err);
        handlePeerDisconnection('OPFS Worker 內部錯誤', true);
    };

    opfsWorker.postMessage({
        type: isResume ? 'init-resume' : 'init',
        downloadId: currentDownloadId,
        tempFilename: currentTempFilename,
        filename: state.incomingFileInfo?.name || 'downloaded_file',
        size: state.incomingFileInfo?.size || 0
    });

    return true;
}

/**
 * 重建信令與 WebRTC 連線通道
 */
async function restartSignalingAndPeerConnection() {
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
        state.dataChannel = state.peerConnection.createDataChannel('fileTransfer', { ordered: true });
        setupDataChannelListeners();

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
            state.dataChannel = event.channel;
            setupDataChannelListeners();
        };
        connectSignalingWS();
    }
}

function startTransportMonitor() {
    stopTransportMonitor();

    transportMonitorTimer = setInterval(async () => {
        const pc = state.peerConnection;
        const dc = state.dataChannel;

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

// 啟動心跳探針與傳輸看門狗
function startHeartbeatAndWatchdog() {
    stopHeartbeatAndWatchdog();

    isDisconnectHandled = false;
    lastActivityTime = Date.now();

    let lastStatsBytesSent = 0;
    let lastStatsBytesReceived = 0;

    heartbeatTimer = setInterval(() => {
        if (state.dataChannel && state.dataChannel.readyState === 'open') {
            const isTransferring =
                (state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size) ||
                (state.selectedFile && totalSent < state.selectedFile.size);

            const idleTime = Date.now() - lastActivityTime;

            if (!isTransferring || idleTime > 3000) {
                try {
                    state.dataChannel.send(JSON.stringify({ type: 'ping' }));
                } catch (e) {
                    console.warn('[Heartbeat] Ping 發送失敗:', e);
                }
            }
        }
    }, 2000);

    watchdogTimer = setInterval(async () => {
        const now = Date.now();

        const isReceiving = state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size;
        const isSending = state.selectedFile && totalSent < state.selectedFile.size;

        if (!isReceiving && !isSending) return;

        if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
            return;
        }

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

        // 傳輸中超過 15 秒無資料流動即判定為卡死，觸發重連
        if (effectiveIdleTime > 15000) {
            console.error(`[看門狗逾時] 傳輸中超過 ${(effectiveIdleTime / 1000).toFixed(1)} 秒無回應，嘗試重新連線續傳...`);
            handlePeerDisconnection('傳輸逾時，正在重新連線...', false);
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

// 設定 PeerConnection 連線與 ICE 狀態監聽
function setupPeerConnectionListeners(pc) {
    if (!pc) return;

    pc.onconnectionstatechange = () => {
        console.log(`[WebRTC 連線狀態變更]: ${pc.connectionState}`);

        if (pc.connectionState === 'disconnected') {
            // 短暫波動：等待 ICE 自動復原，不立刻銷毀
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

async function logWebRTCStats(pc, dc) {
    if (!pc) return;

    const stats = await pc.getStats();

    let transport = null;
    let candidatePair = null;
    let dataChannelStats = null;

    stats.forEach(report => {
        if (report.type === 'transport') {
            transport = report;
        }

        if (
            report.type === 'candidate-pair' &&
            report.state === 'succeeded'
        ) {
            candidatePair = report;
        }

        if (report.type === 'data-channel') {
            dataChannelStats = report;
        }
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

// 監聽視窗卸載主動向對端告警
window.addEventListener('beforeunload', () => {
    if (state.dataChannel && state.dataChannel.readyState === 'open') {
        try {
            state.dataChannel.send(JSON.stringify({ type: 'peer-leave' }));
        } catch (e) { }
    }
});

window.addEventListener('pagehide', () => {
    if (state.dataChannel && state.dataChannel.readyState === 'open') {
        try {
            state.dataChannel.send(JSON.stringify({ type: 'peer-leave' }));
        } catch (e) { }
    }
});

// 非阻塞式背景 IPv4 探針
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

// 發起者流程 (Host)
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

        state.dataChannel = state.peerConnection.createDataChannel('fileTransfer', { ordered: true });
        setupDataChannelListeners();

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

function connectSignalingWS() {
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

        state.peerConnection.ondatachannel = (event) => {
            state.dataChannel = event.channel;
            setupDataChannelListeners();
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

/**
 * 設定 DataChannel 事件監聽 (更新：通道建立後自動重建 Worker 並發起續傳協商)
 */
export function setupDataChannelListeners() {
    state.dataChannel.binaryType = 'arraybuffer';
    state.dataChannel.bufferedAmountLowThreshold = BUFFERED_LOW_THRESHOLD;

    state.dataChannel.onbufferedamountlow = () => {
        if (bufferLowResolver) {
            const resolve = bufferLowResolver;
            bufferLowResolver = null;
            resolve();
        }
    };

    state.dataChannel.onopen = () => {
        isDisconnectHandled = false;
        isReconnecting = false;
        reconnectAttempts = 0;
        lastActivityTime = Date.now();

        // 清除任何殘存的非同步背壓等待
        if (bufferLowResolver) {
            const resolve = bufferLowResolver;
            bufferLowResolver = null;
            resolve();
        }

        updateStatus('P2P 直連已開通 (加密)', 'green');
        showToast('P2P 直連通道就緒', 'success');
        triggerAppleGlow();

        startHeartbeatAndWatchdog();
        startTransportMonitor();

        // 啟動 ACK 強制刷新定時器 (每 200ms) 作為流控兜底
        if (ackFlushTimer) clearInterval(ackFlushTimer);
        ackFlushTimer = setInterval(() => {
            flushCumulativeAck();
        }, 200);

        state.dataChannel.send(JSON.stringify({
            type: 'device-info',
            isMobile: state.localIsMobile
        }));

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

        // 續傳協商：若身為接收端且有未完成檔案，重整 Offset、重建 Worker 並主動請求續傳
        if (state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size) {
            recvQueue = [];
            inFlightBytes = 0;
            currentOffset = state.receivedSize;

            document.getElementById('incoming-file-box')?.classList.remove('hidden');
            const recvFileName = document.getElementById('recv-file-name');
            if (recvFileName) recvFileName.innerText = state.incomingFileInfo.name;
            updateRecvUi();

            console.log(`[續傳協商] 接收端重新建立 OPFS Worker (續傳模式) 並請求從 Offset ${state.receivedSize} 續傳`);
            createOPFSWorker(true);

            state.dataChannel.send(JSON.stringify({
                type: 'resume-request',
                name: state.incomingFileInfo.name,
                receivedOffset: state.receivedSize
            }));
        }
    };

    state.dataChannel.onclose = () => {
        handlePeerDisconnection('直連通道中斷', false);
    };

    state.dataChannel.onerror = (err) => {
        console.error('[DataChannel 異常]:', err);
        handlePeerDisconnection('傳輸通道異常', false);
    };

    state.dataChannel.onmessage = (event) => {
        lastActivityTime = Date.now();
        handleIncomingData(event.data);
    };
}

function updateRecvUi() {
    if (state.incomingFileInfo && state.incomingFileInfo.size > 0) {
        const now = Date.now();
        if (now - state.lastRecvUiTime > 80 || state.receivedSize >= state.incomingFileInfo.size) {
            state.lastRecvUiTime = now;
            const percent = Math.min(100, Math.round((state.receivedSize / state.incomingFileInfo.size) * 100));

            const recvPercent = document.getElementById('recv-percent');
            const recvProgressBar = document.getElementById('recv-progress-bar');
            if (recvPercent) recvPercent.innerText = `${percent}%`;
            if (recvProgressBar) recvProgressBar.style.width = `${percent}%`;
        }
    }
}

function drainRecvQueue() {
    if (!isWorkerReady || !opfsWorker) return;

    while (recvQueue.length > 0 && inFlightBytes < MAX_IN_FLIGHT_BYTES) {
        const item = recvQueue.shift();
        const chunkSize = item.byteLength || (item.buffer ? item.buffer.byteLength : 0);
        if (chunkSize === 0) continue;

        inFlightBytes += chunkSize;

        let bufferToTransfer = null;
        if (item instanceof ArrayBuffer) {
            bufferToTransfer = item;
        } else if (ArrayBuffer.isView(item)) {
            bufferToTransfer = item.buffer.slice(item.byteOffset, item.byteOffset + item.byteLength);
        }

        if (bufferToTransfer) {
            try {
                opfsWorker.postMessage({
                    type: 'write',
                    chunk: bufferToTransfer,
                    offset: currentOffset,
                    size: chunkSize
                }, [bufferToTransfer]);
            } catch (err) {
                opfsWorker.postMessage({
                    type: 'write',
                    chunk: bufferToTransfer,
                    offset: currentOffset,
                    size: chunkSize
                });
            }

            currentOffset += chunkSize;
        }
    }

    if (inFlightBytes >= MAX_IN_FLIGHT_BYTES) {
        flushCumulativeAck();
    }

    if (recvQueue.length > 100) {
        console.warn(`[接收端 佇列積壓] 隊列待處理封包: ${recvQueue.length} 個 | Worker 寫入中記憶體 (inFlight): ${(inFlightBytes / 1024).toFixed(1)} KB`);
    }

    const isTotalReceived = state.incomingFileInfo && state.receivedSize >= state.incomingFileInfo.size;
    if ((fileEndPending || isTotalReceived) && recvQueue.length === 0 && inFlightBytes === 0 && opfsWorker && !isFinishSent) {
        console.log(`[接收端 準備收尾] 總接收已達標 (${(state.receivedSize / (1024 * 1024)).toFixed(2)} MB)，通知 Worker 執行 Finish...`);
        isFinishSent = true;
        fileEndPending = false;
        opfsWorker.postMessage({ type: 'finish' });
    }
}

export async function handleIncomingData(data) {
    if (typeof data === 'string') {
        try {
            const message = JSON.parse(data);

            if (message.type === 'ping') {
                if (state.dataChannel && state.dataChannel.readyState === 'open') {
                    state.dataChannel.send(JSON.stringify({ type: 'pong' }));
                }
                return;
            }

            if (message.type === 'pong') {
                lastActivityTime = Date.now();
                return;
            }

            if (message.type === 'peer-leave') {
                console.warn('[接收端] 收到對端主動離線通知');
                handlePeerDisconnection('對端已主動離開房間', true);
                return;
            }

            if (message.type === 'ack') {
                lastActivityTime = Date.now();
                if (typeof message.offset === 'number') {
                    lastConfirmedAckOffset = message.offset;
                }
                return;
            }

            // 發送端收到續傳請求
            if (message.type === 'resume-request') {
                console.log(`[發送端] 收到續傳請求，對端已完成: ${(message.receivedOffset / (1024 * 1024)).toFixed(2)} MB`);
                showToast(`正在從 ${(message.receivedOffset / (1024 * 1024)).toFixed(1)} MB 處恢復傳輸...`, 'info');
                
                state.dataChannel.send(JSON.stringify({
                    type: 'resume-ack',
                    startOffset: message.receivedOffset
                }));

                // 啟動新的傳送任務（內部會遞增 Session ID 中斷舊任務）
                sendFileChunks(message.receivedOffset);
                return;
            }

            // 接收端收到續傳確認
            if (message.type === 'resume-ack') {
                console.log(`[接收端] 發送端確認續傳 Offset: ${message.startOffset}`);
                currentOffset = message.startOffset;
                // 若 Worker 尚未建立 (兜底防護)，則立即建立
                if (!opfsWorker) {
                    createOPFSWorker(true);
                } else if (isWorkerReady) {
                    drainRecvQueue();
                }
                return;
            }

            if (message.type === 'file-meta') {
                acquireWakeLock();
                recvPacketCount = 0;
                workerAckCount = 0;

                const isSameFile = state.incomingFileInfo &&
                    state.incomingFileInfo.name === message.name &&
                    state.incomingFileInfo.size === message.size &&
                    currentTempFilename;

                if (!isSameFile) {
                    cleanupOPFSWorker(false, true);
                    currentDownloadId = `dl_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
                    currentTempFilename = `temp_${currentDownloadId}.tmp`;
                    state.receivedSize = 0;
                    currentOffset = 0;
                    lastAckedOffset = 0;
                }

                state.incomingFileInfo = { name: message.name, size: message.size };
                state.lastRecvUiTime = 0;
                recvQueue = [];
                inFlightBytes = 0;

                console.log(`[接收端] 收到檔案資訊: 檔名="${message.name}", 總大小=${(message.size / (1024 * 1024)).toFixed(2)} MB, 目前進度=${(state.receivedSize / (1024 * 1024)).toFixed(2)} MB`);

                const isResume = state.receivedSize > 0;
                createOPFSWorker(isResume);

                if (isResume) {
                    state.dataChannel.send(JSON.stringify({
                        type: 'resume-request',
                        name: message.name,
                        receivedOffset: state.receivedSize
                    }));
                }

                document.getElementById('incoming-file-box')?.classList.remove('hidden');
                const recvFileName = document.getElementById('recv-file-name');
                if (recvFileName) recvFileName.innerText = state.incomingFileInfo.name;
                updateRecvUi();

            } else if (message.type === 'file-end') {
                console.log(`[接收端] 收到 file-end 信號 (已收 ${recvPacketCount} 包, 寫入 ${workerAckCount} 次)`);
                fileEndPending = true;
                drainRecvQueue();
            }
        } catch (e) {
            console.error('[接收端] 解析控制訊息失敗:', e);
        }
    } else {
        recvPacketCount++;
        recvQueue.push(data);
        lastActivityTime = Date.now();

        if (isWorkerReady) {
            drainRecvQueue();
        }
    }
}

export function evaluateFileRouting(file) {
    const btnSend = document.getElementById('btn-send');
    if (!btnSend) return;
    btnSend.disabled = false;
    btnSend.classList.remove('opacity-40', 'cursor-not-allowed');
    document.getElementById('progress-status').innerText = '隨時可以發送 (WebRTC P2P 直連)';
    document.getElementById('progress-bar').className = "bg-gradient-to-r from-blue-500 to-emerald-400 h-1 w-0 transition-all duration-75";
    showToast('已啟用 WebRTC P2P 直連傳輸', 'success');
}

/**
 * 發送檔案分塊 (高效雙水位管線 + 2MB 預讀塊極速重構版)
 * @param {number|null} startOffset 指定開始傳輸的 Offset (續傳使用)
 */
export async function sendFileChunks(startOffset = null) {
    if (!state.selectedFile) {
        showToast('未選擇檔案', 'error');
        return;
    }

    if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
        console.warn('[發送端] 通道尚未就緒，等待連線建立...');
        return;
    }

    // 生成當前 Session ID，使舊的異步發送迴圈自動退場
    const mySessionId = ++currentTransferSessionId;
    isSendingInProgress = true;

    const btnSend = document.getElementById('btn-send');
    if (btnSend) {
        btnSend.disabled = true;
        btnSend.classList.add('opacity-40', 'cursor-not-allowed');
    }

    await acquireWakeLock();

    let offset = typeof startOffset === 'number' ? startOffset : 0;
    totalSent = offset;
    lastConfirmedAckOffset = offset;

    if (offset === 0) {
        console.log(`[發送端 開始新傳輸] 檔名: ${state.selectedFile.name}, 總大小: ${(state.selectedFile.size / (1024 * 1024)).toFixed(2)} MB`);
        state.dataChannel.send(JSON.stringify({
            type: 'file-meta',
            name: state.selectedFile.name,
            size: state.selectedFile.size
        }));
    } else {
        console.log(`[發送端 斷點續傳] Session #${mySessionId} 從 ${(offset / (1024 * 1024)).toFixed(2)} MB 處接續傳輸`);
    }

    const CHUNK_SIZE = 256 * 1024;        // 256KB SCTP 最優傳輸封包
    const BLOCK_SIZE = 8 * 1024 * 1024;  // 8MB 記憶體預讀塊 (大幅降低 Blob I/O 呼叫頻率)
    bufferLowResolver = null;

    let lastUiUpdateTime = 0;
    let lastSpeedTime = performance.now();
    let lastSpeedBytes = offset;
    let smoothSpeed = 0;

    const formatEta = (seconds) => {
        if (!isFinite(seconds) || seconds <= 0) return '--:--';
        seconds = Math.ceil(seconds);
        if (seconds < 60) return `00:${String(seconds).padStart(2, '0')}`;
        const minutes = Math.floor(seconds / 60);
        const secs = seconds % 60;
        if (minutes < 60) return `${String(minutes).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
        const hours = Math.floor(minutes / 60);
        const mins = minutes % 60;
        return `${String(hours).padStart(2, '0')}:${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
    };

    try {
        const totalSize = state.selectedFile.size;

        while (offset < totalSize) {
            if (mySessionId !== currentTransferSessionId) {
                console.warn(`[發送端] Session #${mySessionId} 已過期，由新 Session 接手，退出迴圈`);
                return;
            }

            if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
                console.warn(`[發送端 中斷暫停] 通道中斷，傳輸停在 ${(offset / (1024 * 1024)).toFixed(2)} MB，等待自動重連恢復...`);
                isSendingInProgress = false;
                return;
            }

            // 批次從磁碟讀取 2MB 大區塊至記憶體
            const blockEnd = Math.min(offset + BLOCK_SIZE, totalSize);
            const blockSlice = state.selectedFile.slice(offset, blockEnd);
            const blockBuffer = await blockSlice.arrayBuffer();

            let blockOffset = 0;
            const blockLength = blockBuffer.byteLength;

            // 在記憶體中連續切分 64KB 快速發送
            while (blockOffset < blockLength) {
                if (mySessionId !== currentTransferSessionId) return;

                if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
                    console.warn(`[發送端 中斷暫停] 通道中斷，傳輸停在 ${(offset / (1024 * 1024)).toFixed(2)} MB`);
                    isSendingInProgress = false;
                    return;
                }

                // 雙水位背壓控制：緩存超過 4MB (High Watermark) 時才等待其降回 1MB (Low Watermark)
                if (state.dataChannel.bufferedAmount > BUFFERED_HIGH_THRESHOLD) {
                    let stagnationCount = 0;
                    let lastBufferedAmount = state.dataChannel.bufferedAmount;

                    while (state.dataChannel.bufferedAmount > BUFFERED_LOW_THRESHOLD) {
                        await new Promise((resolve) => {
                            bufferLowResolver = resolve;
                            setTimeout(() => {
                                if (bufferLowResolver === resolve) {
                                    bufferLowResolver = null;
                                    resolve();
                                }
                            }, 250);
                        });

                        if (mySessionId !== currentTransferSessionId) return;

                        if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
                            isSendingInProgress = false;
                            return;
                        }

                        const currentBuffered = state.dataChannel.bufferedAmount;
                        if (currentBuffered >= lastBufferedAmount) {
                            stagnationCount++;
                            if (stagnationCount >= 20) { // 約 5 秒無任何消化
                                console.error('[發送端] 偵測到發送背壓持續卡死無進度，將主動重連...');
                                handlePeerDisconnection('發送背壓停滯超時', false);
                                return;
                            }
                        } else {
                            stagnationCount = 0;
                            lastBufferedAmount = currentBuffered;
                        }
                    }
                }

                const nextBlockOffset = Math.min(blockOffset + CHUNK_SIZE, blockLength);
                const chunkBuffer = blockBuffer.slice(blockOffset, nextBlockOffset);

                state.dataChannel.send(chunkBuffer);
                lastActivityTime = Date.now();

                const bytesSent = chunkBuffer.byteLength;
                blockOffset += bytesSent;
                offset += bytesSent;
                totalSent = offset;

                const now = performance.now();
                if (now - lastUiUpdateTime > 80 || totalSent >= totalSize) {
                    lastUiUpdateTime = now;

                    const percent = Math.min(100, Math.round((totalSent / totalSize) * 100));
                    const progPercent = document.getElementById('progress-percent');
                    const progBar = document.getElementById('progress-bar');
                    if (progPercent) progPercent.innerText = `${percent}%`;
                    if (progBar) progBar.style.width = `${percent}%`;

                    const speedElapsed = (now - lastSpeedTime) / 1000;
                    if (speedElapsed > 0) {
                        const instantSpeed = (totalSent - lastSpeedBytes) / speedElapsed;
                        smoothSpeed = smoothSpeed === 0 ? instantSpeed : smoothSpeed * 0.7 + instantSpeed * 0.3;
                        lastSpeedBytes = totalSent;
                        lastSpeedTime = now;

                        const speedText = document.getElementById('transfer-speed');
                        if (speedText) {
                            speedText.innerText = `${(smoothSpeed / (1024 * 1024)).toFixed(2)} MB/s`;
                        }

                        const remainingBytes = Math.max(0, totalSize - totalSent);
                        const etaSeconds = remainingBytes / smoothSpeed;

                        const timeText = document.getElementById('transfer-time');
                        if (timeText) {
                            timeText.innerText = percent >= 100 ? '00:00' : formatEta(etaSeconds);
                        }
                    }
                }
            }
        }

        if (mySessionId !== currentTransferSessionId) return;

        console.log(`[發送端 完成全數發送] 發送 file-end 信號...`);

        if (state.dataChannel && state.dataChannel.readyState === 'open') {
            state.dataChannel.send(JSON.stringify({ type: 'file-end' }));
            lastActivityTime = Date.now();
        }

        const progStatus = document.getElementById('progress-status');
        const progPercent = document.getElementById('progress-percent');
        const progBar = document.getElementById('progress-bar');
        if (progStatus) progStatus.innerText = '傳送完畢';
        if (progPercent) progPercent.innerText = '100%';
        if (progBar) progBar.style.width = '100%';

        const timeText = document.getElementById('transfer-time');
        if (timeText) timeText.innerText = '00:00';

        showToast('檔案已完全投遞！', 'success');
        isSendingInProgress = false;
    } catch (e) {
        if (mySessionId === currentTransferSessionId) {
            console.error('[發送端 異常]:', e);
            isSendingInProgress = false;
            handlePeerDisconnection('傳輸中斷: ' + e.message, false);
        }
    } finally {
        if (mySessionId === currentTransferSessionId) {
            releaseWakeLock();
            if (btnSend && !isSendingInProgress) {
                btnSend.disabled = false;
                btnSend.classList.remove('opacity-40', 'cursor-not-allowed');
            }
        }
    }
}

/**
 * 釋放 OPFS Worker 與內部資源 (修復：狀態完整復原)
 */
export function cleanupOPFSWorker(isSuccess = false, forceDelete = false) {
    if (opfsWorker) {
        try {
            opfsWorker.terminate();
        } catch (e) { }
        opfsWorker = null;
    }

    if (ackFlushTimer) {
        clearInterval(ackFlushTimer);
        ackFlushTimer = null;
    }

    if (forceDelete && currentTempFilename) {
        removeOPFSFile(currentTempFilename);
        currentTempFilename = null;
    }

    recvQueue = [];
    isWorkerReady = false;
    fileEndPending = false;
    isFinishSent = false;
    inFlightBytes = 0;
    bufferLowResolver = null;
}

export async function clearEntireOPFS() {
    if (!navigator.storage || !navigator.storage.getDirectory) return;

    try {
        const rootDir = await navigator.storage.getDirectory();
        for await (const [name, handle] of rootDir.entries()) {
            try {
                await rootDir.removeEntry(name, { recursive: true });
                console.log(`[OPFS 清理] 已清除項目: ${name} (${handle.kind})`);
            } catch (err) {
                console.warn(`[OPFS 清理] 刪除 ${name} 失敗:`, err);
            }
        }
        console.log('[OPFS 清理] OPFS 空間已全數清空完成。');
    } catch (err) {
        console.warn('[OPFS 清理] 執行失敗:', err);
    }
}