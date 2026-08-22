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
const MAX_IN_FLIGHT_BYTES = 8 * 1024 * 1024; // 8MB 接收並發窗口

let totalSent = 0;
let bufferLowResolver = null;
const BUFFERED_LOW_THRESHOLD = 1 * 1024 * 1024; // 8MB 發送端緩衝區低水位觸發閾值

// 接收端累積確認 (Cumulative ACK) 機制
let lastAckedOffset = 0;
let ackFlushTimer = null;
const BATCH_ACK_INTERVAL = 128 * 1024; // 每累積寫入 128KB (2個 chunk) 即發送一次確認

// 心跳與看門狗
let heartbeatTimer = null;
let watchdogTimer = null;
let lastActivityTime = Date.now();
let isDisconnectHandled = false;

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
    if (ackFlushTimer) {
        clearTimeout(ackFlushTimer);
        ackFlushTimer = null;
    }
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

function triggerCumulativeAck() {
    if (state.receivedSize - lastAckedOffset >= BATCH_ACK_INTERVAL) {
        flushCumulativeAck();
    } else if (!ackFlushTimer) {
        ackFlushTimer = setTimeout(flushCumulativeAck, 10);
    }
}

// 統一的斷線處理器
export function handlePeerDisconnection(reason = '連線已斷開') {
    if (isDisconnectHandled) return;
    isDisconnectHandled = true;

    console.warn(`[連線中斷觸發]: ${reason}`);
    stopHeartbeatAndWatchdog();
    releaseWakeLock();
    cleanupOPFSWorker();

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
    unackedBytes = 0;
}

// 啟動心跳探針與傳輸看門狗
function startHeartbeatAndWatchdog() {
    stopHeartbeatAndWatchdog();
    isDisconnectHandled = false;
    lastActivityTime = Date.now();

    // 心跳機制：通道閒置時發送輕量 Ping
    heartbeatTimer = setInterval(() => {
        const isTransferring = (state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size) ||
            (state.selectedFile && totalSent < state.selectedFile.size);

        if (!isTransferring && state.dataChannel && state.dataChannel.readyState === 'open') {
            try {
                state.dataChannel.send(JSON.stringify({ type: 'ping' }));
            } catch (e) { }
        }
    }, 3000);

    // 看門狗：傳輸中超過 20 秒毫無網路或寫入活動才判定為逾時
    watchdogTimer = setInterval(() => {
        const now = Date.now();
        const idleTime = now - lastActivityTime;

        const isReceiving = state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size;
        const isSending = state.selectedFile && totalSent < state.selectedFile.size;

        if ((isReceiving || isSending) && idleTime > 20000) {
            console.error(`[看門狗逾時] 傳輸中超過 ${(idleTime / 1000).toFixed(1)} 秒未收到任何封包`);
            handlePeerDisconnection('傳輸逾時，對端失去回應');
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
        if (['disconnected', 'failed', 'closed'].includes(pc.connectionState)) {
            handlePeerDisconnection(`對端連線狀態已中斷 (${pc.connectionState})`);
        }
    };

    pc.oniceconnectionstatechange = () => {
        console.log(`[ICE 狀態變更]: ${pc.iceConnectionState}`);
        if (['disconnected', 'failed', 'closed'].includes(pc.iceConnectionState)) {
            handlePeerDisconnection(`網路連線通道已中斷 (${pc.iceConnectionState})`);
        }
    };
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
        updateStatus('P2P 直連已開通 (加密)', 'green');
        showToast('極速 P2P 連結就緒', 'success');
        triggerAppleGlow();

        startHeartbeatAndWatchdog();

        state.dataChannel.send(JSON.stringify({
            type: 'device-info',
            isMobile: state.localIsMobile
        }));

        if (state.ws) {
            state.ws.close();
            state.ws = null;
            console.log("[DO 信令] 握手成功，已主動關閉 WebSocket 連線");
        }
        document.getElementById('setup-view')?.classList.add('hidden');
        document.getElementById('signaling-box')?.classList.add('hidden');

        const tfPanel = document.getElementById('transfer-panel');
        if (tfPanel) {
            tfPanel.classList.remove('hidden');
            tfPanel.style.opacity = '1';
        }
    };

    state.dataChannel.onclose = () => {
        handlePeerDisconnection('直連通道已關閉');
    };

    state.dataChannel.onerror = (err) => {
        console.error('[DataChannel 異常]:', err);
        handlePeerDisconnection('傳輸通道發生異常錯誤');
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

    // 雙重判定：收到 file-end 訊號 或是 接收位元組數已完全達標，且 Worker 均已寫入落盤完畢
    const isTotalReceived = state.incomingFileInfo && state.receivedSize >= state.incomingFileInfo.size;
    if ((fileEndPending || isTotalReceived) && recvQueue.length === 0 && inFlightBytes === 0 && opfsWorker) {
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
                handlePeerDisconnection('對端已主動離開房間');
                return;
            }

            if (message.type === 'file-meta') {
                acquireWakeLock();

                state.incomingFileInfo = { name: message.name, size: message.size };
                state.receivedSize = 0;
                state.lastRecvUiTime = 0;
                currentOffset = 0;

                recvQueue = [];
                isWorkerReady = false;
                fileEndPending = false;
                inFlightBytes = 0;

                currentDownloadId = `dl_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
                currentTempFilename = `temp_${currentDownloadId}.tmp`;

                cleanupOPFSWorker();

                try {
                    opfsWorker = new Worker('./opfs-worker.js');
                } catch (err) {
                    showToast('無法建立 OPFS Worker: ' + err.message, 'error');
                    releaseWakeLock();
                    return;
                }

                opfsWorker.onmessage = (e) => {
                    const msg = e.data;
                    if (!msg) return;

                    if (msg.type === 'init-success') {
                        isWorkerReady = true;
                        drainRecvQueue();
                    }
                    else if (msg.type === 'write-ack') {
                        const writtenSize = msg.size || 0;
                        inFlightBytes = Math.max(0, inFlightBytes - writtenSize);
                        state.receivedSize += writtenSize;
                        lastActivityTime = Date.now();

                        updateRecvUi();
                        drainRecvQueue();
                    }
                    else if (msg.type === 'finish-success') {
                        releaseWakeLock();

                        const { file, filename, tempFilename } = msg;
                        const fileToDelete = tempFilename || currentTempFilename;
                        const blobUrl = URL.createObjectURL(file);

                        cleanupOPFSWorker();

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
                        handlePeerDisconnection('接收端寫入錯誤: ' + msg.message);
                    }
                };

                opfsWorker.onerror = (err) => {
                    releaseWakeLock();
                    console.error('[OPFS Worker 錯誤]:', err);
                    handlePeerDisconnection('OPFS Worker 內部錯誤');
                };

                opfsWorker.postMessage({
                    type: 'init',
                    downloadId: currentDownloadId,
                    tempFilename: currentTempFilename,
                    filename: message.name,
                    size: message.size
                });

                document.getElementById('incoming-file-box')?.classList.remove('hidden');
                const recvFileName = document.getElementById('recv-file-name');
                if (recvFileName) recvFileName.innerText = state.incomingFileInfo.name;

                const recvPercent = document.getElementById('recv-percent');
                const recvProgressBar = document.getElementById('recv-progress-bar');
                if (recvPercent) recvPercent.innerText = '0%';
                if (recvProgressBar) recvProgressBar.style.width = '0%';

            } else if (message.type === 'file-end') {
                fileEndPending = true;
                drainRecvQueue();
            }
        } catch (e) {
            console.error('解析控制訊息失敗:', e);
            cleanupOPFSWorker();
        }
    } else {
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

export async function sendFileChunks() {
    if (!state.selectedFile || !state.dataChannel || state.dataChannel.readyState !== 'open') {
        showToast('通道尚未連線或未選擇檔案', 'error');
        return;
    }
    const btnSend = document.getElementById('btn-send');
    if (btnSend) {
        btnSend.disabled = true;
        btnSend.classList.add('opacity-40', 'cursor-not-allowed');
    }

    await acquireWakeLock();

    state.dataChannel.send(JSON.stringify({
        type: 'file-meta',
        name: state.selectedFile.name,
        size: state.selectedFile.size
    }));

    const chunkSize = 64 * 1024; // 64KB 標準傳輸塊
    totalSent = 0;
    bufferLowResolver = null;

    let lastUiUpdateTime = 0;
    let lastSpeedTime = performance.now();
    let lastSpeedBytes = 0;
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
        let offset = 0;
        const totalSize = state.selectedFile.size;

        while (offset < totalSize) {
            if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
                throw new Error('傳輸途中 DataChannel 已關閉');
            }

            // 嚴格背壓控制：若緩衝區高於低水位門檻，循環等待直至降回門檻以下，避免爆倉
            while (state.dataChannel.bufferedAmount > BUFFERED_LOW_THRESHOLD) {
                await new Promise((resolve) => {
                    bufferLowResolver = resolve;
                    setTimeout(() => {
                        if (bufferLowResolver === resolve) {
                            bufferLowResolver = null;
                            resolve();
                        }
                    }, 50);
                });

                if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
                    throw new Error('傳輸途中 DataChannel 已關閉');
                }
            }

            const nextOffset = Math.min(offset + chunkSize, totalSize);
            const slice = state.selectedFile.slice(offset, nextOffset);
            const chunkBuffer = await slice.arrayBuffer();

            state.dataChannel.send(chunkBuffer);
            lastActivityTime = Date.now();

            const bytesSent = chunkBuffer.byteLength;
            offset += bytesSent;
            totalSent += bytesSent;

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

        // 傳輸完成通知
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
    } catch (e) {
        console.error('傳輸發生錯誤:', e);
        handlePeerDisconnection('傳輸中斷：' + e.message);
    } finally {
        releaseWakeLock();
        if (btnSend) {
            btnSend.disabled = false;
            btnSend.classList.remove('opacity-40', 'cursor-not-allowed');
        }
    }
}

// 釋放 OPFS Worker 與內部資源，並在異常中斷時確保清理未完成的暫存檔
export function cleanupOPFSWorker() {
    if (opfsWorker) {
        opfsWorker.terminate();
        opfsWorker = null;
    }

    if (ackFlushTimer) {
        clearTimeout(ackFlushTimer);
        ackFlushTimer = null;
    }

    if (currentTempFilename && fileEndPending) {
        removeOPFSFile(currentTempFilename);
    }

    recvQueue = [];
    isWorkerReady = false;
    fileEndPending = false;
    inFlightBytes = 0;
    unackedBytes = 0;
    lastAckedOffset = 0;
    ackResolver = null;
    bufferLowResolver = null;
    currentOffset = 0;
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