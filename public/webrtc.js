/**
 * webrtc.js - WebRTC 直連通道與信令核心 (OPFS Web Worker 重構版)
 */
import { state, getWorkerUrl } from './state.js';
import { showToast, updateStatus, generateQRCode, triggerAutoDownload, triggerAppleGlow } from './ui.js';

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
const MAX_IN_FLIGHT_BYTES = 2 * 1024 * 1024;

let unackedBytes = 0;
let ackResolver = null;
const MAX_UNACKED_BYTES = 2 * 1024 * 1024;

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

        state.dataChannel = state.peerConnection.createDataChannel('fileTransfer', { ordered: true });
        state.dataChannel.bufferedAmountLowThreshold = 4 * 1024 * 1024;
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
    document.getElementById('setup-view').classList.add('hidden');
    document.getElementById('signaling-box').classList.remove('hidden');
    document.getElementById('joiner-panel').classList.remove('hidden');
    updateStatus('正在連接 DO 信令房間...', 'yellow');
    try {
        state.peerConnection = new RTCPeerConnection(state.rtcConfig);
        state.peerConnection.ondatachannel = (event) => {
            state.dataChannel = event.channel;
            state.dataChannel.bufferedAmountLowThreshold = 4 * 1024 * 1024;
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
    state.dataChannel.bufferedAmountLowThreshold = 4 * 1024 * 1024;

    state.dataChannel.onopen = () => {
        updateStatus('P2P 直連已開通 (加密)', 'green');
        showToast('極速 P2P 連結就緒', 'success');
        triggerAppleGlow();
        state.dataChannel.send(JSON.stringify({
            type: 'device-info',
            isMobile: state.localIsMobile
        }));
        if (state.ws) {
            state.ws.close();
            state.ws = null;
            console.log("[DO 信令] 握手成功，已主動關閉 WebSocket 連線");
        }
        document.getElementById('setup-view').classList.add('hidden');
        document.getElementById('signaling-box').classList.add('hidden');
        
        const tfPanel = document.getElementById('transfer-panel');
        tfPanel.classList.remove('hidden');
        tfPanel.style.opacity = '1';
    };

    state.dataChannel.onclose = () => {
        cleanupOPFSWorker(); 
        updateStatus('連線已被安全釋放', 'red');
        showToast('連結已釋放斷開', 'error');
        document.getElementById('transfer-panel').classList.add('hidden');
        document.getElementById('setup-view').classList.remove('hidden');
    };

    state.dataChannel.onmessage = (event) => {
        handleIncomingData(event.data);
    };
}

function drainRecvQueue() {
    if (!isWorkerReady || !opfsWorker) return;

    while (recvQueue.length > 0 && inFlightBytes < MAX_IN_FLIGHT_BYTES) {
        const item = recvQueue.shift();
        const chunkSize = item.byteLength || (item.buffer ? item.buffer.byteLength : 0);
        if (chunkSize === 0) continue;

        state.receivedSize += chunkSize;
        inFlightBytes += chunkSize;

        let bufferToTransfer = null;
        if (item instanceof ArrayBuffer) {
            bufferToTransfer = item;
        } else if (ArrayBuffer.isView(item)) {
            bufferToTransfer = item.buffer.slice(item.byteOffset, item.byteOffset + item.byteLength);
        }

        if (bufferToTransfer) {
            opfsWorker.postMessage({
                type: 'write',
                chunk: bufferToTransfer,
                offset: currentOffset,
                size: chunkSize
            }, [bufferToTransfer]);

            currentOffset += chunkSize;
        }

        if (state.incomingFileInfo) {
            const now = Date.now();
            if (now - state.lastRecvUiTime > 100 || state.receivedSize >= state.incomingFileInfo.size) {
                state.lastRecvUiTime = now;
                const percent = Math.min(100, Math.round((state.receivedSize / state.incomingFileInfo.size) * 100));

                const recvPercent = document.getElementById('recv-percent');
                const recvProgressBar = document.getElementById('recv-progress-bar');
                if (recvPercent) recvPercent.innerText = `${percent}%`;
                if (recvProgressBar) recvProgressBar.style.width = `${percent}%`;
            }
        }
    }

    if (fileEndPending && recvQueue.length === 0 && inFlightBytes === 0 && opfsWorker) {
        fileEndPending = false;
        opfsWorker.postMessage({ type: 'finish' });
    }
}

export async function handleIncomingData(data) {
    if (typeof data === 'string') {
        try {
            const message = JSON.parse(data);

            if (message.type === 'ack') {
                unackedBytes = Math.max(0, unackedBytes - (message.bytes || 0));
                if (ackResolver && unackedBytes < MAX_UNACKED_BYTES) {
                    ackResolver();
                    ackResolver = null;
                }
                return;
            }

            if (message.type === 'file-meta') {
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

                cleanupOPFSWorker(); // 如果有舊的 Worker 先釋放

                try {
                    opfsWorker = new Worker('./opfs-worker.js');
                } catch (err) {
                    showToast('無法建立 OPFS Worker: ' + err.message, 'error');
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

                        if (state.dataChannel && state.dataChannel.readyState === 'open') {
                            try {
                                state.dataChannel.send(JSON.stringify({ type: 'ack', bytes: writtenSize }));
                            } catch (err) {}
                        }

                        drainRecvQueue();
                    }
                    else if (msg.type === 'finish-success') {
                        const { file, filename, tempFilename } = msg;
                        const fileToDelete = tempFilename || currentTempFilename;
                        const blobUrl = URL.createObjectURL(file);

                        // 立即關閉 Worker，因為 handle 已經在 Worker 內 close() 了
                        cleanupOPFSWorker();

                        if (typeof triggerAutoDownload === 'function') {
                            triggerAutoDownload(blobUrl, filename);
                        } else {
                            const a = document.createElement('a');
                            a.href = blobUrl;
                            a.download = filename;
                            document.body.appendChild(a);
                            a.click();
                            document.body.removeChild(a);
                        }

                        const recvPercent = document.getElementById('recv-percent');
                        const recvProgressBar = document.getElementById('recv-progress-bar');
                        if (recvPercent) recvPercent.innerText = '100% (完成)';
                        if (recvProgressBar) recvProgressBar.style.width = '100%';
                        showToast(`「${state.incomingFileInfo ? state.incomingFileInfo.name : filename}」傳輸成功！`, 'success');

                        // 🚀 由主線程直接執行刪除，不依賴 Worker
                        setTimeout(async () => {
                            URL.revokeObjectURL(blobUrl);
                            if (fileToDelete) {
                                await removeOPFSFile(fileToDelete);
                            }
                        }, 10000);
                    }
                    else if (msg.type === 'error') {
                        showToast('OPFS 寫入發生錯誤: ' + msg.message, 'error');
                        cleanupOPFSWorker();
                    }
                };

                opfsWorker.onerror = (err) => {
                    console.error('[OPFS Worker 錯誤]:', err);
                    showToast('OPFS Worker 發生例外錯誤', 'error');
                    cleanupOPFSWorker();
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
                if (!isWorkerReady || recvQueue.length > 0 || inFlightBytes > 0) {
                    fileEndPending = true;
                } else if (opfsWorker) {
                    opfsWorker.postMessage({ type: 'finish' });
                }
            }
        } catch (e) {
            console.error('解析控制訊息失敗:', e);
            cleanupOPFSWorker();
        }
    } else {
        recvQueue.push(data);
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
    state.dataChannel.send(JSON.stringify({
        type: 'file-meta',
        name: state.selectedFile.name,
        size: state.selectedFile.size
    }));

    const chunkSize = 65536; 
    const maxBufferedAmount = 1024 * 1024; 
    const resumeBufferedAmount = 256 * 1024; 
    unackedBytes = 0;
    ackResolver = null;

    try {
        state.dataChannel.bufferedAmountLowThreshold = resumeBufferedAmount;
    } catch (e) {}

    let totalSent = 0;
    let lastUiUpdateTime = 0;
    let lastSpeedTime = performance.now();
    let lastSpeedBytes = 0;
    let smoothSpeed = 0;
    let loopCounter = 0;

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
        const reader = state.selectedFile.stream().getReader();
        let leftover = null;
        while (true) {
            const { done, value } = await reader.read();
            if (value) {
                let currentData = value;
                if (leftover && leftover.length > 0) {
                    const combined = new Uint8Array(leftover.length + value.length);
                    combined.set(leftover);
                    combined.set(value, leftover.length);
                    currentData = combined;
                    leftover = null;
                }
                let offset = 0;
                while (offset + chunkSize <= currentData.length) {
                    if (state.dataChannel.readyState !== 'open') {
                        throw new Error('傳輸途中 DataChannel 已關閉');
                    }

                    while (
                        state.dataChannel.bufferedAmount > maxBufferedAmount ||
                        unackedBytes >= MAX_UNACKED_BYTES
                    ) {
                        await new Promise((resolve) => {
                            ackResolver = resolve;
                            const timer = setTimeout(() => {
                                if (ackResolver === resolve) {
                                    ackResolver = null;
                                    resolve();
                                }
                            }, 50);
                        });
                    }

                    const chunk = currentData.subarray(offset, offset + chunkSize);
                    state.dataChannel.send(chunk);
                    offset += chunkSize;
                    totalSent += chunkSize;
                    unackedBytes += chunkSize;

                    loopCounter++;
                    if (loopCounter % 16 === 0) {
                        await new Promise((resolve) => setTimeout(resolve, 0));
                    }

                    const now = performance.now();
                    if (now - lastUiUpdateTime > 100 || totalSent >= state.selectedFile.size) {
                        lastUiUpdateTime = now;

                        const percent = Math.min(100, Math.round((totalSent / state.selectedFile.size) * 100));
                        document.getElementById('progress-percent').innerText = `${percent}%`;
                        document.getElementById('progress-bar').style.width = `${percent}%`;

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

                            const remainingBytes = Math.max(0, state.selectedFile.size - totalSent);
                            const etaSeconds = remainingBytes / smoothSpeed;

                            const timeText = document.getElementById('transfer-time');
                            if (timeText) {
                                timeText.innerText = percent >= 100 ? '00:00' : formatEta(etaSeconds);
                            }
                        }
                    }
                }
                if (offset < currentData.length) {
                    leftover = currentData.subarray(offset);
                }
            }
            if (done) {
                if (leftover && leftover.length > 0) {
                    if (state.dataChannel.readyState !== 'open') {
                        throw new Error('傳輸途中 DataChannel 已關閉');
                    }
                    while (state.dataChannel.bufferedAmount > maxBufferedAmount || unackedBytes >= MAX_UNACKED_BYTES) {
                        await new Promise((resolve) => setTimeout(resolve, 30));
                    }
                    state.dataChannel.send(leftover);
                    totalSent += leftover.length;
                    unackedBytes += leftover.length;
                }
                break;
            }
        }

        while (unackedBytes > 0 || state.dataChannel.bufferedAmount > 0) {
            if (state.dataChannel.readyState !== 'open') break;
            await new Promise((resolve) => setTimeout(resolve, 50));
        }

        state.dataChannel.send(JSON.stringify({ type: 'file-end' }));

        document.getElementById('progress-status').innerText = '傳送完畢';
        document.getElementById('progress-percent').innerText = '100%';
        document.getElementById('progress-bar').style.width = '100%';

        const speedText = document.getElementById('transfer-speed');
        if (speedText && smoothSpeed > 0) {
            speedText.innerText = `${(smoothSpeed / (1024 * 1024)).toFixed(2)} MB/s`;
        }

        const timeText = document.getElementById('transfer-time');
        if (timeText) timeText.innerText = '00:00';

        showToast('檔案已完全投遞！', 'success');
    } catch (e) {
        console.error('傳輸發生錯誤:', e);
        showToast('傳輸中斷：' + e.message, 'error');
    } finally {
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

    // 🚀 如果傳輸中途被中止 (未完成狀態)，由主線程確保將暫存檔移除
    if (currentTempFilename && fileEndPending) {
        removeOPFSFile(currentTempFilename);
    }

    recvQueue = [];
    isWorkerReady = false;
    fileEndPending = false;
    inFlightBytes = 0;
    unackedBytes = 0;
    ackResolver = null;
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