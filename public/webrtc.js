/**
 * webrtc.js - WebRTC 直連核心 (修復 Safari 1GB / Flow Control 卡死問題)
 */
import { state, getWorkerUrl } from './state.js';
import { showToast, updateStatus, generateQRCode, triggerAutoDownload, triggerAppleGlow } from './ui.js';

let currentDownloadId = null;
let opfsWorker = null;
let currentOffset = 0;

let localCandidates = [];
let pendingCandidates = [];

let recvQueue = [];
let isWorkerReady = false;
let fileEndPending = false;
let inFlightBytes = 0;
const MAX_IN_FLIGHT_BYTES = 4 * 1024 * 1024; // 提升至 4MB Worker 併發視窗

// 🚀【ACK 聚合機制】避免高頻 JSON 癱瘓 Safari 反向 SCTP 通道
let accumulatedAckBytes = 0;
const ACK_BATCH_THRESHOLD = 512 * 1024; // 每累積 512KB 才發送一次 ACK

let unackedBytes = 0;
let ackResolver = null;
const MAX_UNACKED_BYTES = 4 * 1024 * 1024; // 允許發送端 4MB 在途資料

let transferAborted = false;

// 探針
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
            return ep.type === 'json' ? (await res.json())[ep.key] : (await res.text()).trim();
        } catch (err) {
            clearTimeout(timeoutId);
            throw err;
        }
    };
    try {
        state.localIPv4 = await Promise.any(endpoints.map(ep => fetchEndpoint(ep)));
    } catch (err) {}
    finally {
        controllers.forEach(c => c.abort());
    }
}

// 發起者 (Host)
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
    if (pinDisplay) pinDisplay.innerText = formattedPin;

    const githubPagesOrigin = window.location.origin + window.location.pathname;
    generateQRCode('qr-canvas', `${githubPagesOrigin}?room=${state.roomId}`);

    try {
        state.peerConnection = new RTCPeerConnection(state.rtcConfig);
        state.dataChannel = state.peerConnection.createDataChannel('fileTransfer', { ordered: true });
        setupDataChannelListeners();

        state.peerConnection.onicecandidate = (event) => {
            if (event.candidate) {
                const payload = { type: 'candidate', candidate: event.candidate, roomId: state.roomId };
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
            state.ws.send(JSON.stringify(localCandidates.shift()));
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
            } else if (data.type === 'answer' && state.isHost) {
                updateStatus('成功對齊傳輸描述，建立直連中！', 'yellow');
                await state.peerConnection.setRemoteDescription(new RTCSessionDescription(data.answer));
                await processPendingCandidates();
                showToast('對齊成功，極速握手中...', 'success');
            } else if (data.type === 'candidate' && state.peerConnection) {
                if (state.peerConnection.remoteDescription) {
                    await state.peerConnection.addIceCandidate(new RTCIceCandidate(data.candidate));
                } else {
                    pendingCandidates.push(data.candidate);
                }
            }
        } catch (err) {}
    };
}

async function processPendingCandidates() {
    while (pendingCandidates.length > 0) {
        try {
            await state.peerConnection.addIceCandidate(new RTCIceCandidate(pendingCandidates.shift()));
        } catch (e) {}
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
            setupDataChannelListeners();
        };
        state.peerConnection.onicecandidate = (event) => {
            if (event.candidate) {
                const payload = { type: 'candidate', candidate: event.candidate, roomId: state.roomId };
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
    }
}

export function setupDataChannelListeners() {
    state.dataChannel.binaryType = 'arraybuffer';
    state.dataChannel.bufferedAmountLowThreshold = 512 * 1024;

    state.dataChannel.onbufferedamountlow = () => {
        if (ackResolver) {
            ackResolver();
            ackResolver = null;
        }
    };

    state.dataChannel.onopen = () => {
        updateStatus('P2P 直連已開通 (加密)', 'green');
        showToast('極速 P2P 連結就緒', 'success');
        triggerAppleGlow();
        state.dataChannel.send(JSON.stringify({ type: 'device-info', isMobile: state.localIsMobile }));
        if (state.ws) {
            state.ws.close();
            state.ws = null;
        }
        document.getElementById('setup-view').classList.add('hidden');
        document.getElementById('signaling-box').classList.add('hidden');
        const tfPanel = document.getElementById('transfer-panel');
        tfPanel.classList.remove('hidden');
        tfPanel.style.opacity = '1';
    };

    state.dataChannel.onclose = () => {
        transferAborted = true;
        if (ackResolver) {
            ackResolver();
            ackResolver = null;
        }
        cleanupOPFSWorker();
        updateStatus('連線已中斷', 'red');
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
            if (now - state.lastRecvUiTime > 150 || state.receivedSize >= state.incomingFileInfo.size) {
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
        // 傳送最終剩餘的 ACK
        if (accumulatedAckBytes > 0 && state.dataChannel?.readyState === 'open') {
            try {
                state.dataChannel.send(JSON.stringify({ type: 'ack', bytes: accumulatedAckBytes }));
                accumulatedAckBytes = 0;
            } catch (err) {}
        }
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

            if (message.type === 'transfer-error') {
                transferAborted = true;
                showToast('傳輸中斷：' + message.message, 'error');
                if (ackResolver) {
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
                accumulatedAckBytes = 0;

                recvQueue = [];
                isWorkerReady = false;
                fileEndPending = false;
                inFlightBytes = 0;

                currentDownloadId = `dl_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
                cleanupOPFSWorker();

                try {
                    opfsWorker = new Worker('./opfs-worker.js');
                } catch (err) {
                    showToast('無法建立 Worker: ' + err.message, 'error');
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
                        accumulatedAckBytes += writtenSize;

                        // 🚀【批次 ACK 觸發】：累積到閾值才送出 ACK
                        if (accumulatedAckBytes >= ACK_BATCH_THRESHOLD && state.dataChannel?.readyState === 'open') {
                            try {
                                state.dataChannel.send(JSON.stringify({ type: 'ack', bytes: accumulatedAckBytes }));
                                accumulatedAckBytes = 0;
                            } catch (err) {}
                        }

                        drainRecvQueue();
                    }
                    else if (msg.type === 'finish-success') {
                        const { blobUrl, filename } = msg;
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
                        showToast(`「${filename}」傳輸成功！`, 'success');

                        setTimeout(() => {
                            URL.revokeObjectURL(blobUrl);
                            cleanupOPFSWorker();
                        }, 30000);
                    }
                    else if (msg.type === 'error') {
                        showToast(msg.message, 'error');
                        // 🚀【通知發送端中止】：避免發送端卡死
                        if (state.dataChannel?.readyState === 'open') {
                            try {
                                state.dataChannel.send(JSON.stringify({ type: 'transfer-error', message: msg.message }));
                            } catch (e) {}
                        }
                        cleanupOPFSWorker();
                    }
                };

                opfsWorker.postMessage({
                    type: 'init',
                    downloadId: currentDownloadId,
                    filename: message.name,
                    size: message.size
                });

                document.getElementById('incoming-file-box')?.classList.remove('hidden');
                document.getElementById('recv-file-name').innerText = state.incomingFileInfo.name;
                document.getElementById('recv-percent').innerText = '0%';
                document.getElementById('recv-progress-bar').style.width = '0%';
            } else if (message.type === 'file-end') {
                if (!isWorkerReady || recvQueue.length > 0 || inFlightBytes > 0) {
                    fileEndPending = true;
                } else if (opfsWorker) {
                    if (accumulatedAckBytes > 0 && state.dataChannel?.readyState === 'open') {
                        state.dataChannel.send(JSON.stringify({ type: 'ack', bytes: accumulatedAckBytes }));
                        accumulatedAckBytes = 0;
                    }
                    opfsWorker.postMessage({ type: 'finish' });
                }
            }
        } catch (e) {
            cleanupOPFSWorker();
        }
    } else {
        recvQueue.push(data);
        if (isWorkerReady) drainRecvQueue();
    }
}

export function evaluateFileRouting(file) {
    const btnSend = document.getElementById('btn-send');
    if (!btnSend) return;
    btnSend.disabled = false;
    btnSend.classList.remove('opacity-40', 'cursor-not-allowed');
    document.getElementById('progress-status').innerText = '隨時可以發送 (WebRTC P2P 直連)';
    showToast('已啟用 WebRTC P2P 直連傳輸', 'success');
}

// 發送端核心傳輸
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

    transferAborted = false;
    unackedBytes = 0;
    ackResolver = null;

    state.dataChannel.send(JSON.stringify({
        type: 'file-meta',
        name: state.selectedFile.name,
        size: state.selectedFile.size
    }));

    const chunkSize = 65536; // 64KB
    const maxBufferedAmount = 1024 * 1024; // 1MB 網卡發送緩衝區
    let totalSent = 0;
    let lastUiUpdateTime = 0;
    let lastSpeedTime = performance.now();
    let lastSpeedBytes = 0;
    let smoothSpeed = 0;

    const formatEta = (seconds) => {
        if (!isFinite(seconds) || seconds <= 0) return '--:--';
        seconds = Math.ceil(seconds);
        const mins = Math.floor(seconds / 60);
        const secs = seconds % 60;
        return `${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
    };

    try {
        const reader = state.selectedFile.stream().getReader();
        let leftover = null;

        while (!transferAborted) {
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
                    if (transferAborted || state.dataChannel.readyState !== 'open') {
                        throw new Error('傳輸已被中斷或對端已關閉');
                    }

                    // 🚀 雙重流控等待
                    while (
                        !transferAborted &&
                        (state.dataChannel.bufferedAmount > maxBufferedAmount || unackedBytes >= MAX_UNACKED_BYTES)
                    ) {
                        await new Promise((resolve) => {
                            ackResolver = resolve;
                            setTimeout(() => {
                                if (ackResolver === resolve) {
                                    ackResolver = null;
                                    resolve();
                                }
                            }, 50);
                        });
                    }

                    if (transferAborted) throw new Error('傳輸已被中斷');

                    const chunk = currentData.subarray(offset, offset + chunkSize);
                    state.dataChannel.send(chunk);
                    offset += chunkSize;
                    totalSent += chunkSize;
                    unackedBytes += chunkSize;

                    const now = performance.now();
                    if (now - lastUiUpdateTime > 150 || totalSent >= state.selectedFile.size) {
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
                            if (speedText) speedText.innerText = `${(smoothSpeed / (1024 * 1024)).toFixed(2)} MB/s`;

                            const remainingBytes = Math.max(0, state.selectedFile.size - totalSent);
                            const timeText = document.getElementById('transfer-time');
                            if (timeText) timeText.innerText = formatEta(remainingBytes / smoothSpeed);
                        }
                    }
                }
                if (offset < currentData.length) {
                    leftover = currentData.subarray(offset);
                }
            }

            if (done) {
                if (leftover && leftover.length > 0) {
                    while (!transferAborted && (state.dataChannel.bufferedAmount > maxBufferedAmount || unackedBytes >= MAX_UNACKED_BYTES)) {
                        await new Promise((r) => setTimeout(r, 20));
                    }
                    state.dataChannel.send(leftover);
                    totalSent += leftover.length;
                    unackedBytes += leftover.length;
                }
                break;
            }
        }

        // 等待所有在途數據確認完畢
        while (!transferAborted && (unackedBytes > 0 || state.dataChannel.bufferedAmount > 0)) {
            if (state.dataChannel.readyState !== 'open') break;
            await new Promise((r) => setTimeout(r, 30));
        }

        if (!transferAborted) {
            state.dataChannel.send(JSON.stringify({ type: 'file-end' }));
            document.getElementById('progress-status').innerText = '傳送完畢';
            document.getElementById('progress-percent').innerText = '100%';
            document.getElementById('progress-bar').style.width = '100%';
            showToast('檔案已完全投遞！', 'success');
        }
    } catch (e) {
        showToast('傳輸中斷：' + e.message, 'error');
    } finally {
        if (btnSend) {
            btnSend.disabled = false;
            btnSend.classList.remove('opacity-40', 'cursor-not-allowed');
        }
    }
}

export function cleanupOPFSWorker() {
    if (opfsWorker) {
        try {
            opfsWorker.postMessage({ type: 'abort' });
        } catch (e) {}
        opfsWorker.terminate();
        opfsWorker = null;
    }
    recvQueue = [];
    isWorkerReady = false;
    fileEndPending = false;
    inFlightBytes = 0;
    unackedBytes = 0;
    accumulatedAckBytes = 0;
    ackResolver = null;
    currentOffset = 0;
}