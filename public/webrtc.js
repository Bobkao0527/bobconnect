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

let ackResolver = null;


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

    // 每次消化最多 32 個 chunk 避免主執行緒卡頓
    let processed = 0;
    while (recvQueue.length > 0 && processed < 32) {
        const item = recvQueue.shift();
        forwardChunkToWorker(item);
        processed++;
    }

    if (fileEndPending && recvQueue.length === 0 && opfsWorker) {
        fileEndPending = false;
        opfsWorker.postMessage({ type: 'finish' });
    }
}

export async function handleIncomingData(data) {
    if (typeof data === 'string') {
        try {
            const message = JSON.parse(data);

            if (message.type === 'file-meta') {
                state.incomingFileInfo = { name: message.name, size: message.size };
                state.receivedSize = 0;
                state.lastRecvUiTime = 0;
                currentOffset = 0;
                recvQueue = [];
                isWorkerReady = false;
                fileEndPending = false;

                currentDownloadId = `dl_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
                cleanupOPFSWorker();

                try {
                    opfsWorker = new Worker('./opfs-worker.js');
                } catch (err) {
                    showToast('Worker 建立失敗: ' + err.message, 'error');
                    return;
                }

                opfsWorker.onmessage = (e) => {
                    const msg = e.data;
                    if (!msg) return;

                    if (msg.type === 'init-success') {
                        isWorkerReady = true;
                        drainRecvQueue();
                    } else if (msg.type === 'write-ack') {
                        // Worker 寫入完成，繼續消化佇列
                        drainRecvQueue();
                    } else if (msg.type === 'finish-success') {
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
                        showToast(`「${filename}」傳輸成功！`, 'success');
                        setTimeout(() => {
                            URL.revokeObjectURL(blobUrl);
                            cleanupOPFSWorker();
                        }, 60000);
                    } else if (msg.type === 'error') {
                        showToast('寫入錯誤: ' + msg.message, 'error');
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
                if (!isWorkerReady || recvQueue.length > 0) {
                    fileEndPending = true;
                } else if (opfsWorker) {
                    opfsWorker.postMessage({ type: 'finish' });
                }
            }
        } catch (e) {
            console.error('控制訊息錯誤:', e);
        }
    } else {
        // 收到二進位切片
        if (isWorkerReady && recvQueue.length === 0) {
            // 🚀 若 Worker 閒置，直接零拷貝 Transfer 到 Worker
            forwardChunkToWorker(data);
        } else {
            recvQueue.push(data);
        }
    }
}

function forwardChunkToWorker(chunk) {
    const chunkSize = chunk.byteLength || (chunk.buffer ? chunk.buffer.byteLength : 0);
    state.receivedSize += chunkSize;

    let bufferToTransfer = chunk instanceof ArrayBuffer ? chunk : chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength);

    opfsWorker.postMessage({
        type: 'write',
        chunk: bufferToTransfer,
        offset: currentOffset,
        size: chunkSize
    }, [bufferToTransfer]); // Transferable 零拷貝轉移，釋放主執行緒 RAM

    currentOffset += chunkSize;

    // 更新接收進度條
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

    const file = state.selectedFile;
    state.dataChannel.send(JSON.stringify({
        type: 'file-meta',
        name: file.name,
        size: file.size
    }));

    const CHUNK_SIZE = 64 * 1024; // 64KB
    const MAX_BUFFERED_AMOUNT = 2 * 1024 * 1024; // 2MB SCTP 網卡緩衝區
    state.dataChannel.bufferedAmountLowThreshold = 512 * 1024; // 512KB 喚醒水位

    let offset = 0;
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

    // 等待 DataChannel 緩衝區釋放的 Promise 輔助器
    const waitForBufferDrain = () => {
        return new Promise((resolve) => {
            if (state.dataChannel.bufferedAmount <= state.dataChannel.bufferedAmountLowThreshold) {
                resolve();
                return;
            }
            const onLow = () => {
                state.dataChannel.removeEventListener('bufferedamountlow', onLow);
                resolve();
            };
            state.dataChannel.addEventListener('bufferedamountlow', onLow);
            // 200ms 保險定時器
            setTimeout(resolve, 200);
        });
    };

    try {
        // 🚀【關鍵】：使用 slice 分塊讀取，徹底杜絕 Safari file.stream() 記憶體洩漏
        while (offset < file.size) {
            if (state.dataChannel.readyState !== 'open') {
                throw new Error('DataChannel 連線在傳輸中途關閉');
            }

            // 流控：當瀏覽器 SCTP 緩衝區滿了，暫停讀取與發送
            if (state.dataChannel.bufferedAmount > MAX_BUFFERED_AMOUNT) {
                await waitForBufferDrain();
            }

            const end = Math.min(offset + CHUNK_SIZE, file.size);
            const sliceBlob = file.slice(offset, end);
            const chunkBuffer = await sliceBlob.arrayBuffer();

            state.dataChannel.send(chunkBuffer);
            offset = end;

            // 定期更新 UI
            const now = performance.now();
            if (now - lastUiUpdateTime > 150 || offset >= file.size) {
                lastUiUpdateTime = now;
                const percent = Math.min(100, Math.round((offset / file.size) * 100));
                document.getElementById('progress-percent').innerText = `${percent}%`;
                document.getElementById('progress-bar').style.width = `${percent}%`;

                const speedElapsed = (now - lastSpeedTime) / 1000;
                if (speedElapsed > 0) {
                    const instantSpeed = (offset - lastSpeedBytes) / speedElapsed;
                    smoothSpeed = smoothSpeed === 0 ? instantSpeed : smoothSpeed * 0.7 + instantSpeed * 0.3;
                    lastSpeedBytes = offset;
                    lastSpeedTime = now;

                    const speedText = document.getElementById('transfer-speed');
                    if (speedText) speedText.innerText = `${(smoothSpeed / (1024 * 1024)).toFixed(2)} MB/s`;

                    const remainingBytes = Math.max(0, file.size - offset);
                    const timeText = document.getElementById('transfer-time');
                    if (timeText) timeText.innerText = formatEta(remainingBytes / smoothSpeed);
                }
            }

            // 每發送 2MB 讓出一次 Event Loop 給 WebKit GC
            if (offset % (2 * 1024 * 1024) === 0) {
                await new Promise((r) => setTimeout(r, 0));
            }
        }

        // 等待所有網卡底層資料送出完畢
        while (state.dataChannel.bufferedAmount > 0) {
            if (state.dataChannel.readyState !== 'open') break;
            await new Promise((r) => setTimeout(r, 50));
        }

        state.dataChannel.send(JSON.stringify({ type: 'file-end' }));

        document.getElementById('progress-status').innerText = '傳送完畢';
        document.getElementById('progress-percent').innerText = '100%';
        document.getElementById('progress-bar').style.width = '100%';
        showToast('檔案已完全投遞！', 'success');

    } catch (e) {
        console.error('傳輸錯誤:', e);
        showToast('傳輸失敗: ' + e.message, 'error');
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