/**
 * webrtc.js - WebRTC 直連通道與信令核心 (OPFS Web Worker 重構版)
 * 負責連線握手、ICE 收集、連線狀態維護，並透過 Dedicated Web Worker + OPFS 同步寫入進行零記憶體大檔案接收。
 */
import { state, getWorkerUrl } from './state.js';
import { showToast, updateStatus, generateQRCode, triggerAutoDownload, triggerAppleGlow } from './ui.js';

let currentDownloadId = null;
let opfsWorker = null;
let opfsInitPromise = null;
let resolveOpfsInit = null;
let rejectOpfsInit = null;
let currentOffset = 0;

let localCandidates = [];
let pendingCandidates = [];

// 非阻塞式背景 IPv4 探針
export async function probeIPv4() {
    const endpoints = [
        { url: 'https://api4.ipify.org?format=json', type: 'json', key: 'ip' },
        { url: 'https://ip4.seeip.org/json', type: 'json', key: 'ip' },
        { url: 'https://ipv4.icanhazip.com', type: 'text' }
    ];

    for (const ep of endpoints) {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 1500); // 縮短單次逾時至 1.5 秒

        try {
            const res = await fetch(ep.url, { signal: controller.signal });
            if (!res.ok) continue;

            if (ep.type === 'json') {
                const data = await res.json();
                state.localIPv4 = data[ep.key];
            } else {
                const text = await res.text();
                state.localIPv4 = text.trim();
            }

            if (state.localIPv4) break; // 成功取得即中斷循環
        } catch (e) {
            console.warn(`[IPv4 探針] ${ep.url} 失敗或逾時，切換下一個`);
        } finally {
            clearTimeout(timeoutId);
        }
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
        state.dataChannel.bufferedAmountLowThreshold = 4 * 1024 * 1024; // 4MB
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

// 建立 Durable Object WebSocket 信令轉發通道
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

        // 🚀 補發所有連線建立前搜集到的 Candidate
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

// 接收端：自動對接
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

// 設定 DataChannel 監聽
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
        cleanupOPFSWorker(); // 斷線時主動釋放 OPFS Worker 資源
        updateStatus('連線已被安全釋放', 'red');
        showToast('連結已釋放斷開', 'error');
        document.getElementById('transfer-panel').classList.add('hidden');
        document.getElementById('setup-view').classList.remove('hidden');
    };

    state.dataChannel.onmessage = (event) => {
        handleIncomingData(event.data);
    };
}

// 處理接收資料流 (OPFS Dedicated Web Worker 模式)
export async function handleIncomingData(data) {
    if (typeof data === 'string') {
        try {
            const message = JSON.parse(data);
            if (message.type === 'file-meta') {
                state.incomingFileInfo = { name: message.name, size: message.size };
                state.receivedSize = 0;
                state.lastRecvUiTime = 0;
                currentOffset = 0;

                currentDownloadId = `dl_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;

                cleanupOPFSWorker();

                opfsInitPromise = new Promise((resolve, reject) => {
                    resolveOpfsInit = resolve;
                    rejectOpfsInit = reject;
                });

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
                        if (resolveOpfsInit) {
                            resolveOpfsInit();
                            resolveOpfsInit = null;
                            rejectOpfsInit = null;
                        }
                    }
                    else if (msg.type === 'finish-success') {
                        const { blobUrl, filename } = msg;

                        // 觸發原生檔案下載
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

                        // 30秒後釋放 Blob URL 記憶體指標
                        setTimeout(() => {
                            URL.revokeObjectURL(blobUrl);
                        }, 30000);

                        const recvPercent = document.getElementById('recv-percent');
                        const recvProgressBar = document.getElementById('recv-progress-bar');
                        if (recvPercent) recvPercent.innerText = '100% (完成)';
                        if (recvProgressBar) recvProgressBar.style.width = '100%';
                        showToast(`「${state.incomingFileInfo ? state.incomingFileInfo.name : filename}」傳輸成功！`, 'success');

                        cleanupOPFSWorker();
                    }
                    else if (msg.type === 'error') {
                        showToast('OPFS 寫入發生錯誤: ' + msg.message, 'error');
                        if (rejectOpfsInit) {
                            rejectOpfsInit(new Error(msg.message));
                            resolveOpfsInit = null;
                            rejectOpfsInit = null;
                        }
                        cleanupOPFSWorker();
                    }
                };

                opfsWorker.onerror = (err) => {
                    console.error('[OPFS Worker 錯誤]:', err);
                    showToast('OPFS Worker 發生例外錯誤', 'error');
                    if (rejectOpfsInit) {
                        rejectOpfsInit(err);
                        resolveOpfsInit = null;
                        rejectOpfsInit = null;
                    }
                    cleanupOPFSWorker();
                };

                // 向 Worker 發送初始化命令
                opfsWorker.postMessage({
                    type: 'init',
                    downloadId: currentDownloadId,
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
                if (opfsInitPromise) await opfsInitPromise;

                if (opfsWorker) {
                    opfsWorker.postMessage({ type: 'finish' });
                }
            }
        } catch (e) {
            console.error('解析控制訊息失敗:', e);
            cleanupOPFSWorker();
        }
    } else {
        if (opfsInitPromise) await opfsInitPromise;

        const chunkSize = data.byteLength || (data.buffer ? data.buffer.byteLength : 0);
        if (chunkSize === 0) return;

        state.receivedSize += data.byteLength;

        // 零複製 (Zero-Copy) Transferable Objects 傳輸切片給 OPFS Worker
        if (opfsWorker) {
            try {
                let bufferToTransfer = null;

                if (data instanceof ArrayBuffer) {
                    bufferToTransfer = data;
                } else if (ArrayBuffer.isView(data)) {
                    bufferToTransfer = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
                }

                if (bufferToTransfer) {
                    opfsWorker.postMessage({
                        type: 'write',
                        chunk: bufferToTransfer,
                        offset: currentOffset
                    }, [bufferToTransfer]); // 轉移所有權

                    currentOffset += chunkSize;
                }
            } catch (e) {
                console.error('[OPFS Direct Write] 切片寫入轉送失敗:', e);
                cleanupOPFSWorker();
            }
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
}

// 智慧分流引擎評估（純 WebRTC P2P 直連）
export function evaluateFileRouting(file) {
    const btnSend = document.getElementById('btn-send');

    if (!btnSend) return;
    
    // 確保發送按鈕保持啟用狀態
    btnSend.disabled = false;
    btnSend.classList.remove('opacity-40', 'cursor-not-allowed');

    // 更新 UI 提示文字與進度條樣式
    document.getElementById('progress-status').innerText = '隨時可以發送 (WebRTC P2P 直連)';
    document.getElementById('progress-bar').className = "bg-gradient-to-r from-blue-500 to-emerald-400 h-1 w-0 transition-all duration-75";
    
    showToast('已啟用 WebRTC P2P 直連傳輸', 'success');
}

// 發送端：執行 WebRTC 切片傳輸
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

    const chunkSize = 65536; // 64KB
    const maxBufferedAmount = 12 * 1024 * 1024; // 12MB
    const resumeBufferedAmount = 4 * 1024 * 1024; // 4MB
    let totalSent = 0;
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
                    if (state.dataChannel.bufferedAmount > maxBufferedAmount) {
                        await new Promise((resolve) => {
                            let finished = false;
                            let checkInterval = null;
                            const cleanup = () => {
                                if (finished) return;
                                finished = true;
                                if (checkInterval) clearInterval(checkInterval);
                                if (state.dataChannel) state.dataChannel.onbufferedamountlow = null;
                                resolve();
                            };

                            checkInterval = setInterval(() => {
                                if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
                                    cleanup();
                                    return;
                                }

                                if (state.dataChannel.bufferedAmount <= resumeBufferedAmount) {
                                    cleanup();
                                }
                            }, 50);
                            if (state.dataChannel) {
                                state.dataChannel.onbufferedamountlow = () => {
                                    if (state.dataChannel.bufferedAmount <= resumeBufferedAmount) {
                                        cleanup();
                                    }
                                };
                            }
                        });
                    }
                    const chunk = currentData.subarray(offset, offset + chunkSize);
                    state.dataChannel.send(chunk);
                    offset += chunkSize;
                    totalSent += chunkSize;

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
                    while (state.dataChannel.bufferedAmount > maxBufferedAmount) {
                        await new Promise(resolve => setTimeout(resolve, 50));
                    }
                    state.dataChannel.send(leftover);
                    totalSent += leftover.length;
                }
                break;
            }
        }
        while (state.dataChannel.bufferedAmount > resumeBufferedAmount) {
            await new Promise(resolve => setTimeout(resolve, 50));
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
            if (state.dataChannel) state.dataChannel.onbufferedamountlow = null;
            btnSend.disabled = false;
            btnSend.classList.remove('opacity-40', 'cursor-not-allowed');
        }
    }
}

// 釋放 OPFS Worker 與內部資源
export function cleanupOPFSWorker() {
    if (opfsWorker) {
        try {
            opfsWorker.postMessage({ type: 'abort' });
        } catch (e) {}
        opfsWorker.terminate();
        opfsWorker = null;
    }
    opfsInitPromise = null;
    resolveOpfsInit = null;
    rejectOpfsInit = null;
    currentOffset = 0;
}