/**
 * webrtc.js - WebRTC 直連通道與信令核心
 * 負責連線握手、ICE 收集、連線狀態維護，並在連線開通時自我清理雲端信令空間。
 */
import { state, getWorkerUrl } from './state.js';
import { showToast, updateStatus, generateQRCode, triggerAutoDownload, triggerAppleGlow } from './ui.js';

let useOpfsWorker = false;
let opfsWorkerInitPromise = null;
let resolveOpfsWorkerInit = null;
const opfsWorker = new Worker(new URL('./opfs-worker.js', import.meta.url), { type: 'module' });

opfsWorker.onmessage = (e) => {
    const { type, url, error } = e.data;

    if (type === 'init-success') {
        useOpfsWorker = true;
        if (resolveOpfsWorkerInit) resolveOpfsWorkerInit();
    } 
    else if (type === 'init-error') {
        console.warn("[OPFS Worker] 建立失敗，降級為記憶體模式:", error);
        useOpfsWorker = false;
        if (resolveOpfsWorkerInit) resolveOpfsWorkerInit();
    } 
    else if (type === 'complete') {
        // Worker 寫入完成，觸發下載
        triggerAutoDownload(url, state.incomingFileInfo.name, state.incomingFileInfo.size);
        document.getElementById('recv-percent').innerText = '100% (完成)';
        showToast(`「${state.incomingFileInfo.name}」傳輸成功！`, 'success');

        // 🚀 自動下載後延遲釋放 Blob URL，防止記憶體洩漏
        setTimeout(() => URL.revokeObjectURL(url), 10000);
    }
    else if (type === 'error') {
        console.error("[OPFS Worker 錯誤]:", error);
        showToast(error, 'error');
    }
};

let localCandidates = [];
// 🚀 批次寫入緩衝桶（避免 I/O 佇列爆滿）
let currentBatchSize = 0;
const BATCH_THRESHOLD = 4 * 1024 * 1024; // 累積滿 4MB 才寫一次硬碟
let pendingCandidates = [];
let opfsInitPromise = null;

// 非阻塞式背景 IPv4 探針
export async function probeIPv4() {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 2000); // 2秒逾時

    try {
        const res = await fetch('https://api4.ipify.org?format=json', { signal: controller.signal });
        const data = await res.json();
        state.localIPv4 = data.ip;
    } catch (e) {
        console.warn("[IPv4 探針] 取得失敗或逾時，降級處理:", e);
    } finally {
        clearTimeout(timeoutId);
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
        state.dataChannel.bufferedAmountLowThreshold = 131072;
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

//建立 Durable Object WebSocket 信令轉發通道
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
            state.dataChannel.bufferedAmountLowThreshold = 131072;
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
        updateStatus('連線已被安全釋放', 'red');
        showToast('連結已釋放斷開', 'error');
        document.getElementById('transfer-panel').classList.add('hidden');
        document.getElementById('setup-view').classList.remove('hidden');
    };
    state.dataChannel.onmessage = (event) => {
        handleIncomingData(event.data);
    };
}

// 處理接收資料流
export async function handleIncomingData(data) {
    if (typeof data === 'string') {
        try {
            const message = JSON.parse(data);
            if (message.type === 'file-meta') {
                useOpfsWorker = false;
                state.incomingFileInfo = { name: message.name, size: message.size };
                state.receivedSize = 0;
                state.lastRecvUiTime = 0;
                state.receiveBuffer = [];
                let resolved = false;
                opfsWorkerInitPromise = new Promise((resolve) => {
                    resolveOpfsWorkerInit = () => {
                        if (!resolved) {
                            resolved = true;
                            resolve();
                        }
                    };
                });
                // 🚨 iOS Safari OPFS 初始化最多等待 3 秒，避免永遠卡在 0%
                const initTimeout = setTimeout(() => {
                    if (!resolved) {
                        console.warn('[OPFS] 初始化逾時，降級至記憶體模式');
                        useOpfsWorker = false;
                        if (resolveOpfsWorkerInit) resolveOpfsWorkerInit();
                    }
                }, 3000);
                const originalResolve = resolveOpfsWorkerInit;
                resolveOpfsWorkerInit = () => {
                    clearTimeout(initTimeout);
                    originalResolve();
                };
                opfsWorker.postMessage({ type: 'init', filename: message.name });
                document.getElementById('incoming-file-box')?.classList.remove('hidden');
                const recvFileName = document.getElementById('recv-file-name');
                if (recvFileName) recvFileName.innerText = state.incomingFileInfo.name;

            } else if (message.type === 'file-end') {
                if (opfsWorkerInitPromise) await opfsWorkerInitPromise;
                if (useOpfsWorker) {
                    opfsWorker.postMessage({ type: 'close' });
                } else {
                    const blob = new Blob(state.receiveBuffer);
                    const downloadUrl = URL.createObjectURL(blob);
                    state.receiveBuffer = [];

                    triggerAutoDownload(downloadUrl, state.incomingFileInfo.name, state.incomingFileInfo.size);
                    document.getElementById('recv-percent').innerText = '100% (完成)';
                    showToast(`「${state.incomingFileInfo.name}」傳輸成功！`, 'success');

                    setTimeout(() => URL.revokeObjectURL(downloadUrl), 10000);
                }
            }
        } catch (e) {
            console.error('解析控制訊息失敗:', e);
        }
    } else {
        // 第一個 binary chunk 到達時，如果 OPFS 還沒完成初始化，最多只等 3 秒
        if (opfsWorkerInitPromise) await opfsWorkerInitPromise;

        state.receivedSize += data.byteLength;

        if (useOpfsWorker) {
            try {
                opfsWorker.postMessage({ type: 'write', chunk: data }, [data]);
            } catch (e) {
                console.error('[OPFS] Worker 傳送失敗，降級記憶體模式:', e);
                useOpfsWorker = false;
                state.receiveBuffer.push(data);
            }
        } else {
            state.receiveBuffer.push(data);
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
    const maxBufferedAmount = 8 * 1024 * 1024; // 8MB
    const resumeBufferedAmount = 2 * 1024 * 1024; // 2MB
    let totalSent = 0;
    let lastUiUpdateTime = 0;
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
                        await new Promise((resolve, reject) => {
                            let finished = false;
                            const cleanup = () => {
                                if (finished) return;
                                finished = true;
                                clearInterval(checkInterval);
                                if (state.dataChannel) state.dataChannel.onbufferedamountlow = null;
                                resolve();
                            };
                            const checkInterval = setInterval(() => {
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
                            if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
                                cleanup();
                            }
                        });

                    }
                    const chunk = currentData.subarray(offset, offset + chunkSize);
                    state.dataChannel.send(chunk);
                    offset += chunkSize;
                    totalSent += chunkSize;
                    const now = Date.now();
                    if (now - lastUiUpdateTime > 100 || totalSent >= state.selectedFile.size) {
                        lastUiUpdateTime = now;
                        const percent = Math.min(100, Math.round((totalSent / state.selectedFile.size) * 100));
                        document.getElementById('progress-percent').innerText = `${percent}%`;
                        document.getElementById('progress-bar').style.width = `${percent}%`;
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
        showToast('檔案已完全投遞！', 'success');
    } catch (e) {
        console.error('傳輸發生錯誤:', e);
        showToast('傳輸中斷：' + e.message, 'error');
    } finally {
        if (btnSend) {
            if (state.dataChannel) {
                state.dataChannel.onbufferedamountlow = null;
            }
            btnSend.disabled = false;
            btnSend.classList.remove('opacity-40', 'cursor-not-allowed');
        }
    }
}