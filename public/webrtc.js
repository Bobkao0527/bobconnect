/**
 * webrtc.js - WebRTC 直連通道與信令核心
 * 負責連線握手、ICE 收集、連線狀態維護，並在連線開通時自我清理雲端信令空間。
 * 🚀【更新】：在 dataChannel.onopen 時，完美導入並觸發 triggerAppleGlow 邊框流光。
 */
import { state, getWorkerUrl } from './state.js';
import { showToast, updateStatus, generateQRCode, triggerAutoDownload, triggerAppleGlow } from './ui.js';

let localCandidates = [];
// 🚀 批次寫入緩衝桶（避免 I/O 佇列爆滿）
let batchBuffer = [];
let currentBatchSize = 0;
const BATCH_THRESHOLD = 4 * 1024 * 1024; // 累積滿 4MB 才寫一次硬碟
let pendingCandidates = [];
let opfsWriteChain = Promise.resolve();
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

// 🚀 初始化 OPFS 寫入串流
async function initOPFSStream(filename) {
    opfsInitPromise = (async () => {
        try {
            if (!('storage' in navigator) || !('getDirectory' in navigator.storage)) {
                state.opfsWritable = null;
                return;
            }

            const root = await navigator.storage.getDirectory();
            state.opfsFileHandle = await root.getFileHandle(`temp_${Date.now()}_${filename}`, { create: true });

            if (!('createWritable' in state.opfsFileHandle)) {
                state.opfsWritable = null;
                return;
            }

            const createWritablePromise = state.opfsFileHandle.createWritable();
            const timeoutPromise = new Promise((_, reject) => 
                setTimeout(() => reject(new Error("OPFS Init Timeout")), 1000)
            );

            state.opfsWritable = await Promise.race([createWritablePromise, timeoutPromise]);
            console.log("[OPFS] 本機硬碟串流寫入通道已建立");
        } catch (err) {
            console.warn("[OPFS] 不支援、超時或建立失敗，安全降級為記憶體模式:", err);
            state.opfsWritable = null;
        }
    })();
    await opfsInitPromise;
}

// 處理接收資料流
export async function handleIncomingData(data) {
    if (typeof data === 'string') {
        try {
            const message = JSON.parse(data);
            if (message.type === 'file-meta') {
                state.incomingFileInfo = { name: message.name, size: message.size };
                state.receivedSize = 0;
                state.lastRecvUiTime = 0;
                batchBuffer = [];
                currentBatchSize = 0;
                state.receiveBuffer = []; 
                opfsWriteChain = Promise.resolve();

                await initOPFSStream(message.name);
                document.getElementById('incoming-file-box')?.classList.remove('hidden');
                document.getElementById('recv-file-name').innerText = state.incomingFileInfo.name;
            } else if (message.type === 'file-end') {
                if (opfsInitPromise) {
                    await opfsInitPromise.catch(() => {});
                }

                let downloadUrl = "";

                if (state.opfsWritable) {
                    await opfsWriteChain;
                    
                    if (batchBuffer.length > 0) {
                        const finalBlob = new Blob(batchBuffer);
                        await state.opfsWritable.write(finalBlob);
                        batchBuffer = [];
                        currentBatchSize = 0;
                    }

                    await state.opfsWritable.close();
                    state.opfsWritable = null;
                    const file = await state.opfsFileHandle.getFile();
                    downloadUrl = URL.createObjectURL(file);
                } else {
                    const blob = new Blob(state.receiveBuffer);
                    downloadUrl = URL.createObjectURL(blob);
                    state.receiveBuffer = []; 
                }

                triggerAutoDownload(downloadUrl, state.incomingFileInfo.name, state.incomingFileInfo.size);
                document.getElementById('recv-percent').innerText = '100% (完成)';
                showToast(`「${state.incomingFileInfo.name}」傳輸成功！`, 'success');
            }
        } catch (e) {
            console.error(e);
        }
    } else {
        if (opfsInitPromise) {
            await opfsInitPromise.catch(() => {});
        }

        if (state.opfsWritable) {
            batchBuffer.push(data);
            currentBatchSize += data.byteLength;
            state.receivedSize += data.byteLength;

            if (currentBatchSize >= BATCH_THRESHOLD) {
                const chunksToWrite = [...batchBuffer];
                batchBuffer = [];
                currentBatchSize = 0;

                opfsWriteChain = opfsWriteChain.then(async () => {
                    if (state.opfsWritable) {
                        const combinedBlob = new Blob(chunksToWrite);
                        await state.opfsWritable.write(combinedBlob);
                    }
                }).catch(err => console.error("[OPFS Write Error]:", err));
            }
        } else {
            state.receiveBuffer.push(data);
            state.receivedSize += data.byteLength;
        }

        if (state.incomingFileInfo) {
            const now = Date.now();
            if (now - state.lastRecvUiTime > 100 || state.receivedSize >= state.incomingFileInfo.size) {
                state.lastRecvUiTime = now;
                const percent = Math.min(100, Math.round((state.receivedSize / state.incomingFileInfo.size) * 100));
                document.getElementById('recv-percent').innerText = `${percent}%`;
                document.getElementById('recv-progress-bar').style.width = `${percent}%`;
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
    const maxBufferedAmount = 4 * 1024 * 1024; // 降至 4MB 配合流動性
    let totalSent = 0;
    let lastUiUpdateTime = 0;

    try {
        const reader = state.selectedFile.stream().getReader();
        let pendingBuffer = new Uint8Array(0);

        while (true) {
            const { done, value } = await reader.read();

            if (value) {
                const newBuffer = new Uint8Array(pendingBuffer.length + value.length);
                newBuffer.set(pendingBuffer);
                newBuffer.set(value, pendingBuffer.length);
                pendingBuffer = newBuffer;

                let offset = 0;
                while (offset + chunkSize <= pendingBuffer.length) {
                    if (state.dataChannel.readyState !== 'open') {
                        throw new Error("傳輸途中 DataChannel 已關閉");
                    }

                    if (state.dataChannel.bufferedAmount > maxBufferedAmount) {
                        await new Promise((resolve) => {
                            let timer = setTimeout(() => {
                                state.dataChannel.onbufferedamountlow = null;
                                resolve();
                            }, 200);

                            state.dataChannel.onbufferedamountlow = () => {
                                clearTimeout(timer);
                                state.dataChannel.onbufferedamountlow = null;
                                resolve();
                            };
                        });
                    }

                    const chunk = pendingBuffer.subarray(offset, offset + chunkSize);
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

                pendingBuffer = pendingBuffer.subarray(offset);
            }

            if (done) {
                if (pendingBuffer.length > 0) {
                    state.dataChannel.send(pendingBuffer);
                    totalSent += pendingBuffer.length;
                }
                break;
            }
        }

        state.dataChannel.send(JSON.stringify({ type: 'file-end' }));
        document.getElementById('progress-status').innerText = '傳送完畢';
        document.getElementById('progress-percent').innerText = '100%';
        document.getElementById('progress-bar').style.width = '100%';
        showToast('檔案已完全投遞！', 'success');

    } catch (e) {
        console.error("傳輸發生錯誤:", e);
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