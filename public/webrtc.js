/**
 * webrtc.js - WebRTC 直連通道與信令核心
 * 負責連線握手、ICE 收集、連線狀態維護，並在連線開通時自我清理雲端信令空間。
 */
import { state, getWorkerUrl } from './state.js';
import { showToast, updateStatus, generateQRCode, triggerAutoDownload, triggerAppleGlow } from './ui.js';

let currentDownloadId = null;
let swInitPromise = null;
let resolveSwInit = null;

let currentDesiredSize = 16 * 1024 * 1024; // 16MB
let swBackpressurePromise = null;
let swBackpressureResolve = null;
let swPort = null;

// 🚀 初始化並註冊 Service Worker
async function initServiceWorker() {
    if ('serviceWorker' in navigator) {
        try {
            await navigator.serviceWorker.register('./sw.js', { scope: './' });
            await navigator.serviceWorker.ready;

            if (!navigator.serviceWorker.controller) {
                await new Promise((resolve) => {
                    navigator.serviceWorker.addEventListener('controllerchange', resolve, { once: true });
                });
            }
        } catch (err) {
            console.error('[Service Worker] 註冊或控制失敗，串流下載功能無法使用:', err);
            showToast('Service Worker 初始化失敗，無法進行串流傳輸', 'error');
        }
    } else {
        showToast('您的瀏覽器不支援 Service Worker 串流傳輸', 'error');
    }
}
initServiceWorker();

let localCandidates = [];
let pendingCandidates = [];

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
        state.dataChannel.bufferedAmountLowThreshold = 2 * 1024 * 1024;
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
            state.dataChannel.bufferedAmountLowThreshold = 2 * 1024 * 1024;
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
    state.dataChannel.bufferedAmountLowThreshold = 2 * 1024 * 1024;

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
        cleanupSWStream(); // 斷線時主動釋放 SW 串流與 Port 資源
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
                state.incomingFileInfo = { name: message.name, size: message.size };
                state.receivedSize = 0;
                state.lastRecvUiTime = 0;

                currentDownloadId = `dl_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
                currentDesiredSize = 2 * 1024 * 1024;

                let resolved = false;
                swInitPromise = new Promise((resolve) => {
                    resolveSwInit = () => {
                        if (!resolved) {
                            resolved = true;
                            resolve();
                        }
                    };
                });

                const swAvailable = 'serviceWorker' in navigator && !!navigator.serviceWorker.controller;
                if (!swAvailable) {
                    showToast('Service Worker 未就緒，無法進行串流下載', 'error');
                    return;
                }

                const channel = new MessageChannel();
                swPort = channel.port1;

                swBackpressurePromise = null;
                swBackpressureResolve = null;

                swPort.onmessage = (e) => {
                    console.log(
                        '[PAGE ← SW]',
                        e.data.type,
                        e.data.desiredSize
                    );

                    if (e.data.type === 'init-success') {
                        const downloadUrl = `./sw-download?id=${currentDownloadId}`;
                        
                        // iOS / iPadOS Safari 對 iframe 觸發 SW 下載較敏感，相容性處理
                        const isIOS = /iPad|iPhone|iPod/.test(navigator.userAgent) || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
                        let iframe = document.getElementById('sw-download-iframe');

                        if (!iframe) {
                            iframe = document.createElement('iframe');
                            iframe.id = 'sw-download-iframe';
                            iframe.style.display = 'none';
                            document.body.appendChild(iframe);
                        }

                        const win = window.open('about:blank', '_blank');

                        if (win) {
                            win.location.href = downloadUrl;
                        }

                        if (resolveSwInit) resolveSwInit();
                    } 
                    else if (e.data.type === 'write-ack' || e.data.type === 'drain') {
                        currentDesiredSize = e.data.desiredSize;
                        if (currentDesiredSize > 0 && swBackpressureResolve) {
                            swBackpressureResolve();
                            swBackpressurePromise = null;
                            swBackpressureResolve = null;
                        }
                    }
                };

                navigator.serviceWorker.controller.postMessage({
                    type: 'init',
                    downloadId: currentDownloadId,
                    filename: message.name,
                    size: message.size
                }, [channel.port2]);

                document.getElementById('incoming-file-box')?.classList.remove('hidden');
                const recvFileName = document.getElementById('recv-file-name');
                if (recvFileName) recvFileName.innerText = state.incomingFileInfo.name;

                const recvPercent = document.getElementById('recv-percent');
                const recvProgressBar = document.getElementById('recv-progress-bar');
                if (recvPercent) recvPercent.innerText = '0%';
                if (recvProgressBar) recvProgressBar.style.width = '0%';

            } else if (message.type === 'file-end') {
                if (swInitPromise) await swInitPromise;

                cleanupSWStream();

                const recvPercent = document.getElementById('recv-percent');
                const recvProgressBar = document.getElementById('recv-progress-bar');
                if (recvPercent) recvPercent.innerText = '100% (完成)';
                if (recvProgressBar) recvProgressBar.style.width = '100%';
                showToast(`「${state.incomingFileInfo.name}」傳輸成功！`, 'success');
            }
        } catch (e) {
            console.error('解析控制訊息失敗:', e);
            cleanupSWStream();
        }
    } else {
        if (swInitPromise) await swInitPromise;

        state.receivedSize += data.byteLength;

        // 背壓機制：控制 SW 佇列，並加入 50ms 安全逾時防止 WebKit 死鎖
        if (currentDesiredSize <= 0) {
            if (!swBackpressurePromise) {
                swBackpressurePromise = new Promise((resolve) => {
                    swBackpressureResolve = resolve;
                    setTimeout(() => {
                        if (swBackpressureResolve) {
                            swBackpressureResolve();
                            swBackpressurePromise = null;
                            swBackpressureResolve = null;
                        }
                    }, 50);
                });
            }
            await swBackpressurePromise;
        }

        try {
            if (navigator.serviceWorker?.controller) {
                navigator.serviceWorker.controller.postMessage({
                    type: 'write',
                    downloadId: currentDownloadId,
                    chunk: data
                }, [data]);
            } else {
                throw new Error('Service Worker 控制器不存在');
            }
        } catch (e) {
            console.error('[Service Worker] 串流寫入失敗:', e);
            cleanupSWStream();
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

    // 發送元數據
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
        const file = state.selectedFile;
        const fileSize = file.size;
        let offset = 0;

        // 改用 File.slice() 搭配 ArrayBuffer 讀取，完全相容 iPadOS / Mobile Safari
        while (offset < fileSize) {
            if (state.dataChannel.readyState !== 'open') {
                throw new Error('傳輸途中 DataChannel 已關閉');
            }

            // WebRTC 緩衝區監控
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

            // 讀取 64KB 切片
            const slice = file.slice(offset, offset + chunkSize);
            const buffer = await slice.arrayBuffer();

            state.dataChannel.send(buffer);
            offset += buffer.byteLength;
            totalSent += buffer.byteLength;

            // UI 進度與速率計算
            const now = performance.now();
            if (now - lastUiUpdateTime > 100 || totalSent >= fileSize) {
                lastUiUpdateTime = now;

                const percent = Math.min(100, Math.round((totalSent / fileSize) * 100));
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

                    const remainingBytes = Math.max(0, fileSize - totalSent);
                    const etaSeconds = remainingBytes / smoothSpeed;

                    const timeText = document.getElementById('transfer-time');
                    if (timeText) {
                        timeText.innerText = percent >= 100 ? '00:00' : formatEta(etaSeconds);
                    }
                }
            }
        }

        // 等待 DataChannel 緩衝清空
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

export function cleanupSWStream() {
    if (currentDownloadId && navigator.serviceWorker?.controller) {
        navigator.serviceWorker.controller.postMessage({
            type: 'close',
            downloadId: currentDownloadId
        });
    }
    if (swPort) {
        swPort.close();
        swPort = null;
    }
    swBackpressurePromise = null;
    swBackpressureResolve = null;
}