/**
 * webrtc.js - WebRTC 直連通道與信令核心
 * 負責連線握手、ICE 收集、連線狀態維護，並在連線開通時自我清理雲端信令空間。
 */
import { state, getWorkerUrl } from './state.js';
import { showToast, updateStatus, generateQRCode, triggerAutoDownload, triggerAppleGlow } from './ui.js';

let currentDownloadId = null;
let swInitPromise = null;
let resolveSwInit = null;
let currentDesiredSize = 2 * 1024 * 1024; // 2MB
let swBackpressurePromise = null;
let swBackpressureResolve = null;
let swPort = null;

const RECEIVER_WINDOW = 2 * 1024 * 1024;
const MIN_RECEIVER_CREDIT = 64 * 1024;

let receiverReadySent = false;
let receiverDownloadTriggered = false;
let receiverCreditOutstanding = 0;
let receiverProcessingChain = Promise.resolve();

let senderCredit = 0;
let senderReadyPromise = null;
let senderReadyResolve = null;
let senderCreditResolvers = [];
let senderAbort = false;
let senderAbortReason = '';

let swStartedPromise = null;
let resolveSwStarted = null;
let firstSwDrainReceived = false;

let swBackpressureReject = null;

function isAppleMobile() {
    return /iPad|iPhone|iPod/.test(navigator.userAgent) ||
        (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
}

function resolveSenderCreditWaiters() {
    const resolvers = senderCreditResolvers;
    senderCreditResolvers = [];
    resolvers.forEach(resolve => resolve());
}

function abortSender(reason) {
    senderAbort = true;
    senderAbortReason = reason || '傳輸中斷';

    if (senderReadyResolve) {
        const resolve = senderReadyResolve;
        senderReadyResolve = null;
        resolve();
    }

    resolveSenderCreditWaiters();
}

function resetSenderFlow() {
    senderCredit = 0;
    senderAbort = false;
    senderAbortReason = '';

    senderReadyPromise = new Promise((resolve) => {
        senderReadyResolve = resolve;
    });

    senderCreditResolvers = [];
}

async function waitForSenderCredit(minBytes = 1) {
    while (!senderAbort && senderCredit < minBytes && state.dataChannel?.readyState === 'open') {
        await new Promise((resolve) => {
            senderCreditResolvers.push(resolve);
        });
    }

    if (senderAbort) {
        throw new Error(senderAbortReason || '接收端取消傳輸');
    }

    if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
        throw new Error('DataChannel 已關閉');
    }
}

function sendReceiverCancel(reason) {
    if (state.dataChannel && state.dataChannel.readyState === 'open') {
        try {
            state.dataChannel.send(JSON.stringify({
                type: 'receiver-cancel',
                reason: String(reason || '')
            }));
        } catch (e) {
            console.warn('[Receiver] 發送 receiver-cancel 失敗:', e);
        }
    }
}

function resolveSwBackpressure() {
    if (swBackpressureResolve) {
        const resolve = swBackpressureResolve;

        swBackpressurePromise = null;
        swBackpressureResolve = null;
        swBackpressureReject = null;

        resolve();
    }
}

function rejectSwBackpressure(err) {
    if (swBackpressureReject) {
        const reject = swBackpressureReject;

        swBackpressurePromise = null;
        swBackpressureResolve = null;
        swBackpressureReject = null;

        reject(err || new Error('串流背壓被中斷'));
    } else {
        swBackpressurePromise = null;
        swBackpressureResolve = null;
        swBackpressureReject = null;
    }
}

function waitForSwDrain() {
    if (currentDesiredSize > 0) {
        return Promise.resolve();
    }

    if (!swBackpressurePromise) {
        swBackpressurePromise = new Promise((resolve, reject) => {
            swBackpressureResolve = resolve;
            swBackpressureReject = reject;
        });
    }

    return swBackpressurePromise;
}

function updateIncomingFileUI(force = false) {
    if (!state.incomingFileInfo) return;

    const now = Date.now();

    if (!force && now - state.lastRecvUiTime <= 100 && state.receivedSize < state.incomingFileInfo.size) {
        return;
    }

    state.lastRecvUiTime = now;

    const percent = Math.min(
        100,
        Math.round((state.receivedSize / state.incomingFileInfo.size) * 100)
    );

    const recvPercent = document.getElementById('recv-percent');
    const recvProgressBar = document.getElementById('recv-progress-bar');

    if (recvPercent) recvPercent.innerText = `${percent}%`;
    if (recvProgressBar) recvProgressBar.style.width = `${percent}%`;
}

function showReceiverAcceptUI() {
    const box = document.getElementById('incoming-file-box') || document.body;

    let btn = document.getElementById('btn-accept-download');

    if (!btn) {
        btn = document.createElement('button');
        btn.id = 'btn-accept-download';
        btn.type = 'button';

        Object.assign(btn.style, {
            display: 'block',
            marginTop: '12px',
            padding: '10px 16px',
            borderRadius: '10px',
            border: 'none',
            background: '#2563eb',
            color: '#ffffff',
            fontSize: '15px',
            cursor: 'pointer'
        });

        box.appendChild(btn);
    }

    btn.disabled = true;
    btn.innerText = '正在準備接收...';

    btn.onclick = () => {
        btn.disabled = true;
        hideReceiverAcceptUI();
        startReceiverDownloadFlow();
    };
}

function enableReceiverAcceptUI() {
    const btn = document.getElementById('btn-accept-download');

    if (btn) {
        btn.disabled = false;
        btn.innerText = '允許並開始儲存';
    }
}

function hideReceiverAcceptUI() {
    const btn = document.getElementById('btn-accept-download');

    if (btn) {
        btn.onclick = null;
        btn.remove();
    }
}

function triggerStreamDownload(downloadUrl, filename) {
    if (isAppleMobile()) {
        const a = document.createElement('a');
        a.href = downloadUrl;
        a.download = filename || 'download';
        a.style.display = 'none';
        a.rel = 'noopener';

        document.body.appendChild(a);
        a.click();

        setTimeout(() => {
            a.remove();
        }, 10000);
    } else {
        let iframe = document.getElementById('sw-download-iframe');

        if (!iframe) {
            iframe = document.createElement('iframe');
            iframe.id = 'sw-download-iframe';
            iframe.style.display = 'none';
            document.body.appendChild(iframe);
        }

        iframe.src = downloadUrl;
    }
}

function setupReceiverSwPort(filename, size) {
    const channel = new MessageChannel();
    swPort = channel.port1;

    swInitPromise = new Promise((resolve) => {
        resolveSwInit = resolve;
    });

    swStartedPromise = new Promise((resolve) => {
        resolveSwStarted = resolve;
    });

    firstSwDrainReceived = false;

    swPort.onmessage = (e) => {
        const msg = e.data || {};

        if (msg.type === 'init-success') {
            if (resolveSwInit) resolveSwInit();

            if (isAppleMobile()) {
                enableReceiverAcceptUI();
            } else {
                startReceiverDownloadFlow();
            }
        }

        else if (msg.type === 'drain' || msg.type === 'write-ack') {
            currentDesiredSize = Number(msg.desiredSize) || 0;

            if (!firstSwDrainReceived) {
                firstSwDrainReceived = true;

                if (resolveSwStarted) {
                    resolveSwStarted();
                }

                waitForSwStartAndNotifySender();
            }

            if (currentDesiredSize > 0) {
                resolveSwBackpressure();
                grantCreditToSender();
            }
        }

        else if (msg.type === 'stream-cancel') {
            console.warn('[PAGE] SW 串流取消:', msg.reason);
            showToast('接收端下載已取消或失敗', 'error');
            sendReceiverCancel(msg.reason || 'stream-cancel');
            cleanupSWStream();
        }
    };

    navigator.serviceWorker.controller.postMessage({
        type: 'init',
        downloadId: currentDownloadId,
        filename,
        size
    }, [channel.port2]);
}

function startReceiverDownloadFlow() {
    if (!currentDownloadId || receiverDownloadTriggered) return;

    receiverDownloadTriggered = true;

    const downloadUrl = `./sw-download?id=${currentDownloadId}`;
    triggerStreamDownload(downloadUrl, state.incomingFileInfo?.name);

    waitForSwStartAndNotifySender();
}

function waitForSwStartAndNotifySender() {
    if (!swStartedPromise) return;

    swStartedPromise.then(() => {
        if (receiverReadySent || !state.dataChannel || state.dataChannel.readyState !== 'open') {
            return;
        }

        receiverReadySent = true;

        const desired = Number(currentDesiredSize) > 0
            ? Number(currentDesiredSize)
            : RECEIVER_WINDOW;

        const credit = Math.max(
            MIN_RECEIVER_CREDIT,
            Math.min(RECEIVER_WINDOW, desired)
        );

        receiverCreditOutstanding += credit;

        state.dataChannel.send(JSON.stringify({
            type: 'receiver-ready',
            credit
        }));
    }).catch(() => {
        // 串流可能被取消或清理，忽略即可
    });
}

function grantCreditToSender() {
    if (!receiverReadySent || !state.dataChannel || state.dataChannel.readyState !== 'open') {
        return;
    }

    const target = Math.max(0, Math.floor(Number(currentDesiredSize) || 0));

    let credit = target - receiverCreditOutstanding;

    if (credit <= 0) return;

    // 避免過度頻繁送出小 credit
    if (credit < MIN_RECEIVER_CREDIT && receiverCreditOutstanding > 0) {
        return;
    }

    credit = Math.floor(Math.min(credit, RECEIVER_WINDOW));

    if (credit <= 0) return;

    receiverCreditOutstanding += credit;

    try {
        state.dataChannel.send(JSON.stringify({
            type: 'receiver-credit',
            credit
        }));
    } catch (e) {
        console.warn('[Receiver] 發送 receiver-credit 失敗:', e);
    }
}

async function processBinaryChunk(data) {
    if (!currentDownloadId || !state.incomingFileInfo) return;

    if (swInitPromise) {
        await swInitPromise;
    }

    const byteLength = data.byteLength;

    receiverCreditOutstanding = Math.max(
        0,
        receiverCreditOutstanding - byteLength
    );

    state.receivedSize += byteLength;

    if (currentDesiredSize <= 0) {
        await waitForSwDrain();
    }

    if (!navigator.serviceWorker?.controller) {
        throw new Error('Service Worker 控制器不存在');
    }

    navigator.serviceWorker.controller.postMessage({
        type: 'write',
        downloadId: currentDownloadId,
        chunk: data
    }, [data]);

    updateIncomingFileUI();
}

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
            console.log('[DO 信令] 握手成功，已主動關閉 WebSocket 連線');
        }

        document.getElementById('setup-view').classList.add('hidden');
        document.getElementById('signaling-box').classList.add('hidden');

        const tfPanel = document.getElementById('transfer-panel');
        tfPanel.classList.remove('hidden');
        tfPanel.style.opacity = '1';
    };

    state.dataChannel.onclose = () => {
        abortSender('DataChannel 已關閉');
        cleanupSWStream();

        updateStatus('連線已被安全釋放', 'red');
        showToast('連結已釋放斷開', 'error');

        document.getElementById('transfer-panel').classList.add('hidden');
        document.getElementById('setup-view').classList.remove('hidden');
    };

    state.dataChannel.onerror = () => {
        abortSender('DataChannel 發生錯誤');
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
                state.incomingFileInfo = {
                    name: message.name,
                    size: message.size
                };

                state.receivedSize = 0;
                state.lastRecvUiTime = 0;

                cleanupSWStream();

                currentDownloadId = `dl_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
                currentDesiredSize = RECEIVER_WINDOW;
                receiverProcessingChain = Promise.resolve();

                document.getElementById('incoming-file-box')?.classList.remove('hidden');

                const recvFileName = document.getElementById('recv-file-name');
                if (recvFileName) {
                    recvFileName.innerText = state.incomingFileInfo.name;
                }

                updateIncomingFileUI(true);

                const swAvailable = 'serviceWorker' in navigator && !!navigator.serviceWorker.controller;

                if (!swAvailable) {
                    showToast('Service Worker 未就緒，無法進行串流下載', 'error');
                    sendReceiverCancel('Service Worker 未就緒');
                    return;
                }

                setupReceiverSwPort(message.name, message.size);

                if (isAppleMobile()) {
                    showReceiverAcceptUI();
                }
            }

            else if (message.type === 'receiver-ready') {
                if (state.isHost) {
                    senderCredit += Math.max(0, Number(message.credit) || 0);

                    if (senderReadyResolve) {
                        const resolve = senderReadyResolve;
                        senderReadyResolve = null;
                        resolve();
                    }

                    resolveSenderCreditWaiters();

                    updateStatus('接收端已允許下載，開始傳輸...', 'green');
                }
            }

            else if (message.type === 'receiver-credit') {
                if (state.isHost) {
                    senderCredit += Math.max(0, Number(message.credit) || 0);
                    resolveSenderCreditWaiters();
                }
            }

            else if (message.type === 'receiver-cancel') {
                if (state.isHost) {
                    abortSender(message.reason || '接收端取消下載');
                    showToast('接收端取消或下載失敗', 'error');
                    updateStatus('接收端取消或下載失敗', 'red');
                }
            }

            else if (message.type === 'file-end') {
                await receiverProcessingChain;

                cleanupSWStream();

                updateIncomingFileUI(true);

                const recvPercent = document.getElementById('recv-percent');
                const recvProgressBar = document.getElementById('recv-progress-bar');

                if (recvPercent) recvPercent.innerText = '100% (完成)';
                if (recvProgressBar) recvProgressBar.style.width = '100%';

                showToast(`「${state.incomingFileInfo?.name || '檔案'}」傳輸成功！`, 'success');
            }
        } catch (e) {
            console.error('解析控制訊息失敗:', e);
            cleanupSWStream();
        }
    } else {
        receiverProcessingChain = receiverProcessingChain
            .then(() => processBinaryChunk(data))
            .catch((err) => {
                console.error('[Receiver] 處理二進位資料失敗:', err);
                sendReceiverCancel(err?.message || 'receiver chunk error');
                cleanupSWStream();
            });
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

    resetSenderFlow();

    state.dataChannel.send(JSON.stringify({
        type: 'file-meta',
        name: state.selectedFile.name,
        size: state.selectedFile.size
    }));

    updateStatus('等待接收端允許下載...', 'yellow');

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

        if (seconds < 60) {
            return `00:${String(seconds).padStart(2, '0')}`;
        }

        const minutes = Math.floor(seconds / 60);
        const secs = seconds % 60;

        if (minutes < 60) {
            return `${String(minutes).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
        }

        const hours = Math.floor(minutes / 60);
        const mins = minutes % 60;

        return `${String(hours).padStart(2, '0')}:${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
    };

    try {
        await senderReadyPromise;

        if (senderAbort) {
            throw new Error(senderAbortReason || '接收端取消下載');
        }

        if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
            throw new Error('DataChannel 已關閉');
        }

        updateStatus('接收端已允許，開始傳輸...', 'green');

        const file = state.selectedFile;
        const fileSize = file.size;
        let offset = 0;

        while (offset < fileSize) {
            if (senderAbort) {
                throw new Error(senderAbortReason || '接收端取消下載');
            }

            if (state.dataChannel.readyState !== 'open') {
                throw new Error('傳輸途中 DataChannel 已關閉');
            }

            // WebRTC DataChannel 本地緩衝區背壓
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
                        if (senderAbort || !state.dataChannel || state.dataChannel.readyState !== 'open') {
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

                if (senderAbort) {
                    throw new Error(senderAbortReason || '接收端取消下載');
                }
            }

            // 接收端 Service Worker / 下載端 credit 背壓
            await waitForSenderCredit(1);

            if (senderAbort) {
                throw new Error(senderAbortReason || '接收端取消下載');
            }

            const creditAvailable = Math.max(0, Math.floor(senderCredit));

            const nextChunkSize = Math.min(
                chunkSize,
                creditAvailable,
                fileSize - offset
            );

            if (nextChunkSize <= 0) {
                await waitForSenderCredit(1);
                continue;
            }

            const slice = file.slice(offset, offset + nextChunkSize);
            const buffer = await slice.arrayBuffer();

            senderCredit -= buffer.byteLength;
            state.dataChannel.send(buffer);

            offset += buffer.byteLength;
            totalSent += buffer.byteLength;

            const now = performance.now();

            if (now - lastUiUpdateTime > 100 || totalSent >= fileSize) {
                lastUiUpdateTime = now;

                const percent = Math.min(100, Math.round((totalSent / fileSize) * 100));

                document.getElementById('progress-percent').innerText = `${percent}%`;
                document.getElementById('progress-bar').style.width = `${percent}%`;

                const speedElapsed = (now - lastSpeedTime) / 1000;

                if (speedElapsed > 0) {
                    const instantSpeed = (totalSent - lastSpeedBytes) / speedElapsed;

                    smoothSpeed = smoothSpeed === 0
                        ? instantSpeed
                        : smoothSpeed * 0.7 + instantSpeed * 0.3;

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
            if (senderAbort) {
                throw new Error(senderAbortReason || '接收端取消下載');
            }

            if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
                throw new Error('DataChannel 已關閉');
            }

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
        updateStatus('傳輸中斷', 'red');
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
        try {
            navigator.serviceWorker.controller.postMessage({
                type: 'close',
                downloadId: currentDownloadId
            });
        } catch (e) {
            console.warn('[Cleanup] 發送 SW close 失敗:', e);
        }
    }

    rejectSwBackpressure(new Error('串流已清理'));

    if (swPort) {
        try {
            swPort.close();
        } catch (e) {
            // ignore
        }
        swPort = null;
    }

    currentDownloadId = null;

    swInitPromise = null;
    resolveSwInit = null;

    swStartedPromise = null;
    resolveSwStarted = null;

    firstSwDrainReceived = false;
    receiverDownloadTriggered = false;
    receiverReadySent = false;
    receiverCreditOutstanding = 0;

    currentDesiredSize = RECEIVER_WINDOW;

    swBackpressurePromise = null;
    swBackpressureResolve = null;
    swBackpressureReject = null;

    receiverProcessingChain = Promise.resolve();

    hideReceiverAcceptUI();
}