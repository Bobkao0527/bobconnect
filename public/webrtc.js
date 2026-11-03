/**
 * webrtc.js - WebRTC 直連通道與信令核心 (OPFS Web Worker 重構版 - 支援超大檔案 2GB+ 與累積確認 Cumulative ACK)
 */
import { state, getWorkerUrl } from './state.js';
import { showToast, updateStatus, generateQRCode, triggerAutoDownload, triggerAppleGlow, acquireWakeLock, releaseWakeLock } from './ui.js';

let currentDownloadId = null;
let currentTempFilename = null;
let opfsWorker = null;
let currentOffset = 0;
let signalingQueue = Promise.resolve();

let localCandidates = [];
let pendingCandidates = [];

let recvQueue = [];
let isWorkerReady = false;
let inFlightBytes = 0;
const MAX_IN_FLIGHT_BYTES = 16 * 1024 * 1024; // 16MB 接收並發窗口

// 【接收端流量控制】佇列積壓門檻：超過即請求發送端暫停，消化後恢復
const PAUSE_QUEUE_COUNT = 256;   // 約 4MB (16KB chunks)
const RESUME_QUEUE_COUNT = 64;   // 積壓消化到 64 個以下才恢復
let isPausedRemote = false;

// 【續傳對齊】接收端送出 resume-request 後，丟棄 fileChannel 上的舊 session 資料，
// 直到收到發送端在 fileChannel (同一條有序串流) 上送出的 resume-marker 為止。
let awaitingResumeMarker = false;

let totalSent = 0;
let bufferLowResolver = null;
let ackWindowResolver = null;
let flowResumeResolver = null;
const BUFFERED_LOW_THRESHOLD = 1024 * 1024; // 1MB 累積緩存低水位
const BUFFERED_HIGH_THRESHOLD = 8 * 1024 * 1024; // 8MB 累積緩存高水位 (Chrome SCTP 佇列上限 16MB，8MB 留足安全餘裕並能吃滿高頻寬延遲乘積)
const MAX_UNACKED_WINDOW_BYTES = 32 * 1024 * 1024; // 32MB 發送端未確認窗口

const BACKPRESSURE_SAMPLE_INTERVAL_MS = 250;
// 【穩定性修復】大檔案傳輸時網路抖動或 SCTP 重傳退避 (RFC 4960) 造成短暫停滯是正常現象，
// 過早發動 ICE Restart 或直接拆掉 PeerConnection 反而會製造更多斷線與慢啟動。
// 遵循 W3C 規範：停滯 15 秒僅嘗試無損的 In-Band ICE Restart，絕不在發送迴圈內因背壓拆掉 PC (交由 connectionState=failed 或看門狗處理)。
const BACKPRESSURE_RECOVERY_DELAY_MS = 15000;  // 停滯 15 秒才嘗試 ICE Restart

// 【背景分頁零延遲讓出執行緒】使用 MessageChannel 取代 setTimeout(0)，
// 避免分頁切到背景時被 Chrome 節流為每秒最多觸發 1 次 (導致傳輸速率被鎖在 1~2 MB/s)。
const yieldChannel = typeof MessageChannel !== 'undefined' ? new MessageChannel() : null;
const pendingYieldResolvers = [];
if (yieldChannel) {
    yieldChannel.port1.onmessage = () => {
        const resolve = pendingYieldResolvers.shift();
        if (resolve) resolve();
    };
}
function yieldToEventLoop() {
    if (!yieldChannel) {
        return new Promise(resolve => setTimeout(resolve, 0));
    }
    return new Promise(resolve => {
        pendingYieldResolvers.push(resolve);
        yieldChannel.port2.postMessage(null);
    });
}

function wakeTransferWaiters() {
    if (bufferLowResolver) {
        const resolve = bufferLowResolver;
        bufferLowResolver = null;
        resolve();
    }
    if (ackWindowResolver) {
        const resolve = ackWindowResolver;
        ackWindowResolver = null;
        resolve();
    }
    if (flowResumeResolver) {
        const resolve = flowResumeResolver;
        flowResumeResolver = null;
        resolve();
    }
}

// 【動態 chunk size】依 RFC 8841 a=max-message-size 協商結果決定。
// Safari/WebKit 的 maxMessageSize 為 65536 (64KB)，SDP 未聲明時規範預設也是 64KB。
// 上限保守取 64KB 以確保跨瀏覽器相容 (Firefox 舊版對 ordered channel 僅保證 16KB)。
let CHUNK_SIZE = 16 * 1024; // 預設值，連線建立後依 sctp.maxMessageSize 動態調整
const MAX_SAFE_CHUNK_SIZE = 64 * 1024;

/**
 * 依協商結果計算安全 chunk size (在 PeerConnection 建立後呼叫)
 */
function negotiateChunkSize() {
    try {
        const sctpMax = state.peerConnection?.sctp?.maxMessageSize || 0;
        if (sctpMax > 0) {
            // 取 min(對端可接收上限, 64KB 安全上限)，再預留 1KB 給潛在 framing 開銷
            CHUNK_SIZE = Math.min(sctpMax - 1024, MAX_SAFE_CHUNK_SIZE);
        } else {
            CHUNK_SIZE = 16 * 1024; // 無法取得時退回最保守的 16KB
        }
        console.log(`[傳輸參數] sctp.maxMessageSize=${sctpMax}, 採用 chunk size=${CHUNK_SIZE / 1024}KB`);
    } catch (e) {
        CHUNK_SIZE = 16 * 1024;
    }
}

// 接收端累積確認 (Cumulative ACK) 機制
let lastAckedOffset = 0;
let ackFlushTimer = null;
const BATCH_ACK_INTERVAL = 1024 * 1024; // 每累積寫入 1MB 即發送一次確認 (加速回饋)

// 心跳與看門狗
let heartbeatTimer = null;
let watchdogTimer = null;
let disconnectedGraceTimer = null;
const DISCONNECTED_GRACE_MS = 6000; // ICE disconnected 持續 6 秒未恢復即主動重連 (不需苦等 15 秒 liveness)
let lastActivityTime = Date.now();
let isDisconnectHandled = false;

// 【看門狗】以「檔案進度」而非封包流量判斷停滯。
// 原本用 candidate-pair bytes 判斷，但 heartbeat ping 本身就會讓 bytes 增加，
// 導致 fileChannel 死掉時看門狗永遠不會觸發。
let lastProgressTime = Date.now();
let progressRecoveryAttempted = false;
let sendTransferActive = false; // 發送端是否有進行中的傳輸任務 (避免「只選了檔案未發送」被誤判)
const LIVENESS_TIMEOUT_MS = 15000;        // 控制通道 15 秒完全無訊息 → 視為斷線
const PROGRESS_SOFT_TIMEOUT_MS = 20000;   // 20 秒無檔案進度 → 嘗試通道內修復
const PROGRESS_HARD_TIMEOUT_MS = 40000;   // 40 秒無檔案進度 → 完整重連

let isFinishSent = false;

// 斷線重連區
let isReconnecting = false;
let reconnectTimer = null;
let reconnectAttemptTimer = null;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 15;
// 單次重連嘗試的逾時：時間內 stateChannel 未開通即判定失敗並重試，避免永久卡住
const RECONNECT_ATTEMPT_TIMEOUT_MS = 12000;

// 信令 WebSocket 保活：Cloudflare 會切斷閒置 100 秒的 WebSocket
let wsKeepaliveTimer = null;
const WS_KEEPALIVE_INTERVAL_MS = 30000;

// fileChannel 通道內重開的頻率限制 (超過則改走完整重連)
let fileReopenTimestamps = [];
const MAX_FILE_REOPEN_PER_MINUTE = 3;

let isSendingInProgress = false;
let lastConfirmedAckOffset = 0;
let currentTransferSessionId = 0;
let isFlowPausedByRemote = false; // 接收端流量控制旗標

// 接收端速率與 ETA 追蹤變數
let lastRecvSpeedTime = 0;
let lastRecvSpeedBytes = 0;
let smoothRecvSpeed = 0;
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

// 獨立輔助函式：由主線程直接刪除指定 OPFS 檔案
async function removeOPFSFile(filename) {
    if (!filename || !navigator.storage?.getDirectory) return;
    try {
        const rootDir = await navigator.storage.getDirectory();
        await rootDir.removeEntry(filename);
        console.log(`[OPFS 清理] 暫存檔已安全刪除: ${filename}`);
    } catch (e) {
        // 檔案若已刪除或不存在則忽略
    }
}

// 標記「檔案傳輸有實質進度」(供看門狗判斷)
function markProgress() {
    lastProgressTime = Date.now();
    progressRecoveryAttempted = false;
}

// 停止信令 WebSocket 保活
function stopWsKeepalive() {
    if (wsKeepaliveTimer) {
        clearInterval(wsKeepaliveTimer);
        wsKeepaliveTimer = null;
    }
}

// 關閉信令 WebSocket (先解除 handler，避免舊連線的 close/error 事件誤觸重連邏輯)
function closeSignalingWS() {
    stopWsKeepalive();
    const ws = state.ws;
    if (!ws) return;
    ws.onopen = ws.onmessage = ws.onerror = ws.onclose = null;
    try { ws.close(); } catch (e) { }
    state.ws = null;
}

// 解除單一 DataChannel 的所有 handler 後關閉
function detachAndCloseChannel(ch) {
    if (!ch) return;
    ch.onopen = ch.onclose = ch.onerror = ch.onmessage = ch.onbufferedamountlow = null;
    try { ch.close(); } catch (e) { }
}

/**
 * 解除並關閉目前所有 WebRTC 物件與信令連線。
 * 先將 handler 設為 null 再 close，避免舊物件的延遲事件作用到新建立的連線 (race condition)。
 */
function detachAndClosePeer() {
    detachAndCloseChannel(state.fileChannel);
    state.fileChannel = null;
    detachAndCloseChannel(state.stateChannel);
    state.stateChannel = null;

    const pc = state.peerConnection;
    if (pc) {
        pc.onconnectionstatechange = null;
        pc.oniceconnectionstatechange = null;
        pc.onicecandidate = null;
        pc.ondatachannel = null;
        try { pc.close(); } catch (e) { }
        state.peerConnection = null;
    }
    closeSignalingWS();
}

/**
 * 送出本地 ICE candidate。
 * P2P 控制通道已開通時優先走 stateChannel (In-Band)，不依賴可能已被 Cloudflare 切斷的 DO WebSocket；
 * 否則走 DO WebSocket，再不行就暫存等 WS 開通後補送。
 */
function sendLocalCandidate(candidate) {
    const json = typeof candidate.toJSON === 'function' ? candidate.toJSON() : candidate;
    if (state.stateChannel?.readyState === 'open') {
        try {
            state.stateChannel.send(JSON.stringify({ type: 'inband-candidate', candidate: json }));
            return;
        } catch (e) {
            console.warn('[ICE] In-Band candidate 發送失敗，改走信令伺服器:', e);
        }
    }
    const payload = { type: 'candidate', candidate: json, roomId: state.roomId };
    if (state.ws?.readyState === WebSocket.OPEN) {
        state.ws.send(JSON.stringify(payload));
    } else {
        localCandidates.push(payload);
    }
}

function handleSignalingMessage(data) {
    signalingQueue = signalingQueue.then(async () => {
        try {
            if (data.type === 'offer' && !state.isHost) {
                if (data.sessionToken) {
                    state.sessionToken = data.sessionToken;
                }
                // 【快速重連對齊】若 Joiner 的舊 PeerConnection 已經設定過 remoteDescription (或已連通)，
                // 代表 Host 端已經判定斷線並重建了全新的 PeerConnection 發起新 Offer。
                // 此時必須立即拆掉本地殘留的舊 PC、換上全新 PC 來接收這個 Offer，不需苦等本地逾時。
                const needFreshPc = !state.peerConnection ||
                    state.peerConnection.signalingState === 'closed' ||
                    Boolean(state.peerConnection.remoteDescription?.type);

                if (needFreshPc) {
                    console.log('[信令快速重連] 收到 Host 新 Offer，立即重建本地 PeerConnection 對齊...');
                    stopHeartbeatAndWatchdog();
                    clearReconnectTimers();
                    releaseInBandLock();
                    wakeTransferWaiters();

                    // 保留當前的信令 WebSocket，僅重建 WebRTC 實例
                    detachAndCloseChannel(state.fileChannel);
                    state.fileChannel = null;
                    detachAndCloseChannel(state.stateChannel);
                    state.stateChannel = null;
                    if (state.peerConnection) {
                        const oldPc = state.peerConnection;
                        oldPc.onconnectionstatechange = null;
                        oldPc.oniceconnectionstatechange = null;
                        oldPc.onicecandidate = null;
                        oldPc.ondatachannel = null;
                        try { oldPc.close(); } catch (e) { }
                    }

                    pendingCandidates = [];
                    localCandidates = [];
                    isDisconnectHandled = false;

                    state.peerConnection = new RTCPeerConnection(state.rtcConfig);
                    setupPeerConnectionListeners(state.peerConnection);
                    state.peerConnection.ondatachannel = handleIncomingDataChannel;
                    state.peerConnection.onicecandidate = (event) => {
                        if (event.candidate) sendLocalCandidate(event.candidate);
                    };
                }

                updateStatus('已接收 Offer 信令，正在回應 Answer...', 'yellow');
                await state.peerConnection.setRemoteDescription(new RTCSessionDescription(data.offer));
                await processPendingCandidates();

                const answer = await state.peerConnection.createAnswer();
                await state.peerConnection.setLocalDescription(answer);

                if (state.ws?.readyState === WebSocket.OPEN) {
                    state.ws.send(JSON.stringify({
                        type: 'answer',
                        answer,
                        roomId: state.roomId,
                        sessionToken: state.sessionToken
                    }));
                }
            } else if (data.type === 'answer' && state.isHost) {
                if (state.peerConnection?.signalingState === 'have-local-offer') {
                    updateStatus('成功對齊傳輸描述，建立直連中！', 'yellow');
                    await state.peerConnection.setRemoteDescription(new RTCSessionDescription(data.answer));
                    await processPendingCandidates();
                    showToast('對齊成功，極速握手中...', 'success');
                }
            } else if (data.type === 'candidate' && state.peerConnection) {
                if (state.peerConnection.remoteDescription?.type) {
                    await state.peerConnection.addIceCandidate(new RTCIceCandidate(data.candidate));
                } else {
                    pendingCandidates.push(data.candidate);
                }
            }
        } catch (err) {
            console.error("[DO WebSocket 訊息處理失敗]:", err);
        }
    });
}

// 接收端發送累積確認 (Cumulative ACK)
function flushCumulativeAck() {
    if (state.receivedSize > lastAckedOffset && state.stateChannel?.readyState === 'open') {
        try {
            state.stateChannel.send(JSON.stringify({
                type: 'ack',
                offset: state.receivedSize
            }));
            lastAckedOffset = state.receivedSize;
        } catch (e) {
            console.error('[CUM ACK 發送失敗]', e);
        }
    }
}

/**
 * 統一的斷線處理器 (支援短暫中斷重連與致命斷線區分)
 * @param {string} reason 斷線原因
 * @param {boolean} isFatal 是否為致命斷線 (如對端主動離線/使用者手動取消)
 */
export function handlePeerDisconnection(reason = '連線已斷開', isFatal = false) {
    if (isDisconnectHandled) {
        console.warn(`[連線中斷忽略重複觸發]: ${reason}`);
        return;
    }

    isDisconnectHandled = true;
    console.warn(`[連線中斷觸發]: ${reason} (isFatal=${isFatal})`);

    stopHeartbeatAndWatchdog();

    // 同時釋放兩條 DataChannel、PeerConnection 與信令連線 (先解除 handler)
    detachAndClosePeer();
    releaseInBandLock();

    // 中斷當前所有正在等待 buffer / ACK / flow-resume 的 promise
    wakeTransferWaiters();

    // 中斷發送迴圈
    currentTransferSessionId++;
    isSendingInProgress = false;
    isFlowPausedByRemote = false;

    // 清空接收端尚未寫入的隊列
    recvQueue = [];
    inFlightBytes = 0;
    isPausedRemote = false;

    const isReceivingIncomplete = state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size;
    const isSendingIncomplete = state.selectedFile && totalSent < state.selectedFile.size;
    const hasActiveSession = Boolean(state.roomId && (isReceivingIncomplete || isSendingIncomplete));

    if (!isFatal && hasActiveSession) {
        cleanupOPFSWorker(false, false);
        if (!isReconnecting) {
            attemptReconnection(reason);
        }
        return;
    }

    // 致命斷線或無傳輸任務，徹底清理
    isDisconnectHandled = true;
    releaseWakeLock();
    cleanupOPFSWorker(false, true);

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
    state.sessionToken = null;
    state.incomingFileInfo = null;
    state.receivedSize = 0;
    totalSent = 0;
    sendTransferActive = false;
    awaitingResumeMarker = false;
    fileReopenTimestamps = [];
    clearReconnectTimers();
    reconnectAttempts = 0;
    isReconnecting = false;
}

// 清除所有重連相關計時器
function clearReconnectTimers() {
    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }
    if (reconnectAttemptTimer) {
        clearTimeout(reconnectAttemptTimer);
        reconnectAttemptTimer = null;
    }
}

/**
 * 本次重連嘗試失敗 (逾時 / 信令 WS 錯誤 / 握手例外)：拆掉半成品連線並排程下一次。
 * 原本 WS 被 DO 拒絕 (429/409) 或中途斷開時沒有任何後續動作，會讓重連永久卡住。
 */
function failReconnectAttempt(reason) {
    if (!isReconnecting) return;
    console.warn(`[自動重連] 第 ${reconnectAttempts} 次嘗試失敗: ${reason}`);
    clearReconnectTimers();
    detachAndClosePeer();
    isReconnecting = false;
    attemptReconnection(reason);
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
        // 【修復】此時 isDisconnectHandled 仍為 true，必須先重設，否則 fatal 清理會被當成重複觸發而忽略
        isReconnecting = false;
        isDisconnectHandled = false;
        handlePeerDisconnection('連線逾時且無法自動恢復', true);
        return;
    }

    // 指數退避 (0.5s → 最長 8s)，避免同區網兩端共用 IP 時撞到 DO 的連線頻率限制
    const backoffDelay = Math.min(500 * Math.pow(1.5, reconnectAttempts - 1), 8000);
    console.log(`[自動重連] 第 ${reconnectAttempts} 次嘗試，${(backoffDelay / 1000).toFixed(1)} 秒後重敲信令伺服器... 原因: ${reason}`);

    updateStatus(`網路波動，正在自動重連 (第 ${reconnectAttempts} 次)...`, 'yellow');
    showToast(`網路中斷，正在嘗試續傳 (${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})...`, 'warning');

    clearReconnectTimers();
    reconnectTimer = setTimeout(async () => {
        reconnectTimer = null;
        try {
            await restartSignalingAndPeerConnection();
            // 握手只是「發起」成功，必須在時限內等到 stateChannel 開通才算真正成功
            reconnectAttemptTimer = setTimeout(() => {
                reconnectAttemptTimer = null;
                failReconnectAttempt(`${RECONNECT_ATTEMPT_TIMEOUT_MS / 1000} 秒內未完成 P2P 握手`);
            }, RECONNECT_ATTEMPT_TIMEOUT_MS);
        } catch (err) {
            failReconnectAttempt(err?.message || '握手例外');
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

    try {
        opfsWorker = new Worker(new URL('./opfs-worker.js', import.meta.url));
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
        } else if (msg.type === 'write-ack') {
            const writtenSize = msg.size || 0;
            inFlightBytes = Math.max(0, inFlightBytes - writtenSize);
            state.receivedSize += writtenSize;
            lastActivityTime = Date.now();
            if (writtenSize > 0) markProgress();

            if (state.receivedSize - lastAckedOffset >= BATCH_ACK_INTERVAL) {
                flushCumulativeAck();
            }

            updateRecvUi();
            drainRecvQueue();
        } else if (msg.type === 'finish-success') {
            releaseWakeLock();
            flushCumulativeAck();

            const { file, filename, tempFilename } = msg;
            const fileToDelete = tempFilename || currentTempFilename;
            const actualFileSize = file ? file.size : (state.incomingFileInfo?.size || 0);

            const blobUrl = URL.createObjectURL(file);
            triggerAutoDownload(blobUrl, filename, actualFileSize);

            // 完成後正常清理 Worker 與暫存檔
            cleanupOPFSWorker(true, false);
            setTimeout(async () => {
                URL.revokeObjectURL(blobUrl);
                if (fileToDelete) {
                    await removeOPFSFile(fileToDelete);
                }
            }, 30000);
        } else if (msg.type === 'error') {
            releaseWakeLock();
            handlePeerDisconnection('接收端寫入錯誤: ' + msg.message, true);
        }
    };

    opfsWorker.onerror = () => {
        releaseWakeLock();
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

    state.stateChannel = state.peerConnection.createDataChannel('stateChannel', { ordered: true });
    setupStateChannelListeners();

    state.fileChannel = state.peerConnection.createDataChannel('fileChannel', { ordered: true });
    setupFileChannelListeners();

    state.peerConnection.onicecandidate = (event) => {
        if (event.candidate) sendLocalCandidate(event.candidate);
    };

    if (state.isHost) {
        // 先確定 sessionToken，讓重連 WS 與 offer 使用同一把金鑰
        if (!state.sessionToken) {
            state.sessionToken = 'st_' + Math.random().toString(36).substring(2, 10) + Date.now().toString(36);
        }
        connectSignalingWS();

        const offer = await state.peerConnection.createOffer();
        await state.peerConnection.setLocalDescription(offer);

        const ipKey = state.localIPv4 ? `ipv4:${state.localIPv4}` : null;
        const offerPayload = {
            type: 'offer',
            offer: state.peerConnection.localDescription,
            roomId: state.roomId,
            sessionToken: state.sessionToken,
            ipKey: ipKey
        };

        if (state.ws?.readyState === WebSocket.OPEN) {
            state.ws.send(JSON.stringify(offerPayload));
        } else {
            state.pendingOffer = offerPayload;
        }
    } else {
        state.peerConnection.ondatachannel = handleIncomingDataChannel;
        connectSignalingWS();
    }
}

/**
 * Joiner 端接收對端建立的 DataChannel (含 Host 於現有連線上重開的 fileChannel)
 */
function handleIncomingDataChannel(event) {
    const channel = event.channel;
    if (channel.label === 'stateChannel') {
        if (state.stateChannel && state.stateChannel !== channel) detachAndCloseChannel(state.stateChannel);
        state.stateChannel = channel;
        setupStateChannelListeners();
    } else if (channel.label === 'fileChannel') {
        if (state.fileChannel && state.fileChannel !== channel) {
            console.log('[fileChannel] 收到對端重開的檔案通道，替換舊通道');
            detachAndCloseChannel(state.fileChannel);
        }
        state.fileChannel = channel;
        setupFileChannelListeners();
    }
}

// 啟動心跳探針與傳輸看門狗
function startHeartbeatAndWatchdog() {
    stopHeartbeatAndWatchdog();
    isDisconnectHandled = false;
    lastActivityTime = Date.now();
    markProgress();

    heartbeatTimer = setInterval(() => {
        if (state.stateChannel?.readyState === 'open') {
            const isTransferring = (state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size) ||
                (state.selectedFile && totalSent < state.selectedFile.size);
            const idleTime = Date.now() - lastActivityTime;

            if (!isTransferring || idleTime > 3000) {
                try {
                    state.stateChannel.send(JSON.stringify({ type: 'ping' }));
                } catch (e) {
                    console.warn('[Heartbeat] Ping 發送失敗:', e);
                }
            }
        }
    }, 1000);

    watchdogTimer = setInterval(() => {
        const now = Date.now();
        const isReceiving = Boolean(state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size);
        const isSending = Boolean(sendTransferActive && state.selectedFile && totalSent < state.selectedFile.size);

        if (!isReceiving && !isSending) {
            // 沒有傳輸任務時持續刷新，避免下次開始傳輸時立即誤判
            lastProgressTime = now;
            return;
        }
        if (!state.stateChannel || state.stateChannel.readyState !== 'open') return;

        // 1) 控制通道存活性：對端每秒會回 ping/pong，15 秒完全沒訊息代表 P2P 已死
        const silentTime = now - lastActivityTime;
        if (silentTime > LIVENESS_TIMEOUT_MS) {
            console.error(`[看門狗] 控制通道 ${(silentTime / 1000).toFixed(1)} 秒無任何訊息，重新連線續傳...`);
            handlePeerDisconnection('控制通道無回應，正在重新連線...', false);
            return;
        }

        // 2) 檔案進度：控制通道活著但檔案不動 (例如 fileChannel 已死、發送迴圈已退出)
        const stallTime = now - lastProgressTime;
        if (stallTime > PROGRESS_HARD_TIMEOUT_MS) {
            console.error(`[看門狗] 檔案傳輸 ${(stallTime / 1000).toFixed(1)} 秒無進度，改走完整重連...`);
            handlePeerDisconnection('傳輸長時間無進度，正在重新連線...', false);
            return;
        }
        if (stallTime > PROGRESS_SOFT_TIMEOUT_MS && !progressRecoveryAttempted) {
            progressRecoveryAttempted = true;
            console.warn(`[看門狗] 檔案傳輸 ${(stallTime / 1000).toFixed(1)} 秒無進度，嘗試通道內修復...`);
            if (!state.fileChannel || state.fileChannel.readyState !== 'open') {
                requestFileChannelReopen('看門狗偵測到檔案通道不可用');
            } else {
                triggerInBandRenegotiation();
            }
        }
    }, 1000);
}

function clearDisconnectedGraceTimer() {
    if (disconnectedGraceTimer) {
        clearTimeout(disconnectedGraceTimer);
        disconnectedGraceTimer = null;
    }
}

function scheduleDisconnectedGraceTimeout(pc) {
    clearDisconnectedGraceTimer();
    disconnectedGraceTimer = setTimeout(() => {
        disconnectedGraceTimer = null;
        if (pc !== state.peerConnection) return;
        const connState = pc.connectionState;
        const iceState = pc.iceConnectionState;
        if (connState === 'disconnected' || iceState === 'disconnected') {
            console.warn(`[ICE 快速重連] 網路處於 disconnected 超過 ${DISCONNECTED_GRACE_MS / 1000} 秒未恢復，主動發起重新連線...`);
            handlePeerDisconnection('網路連線中斷，正在重新連線...', false);
        }
    }, DISCONNECTED_GRACE_MS);
}

function stopHeartbeatAndWatchdog() {
    clearDisconnectedGraceTimer();
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
        if (pc !== state.peerConnection) return;
        console.log(`[WebRTC 連線狀態變更]: ${pc.connectionState}`);
        if (pc.connectionState === 'disconnected') {
            console.warn('[WebRTC 狀態] 連線短暫中斷 (disconnected)，等待底層 ICE 復原...');
            updateStatus('網路連線波動中，等待恢復...', 'yellow');
            scheduleDisconnectedGraceTimeout(pc);
        } else if (pc.connectionState === 'failed') {
            clearDisconnectedGraceTimer();
            handlePeerDisconnection('對端連線狀態失敗 (failed)', false);
        } else if (pc.connectionState === 'closed') {
            clearDisconnectedGraceTimer();
            handlePeerDisconnection('對端連線已關閉 (closed)', false);
        } else if (pc.connectionState === 'connected') {
            clearDisconnectedGraceTimer();
            updateStatus('P2P 直連已開通 (加密)', 'green');
            // 連線建立後依協商結果動態決定 chunk size
            negotiateChunkSize();
        }
    };

    pc.oniceconnectionstatechange = () => {
        if (pc !== state.peerConnection) return;
        console.log(`[ICE 狀態變更]: ${pc.iceConnectionState}`);
        if (pc.iceConnectionState === 'disconnected') {
            console.warn('[ICE 狀態] ICE 暫時中斷，等待重連...');
            scheduleDisconnectedGraceTimeout(pc);
        } else if (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed') {
            clearDisconnectedGraceTimer();
        } else if (pc.iceConnectionState === 'failed') {
            clearDisconnectedGraceTimer();
            handlePeerDisconnection('ICE 連線失敗 (failed)', false);
        } else if (pc.iceConnectionState === 'closed') {
            clearDisconnectedGraceTimer();
            handlePeerDisconnection('ICE 連線已關閉 (closed)', false);
        }
    };
}

// 監聽視窗卸載主動向對端告警
window.addEventListener('beforeunload', () => {
    if (state.stateChannel?.readyState === 'open') {
        try { state.stateChannel.send(JSON.stringify({ type: 'peer-leave' })); } catch (e) { }
    }
});

window.addEventListener('pagehide', () => {
    if (state.stateChannel?.readyState === 'open') {
        try { state.stateChannel.send(JSON.stringify({ type: 'peer-leave' })); } catch (e) { }
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

            let ip = ep.type === 'json' ? (await res.json())[ep.key] : (await res.text()).trim();
            if (ip) return ip;
            throw new Error('解析出空 IP');
        } catch (err) {
            clearTimeout(timeoutId);
            throw err;
        }
    };

    try {
        state.localIPv4 = await Promise.any(endpoints.map(ep => fetchEndpoint(ep)));
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
    if (pinDisplay) pinDisplay.innerText = formattedPin;

    const githubPagesOrigin = window.location.origin + window.location.pathname;
    generateQRCode('qr-canvas', `${githubPagesOrigin}?room=${state.roomId}`);

    try {
        state.peerConnection = new RTCPeerConnection(state.rtcConfig);
        setupPeerConnectionListeners(state.peerConnection);

        state.stateChannel = state.peerConnection.createDataChannel('stateChannel', { ordered: true });
        setupStateChannelListeners();

        state.fileChannel = state.peerConnection.createDataChannel('fileChannel', { ordered: true });
        setupFileChannelListeners();

        state.peerConnection.onicecandidate = (event) => {
            if (event.candidate) sendLocalCandidate(event.candidate);
        };

        // 初次連線用 PIN 碼進房；之後的重連改用 sessionToken (隨 offer 一併交給 Joiner)
        state.sessionToken = null;
        connectSignalingWS();
        state.sessionToken = 'st_' + Math.random().toString(36).substring(2, 10) + Date.now().toString(36);

        const offer = await state.peerConnection.createOffer();
        await state.peerConnection.setLocalDescription(offer);

        const offerPayload = {
            type: 'offer',
            offer: state.peerConnection.localDescription,
            roomId: state.roomId,
            sessionToken: state.sessionToken,
            ipKey: state.localIPv4 ? `ipv4:${state.localIPv4}` : null
        };

        if (state.ws?.readyState === WebSocket.OPEN) {
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
    const targetRoom = state.sessionToken || state.roomId;
    const wsUrl = getWorkerUrl().replace(/^http/, 'ws') + `/ws/${targetRoom}`;
    closeSignalingWS();
    const ws = new WebSocket(wsUrl);
    state.ws = ws;

    ws.onopen = () => {
        if (ws !== state.ws) return;
        if (state.pendingOffer) {
            ws.send(JSON.stringify(state.pendingOffer));
            state.pendingOffer = null;
            updateStatus('信令通道已就緒，等待連線...', 'yellow');
        }
        while (localCandidates.length > 0) {
            ws.send(JSON.stringify(localCandidates.shift()));
        }

        // 保活：Cloudflare 會切斷閒置 100 秒的 WebSocket；DO 端以 setWebSocketAutoResponse 回 pong，不會喚醒 DO
        stopWsKeepalive();
        wsKeepaliveTimer = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
                try { ws.send('ping'); } catch (e) { }
            } else if (ws === state.ws) {
                stopWsKeepalive();
            }
        }, WS_KEEPALIVE_INTERVAL_MS);
    };

    ws.onmessage = async (event) => {
        if (event.data === 'pong') return;
        try {
            handleSignalingMessage(JSON.parse(event.data));
        } catch (e) {
            console.error("解析 JSON 失敗:", e);
        }
    };

    ws.onerror = () => {
        if (ws !== state.ws) return;
        console.warn('[信令 WebSocket 錯誤] (可能被 DO 限流 429 / 房間已滿 409 / 網路中斷)');
        // 重連期間 WS 失敗必須立即重試，否則重連會永久卡住
        if (isReconnecting) failReconnectAttempt('信令 WebSocket 連線失敗');
    };

    ws.onclose = (event) => {
        if (ws !== state.ws) return;
        console.log(`[信令 WebSocket 關閉] code=${event.code} reason=${event.reason || '-'}`);
        stopWsKeepalive();
        state.ws = null;
        if (isReconnecting) failReconnectAttempt(`信令 WebSocket 關閉 (code=${event.code})`);
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
    // 初次加入一律以 PIN 碼進房，sessionToken 會在收到 offer 時由 Host 下發
    state.sessionToken = null;

    document.getElementById('setup-view').classList.add('hidden');
    document.getElementById('signaling-box').classList.remove('hidden');
    document.getElementById('joiner-panel').classList.remove('hidden');
    updateStatus('正在連接 DO 信令房間...', 'yellow');

    try {
        state.peerConnection = new RTCPeerConnection(state.rtcConfig);
        setupPeerConnectionListeners(state.peerConnection);

        state.peerConnection.ondatachannel = handleIncomingDataChannel;

        state.peerConnection.onicecandidate = (event) => {
            if (event.candidate) sendLocalCandidate(event.candidate);
        };

        connectSignalingWS();
    } catch (e) {
        showToast(e.message, 'error');
        updateStatus('連線建立中斷', 'red');
    }
}

/**
 * 設定 Channel 事件監聽兩條獨立通道的監聽函式
 * 每組 handler 都綁定建立當下的通道實例 (ch)，若事件來自已被替換的舊通道則忽略。
 */
export function setupStateChannelListeners() {
    const ch = state.stateChannel;
    if (!ch) return;

    ch.onopen = () => {
        if (ch !== state.stateChannel) return;
        console.log('[stateChannel] 控制通道已就緒');
        const isResumeOrReconnect = isReconnecting || reconnectAttempts > 0 || Boolean(state.incomingFileInfo) || state.receivedSize > 0;
        clearReconnectTimers();
        isDisconnectHandled = false;
        isReconnecting = false;
        reconnectAttempts = 0;
        lastActivityTime = Date.now();

        updateStatus('P2P 直連已開通 (加密雙通道)', 'green');
        if (!isResumeOrReconnect) {
            showToast('P2P 控制通道已開通', 'success');
            triggerAppleGlow();
        } else {
            showToast('已完成背景金鑰重連', 'info');
        }

        startHeartbeatAndWatchdog();

        if (ackFlushTimer) clearInterval(ackFlushTimer);
        ackFlushTimer = setInterval(() => flushCumulativeAck(), 200);

        try {
            ch.send(JSON.stringify({
                type: 'device-info',
                isMobile: state.localIsMobile
            }));
        } catch (e) { }
        /* 保留休眠連線以實現毫秒級重連
        if (state.ws) {
            state.ws.close();
            state.ws = null;
        }
        */
        document.getElementById('setup-view')?.classList.add('hidden');
        document.getElementById('signaling-box')?.classList.add('hidden');
        const tfPanel = document.getElementById('transfer-panel');
        if (tfPanel) {
            tfPanel.classList.remove('hidden');
            tfPanel.style.opacity = '1';
        }
    };

    ch.onclose = () => {
        if (ch !== state.stateChannel) return;
        console.warn('[stateChannel] 控制通道斷開，執行整體連線清理');
        handlePeerDisconnection('控制通道中斷', false);
    };

    ch.onerror = (err) => {
        if (ch !== state.stateChannel) return;
        console.error('[stateChannel 異常]:', err);
    };

    ch.onmessage = (event) => {
        if (ch !== state.stateChannel) return;
        lastActivityTime = Date.now();
        if (typeof event.data === 'string') {
            handleControlMessage(event.data);
        }
    };
}

export function setupFileChannelListeners() {
    const ch = state.fileChannel;
    if (!ch) return;

    ch.binaryType = 'arraybuffer';
    ch.bufferedAmountLowThreshold = BUFFERED_LOW_THRESHOLD;

    ch.onbufferedamountlow = () => {
        if (ch !== state.fileChannel) return;
        if (bufferLowResolver) {
            const resolve = bufferLowResolver;
            bufferLowResolver = null;
            resolve();
        }
    };

    ch.onopen = () => {
        if (ch !== state.fileChannel) return;
        console.log('[fileChannel] 檔案傳輸通道已開通');
        markProgress();
        if (state.incomingFileInfo && state.receivedSize < state.incomingFileInfo.size) {
            recvQueue = [];
            inFlightBytes = 0;
            currentOffset = state.receivedSize;
            lastAckedOffset = state.receivedSize;

            document.getElementById('incoming-file-box')?.classList.remove('hidden');
            const recvFileName = document.getElementById('recv-file-name');
            if (recvFileName) recvFileName.innerText = state.incomingFileInfo.name;
            updateRecvUi();

            console.log(`[續傳協商] 發送內部續傳請求，Offset: ${state.receivedSize}`);
            createOPFSWorker(true);
            awaitingResumeMarker = true;

            if (state.stateChannel?.readyState === 'open') {
                state.stateChannel.send(JSON.stringify({
                    type: 'resume-request',
                    name: state.incomingFileInfo.name,
                    receivedOffset: state.receivedSize
                }));
            }
        }
    };

    ch.onclose = () => {
        if (ch !== state.fileChannel) {
            console.log('[fileChannel] 舊通道關閉事件，忽略');
            return;
        }
        if (isDisconnectHandled) {
            console.log('[fileChannel] 已知是主動清理，不再觸發重連');
            return;
        }
        console.warn('[fileChannel] 檔案通道已關閉');

        // 立即終止綁在此通道上的發送迴圈，避免舊迴圈在新通道開通後以錯誤 offset 繼續送資料
        currentTransferSessionId++;
        isFlowPausedByRemote = false;
        wakeTransferWaiters();

        if (state.stateChannel?.readyState === 'open') {
            // 已關閉的 DataChannel 無法靠 ICE Restart 復活，改為在現有 SCTP 連線上重開一條 (RFC 8832 DCEP)
            console.log('[fileChannel] stateChannel 仍存活，於現有連線上重開檔案通道');
            requestFileChannelReopen('檔案通道關閉');
        } else {
            console.warn('[fileChannel] stateChannel 也不可用，進入完整重連');
            handlePeerDisconnection('檔案通道與控制通道皆不可用', false);
        }
    };

    ch.onerror = (err) => {
        if (ch !== state.fileChannel || isDisconnectHandled) return;
        // 通道錯誤後必定伴隨 close 事件，修復交由 onclose 統一處理，避免重複觸發
        console.error('[fileChannel 異常]:', err);
    };

    ch.onmessage = (event) => {
        // 舊通道遲到的資料一律丟棄，否則會以錯誤 offset 寫入檔案
        if (ch !== state.fileChannel) return;
        lastActivityTime = Date.now();
        if (typeof event.data === 'string') {
            handleFileChannelControl(event.data);
            return;
        }
        if (awaitingResumeMarker) return; // 續傳標記前的舊 session 資料，丟棄
        handleIncomingData(event.data);
    };
}

/**
 * fileChannel 上的文字控制訊息 (目前僅 resume-marker)
 * 標記與檔案資料走同一條 ordered 串流，因此標記之後的資料必定屬於新 session，offset 不會錯位。
 */
function handleFileChannelControl(text) {
    try {
        const msg = JSON.parse(text);
        if (msg.type === 'resume-marker' && typeof msg.offset === 'number') {
            if (msg.offset !== state.receivedSize) {
                console.warn(`[續傳標記] 發送端 offset (${msg.offset}) 與本地已寫入量 (${state.receivedSize}) 不一致`);
            }
            recvQueue = [];
            currentOffset = msg.offset;
            awaitingResumeMarker = false;
            markProgress();
            console.log(`[續傳標記] 從 ${(msg.offset / (1024 * 1024)).toFixed(2)} MB 開始接收新 session 資料`);
        }
    } catch (e) {
        console.warn('[fileChannel] 文字訊息解析失敗:', e);
    }
}

/**
 * 請求重開 fileChannel (只由 Host 建立，避免雙方同時建立兩條通道)
 */
function requestFileChannelReopen(reason) {
    if (isDisconnectHandled) return;
    if (!state.peerConnection || state.stateChannel?.readyState !== 'open') {
        handlePeerDisconnection(`${reason}，且控制通道不可用`, false);
        return;
    }

    const now = Date.now();
    fileReopenTimestamps = fileReopenTimestamps.filter(t => now - t < 60000);
    if (fileReopenTimestamps.length >= MAX_FILE_REOPEN_PER_MINUTE) {
        console.warn('[fileChannel] 一分鐘內重開次數過多，改走完整重連');
        handlePeerDisconnection('檔案通道反覆中斷，正在重新連線...', false);
        return;
    }
    fileReopenTimestamps.push(now);

    showToast('檔案通道中斷，正在通道內快速修復...', 'warning');
    if (state.isHost) {
        reopenFileChannelAsHost();
    } else {
        try {
            state.stateChannel.send(JSON.stringify({ type: 'inband-reopen-file' }));
        } catch (e) {
            handlePeerDisconnection('無法請求重開檔案通道', false);
        }
    }
}

/**
 * Host 於現有 PeerConnection 上建立新的 fileChannel。
 * SCTP association 已存在時，createDataChannel 透過 DCEP 直接開通，不需 SDP 重新協商。
 */
function reopenFileChannelAsHost() {
    const pc = state.peerConnection;
    if (!pc) return;
    const old = state.fileChannel;
    if (old && (old.readyState === 'open' || old.readyState === 'connecting')) {
        console.log(`[fileChannel] 目前通道狀態為 ${old.readyState}，不需重開`);
        return;
    }
    detachAndCloseChannel(old);
    try {
        state.fileChannel = pc.createDataChannel('fileChannel', { ordered: true });
        setupFileChannelListeners();
        console.log('[fileChannel] Host 已建立新的檔案通道，等待開通後續傳');
    } catch (err) {
        console.error('[fileChannel] 重開失敗:', err);
        handlePeerDisconnection('重開檔案通道失敗', false);
    }
}

/**
 * 免經過 DO 信令伺服器的「通道內極速重新握手 (In-Band Renegotiation)」
 * offer/answer 與新一代 ICE candidates 全部經由 stateChannel 交換 (見 sendLocalCandidate)。
 */
let isInBandRenegotiating = false;
let inBandLockTimer = null;
const INBAND_LOCK_TIMEOUT_MS = 10000;
// In-Band 信令依序處理，避免 offer/answer/candidate 交錯執行
let inBandQueue = Promise.resolve();

function acquireInBandLock() {
    isInBandRenegotiating = true;
    if (inBandLockTimer) clearTimeout(inBandLockTimer);
    // 保底解鎖：對端無回應時不讓鎖永久卡住
    inBandLockTimer = setTimeout(() => {
        inBandLockTimer = null;
        isInBandRenegotiating = false;
    }, INBAND_LOCK_TIMEOUT_MS);
}

function releaseInBandLock() {
    if (inBandLockTimer) {
        clearTimeout(inBandLockTimer);
        inBandLockTimer = null;
    }
    isInBandRenegotiating = false;
}

// Host 發起 ICE Restart offer
async function startHostIceRestart() {
    const pc = state.peerConnection;
    if (!pc || state.stateChannel?.readyState !== 'open') return;
    if (pc.signalingState !== 'stable') {
        console.warn(`[In-Band 重連] signalingState=${pc.signalingState}，已有協商進行中，略過`);
        return;
    }
    const offer = await pc.createOffer({ iceRestart: true });
    await pc.setLocalDescription(offer);
    state.stateChannel.send(JSON.stringify({
        type: 'inband-offer',
        offer: pc.localDescription
    }));
}

export async function triggerInBandRenegotiation() {
    if (isInBandRenegotiating) {
        console.warn('[In-Band 重連] 已有一個修復流程進行中，忽略重複請求');
        return;
    }
    if (!state.stateChannel || state.stateChannel.readyState !== 'open') {
        console.warn('[In-Band 重連] stateChannel 已無法使用，降級回傳統 DO 信令重連');
        handlePeerDisconnection('傳輸通道中斷且無法進行內部重連', false);
        return;
    }
    // 鎖會維持到收到 answer (或逾時)，而不是送出 offer 後立即解除
    acquireInBandLock();

    try {
        console.log('[In-Band 重連] 透過 stateChannel 發起內部 WebRTC ICE Restart...');
        showToast('傳輸停滯，正在進行通道內快速修復...', 'warning');

        if (state.isHost) {
            await startHostIceRestart();
        } else {
            state.stateChannel.send(JSON.stringify({ type: 'inband-request-reconnect' }));
        }
    } catch (err) {
        console.error('[In-Band 重連失敗]:', err);
        releaseInBandLock();
        handlePeerDisconnection('內部快速重連失敗', false);
    }
}

/**
 * 處理經由 stateChannel 傳遞的 In-Band 信令 (依序執行)
 */
function handleInBandSignaling(message) {
    inBandQueue = inBandQueue.then(async () => {
        const pc = state.peerConnection;
        if (!pc) return;
        try {
            if (message.type === 'inband-candidate') {
                if (!message.candidate) return;
                if (pc.remoteDescription?.type) {
                    await pc.addIceCandidate(message.candidate);
                } else {
                    pendingCandidates.push(message.candidate);
                }
            } else if (message.type === 'inband-request-reconnect' && state.isHost) {
                acquireInBandLock();
                await startHostIceRestart();
            } else if (message.type === 'inband-offer' && !state.isHost) {
                await pc.setRemoteDescription(message.offer);
                const answer = await pc.createAnswer();
                await pc.setLocalDescription(answer);
                state.stateChannel?.send(JSON.stringify({ type: 'inband-answer', answer: pc.localDescription }));
                releaseInBandLock();
            } else if (message.type === 'inband-answer' && state.isHost) {
                if (pc.signalingState === 'have-local-offer') {
                    await pc.setRemoteDescription(message.answer);
                    console.log('[In-Band 重連] ICE Restart 協商完成');
                }
                releaseInBandLock();
            } else if (message.type === 'inband-reopen-file' && state.isHost) {
                reopenFileChannelAsHost();
            }
        } catch (err) {
            console.warn(`[In-Band 信令] 處理 ${message.type} 失敗:`, err);
        }
    });
}

// 獨立控制訊息邏輯 handleControlMessage (由 stateChannel 呼叫)
export async function handleControlMessage(jsonString) {
    try {
        const message = JSON.parse(jsonString);

        if (message.type === 'ping') {
            if (state.stateChannel?.readyState === 'open') {
                state.stateChannel.send(JSON.stringify({ type: 'pong' }));
            }
            return;
        }

        if (message.type === 'pong') {
            lastActivityTime = Date.now();
            return;
        }

        if (message.type && message.type.startsWith('inband-')) {
            handleInBandSignaling(message);
            return;
        }

        if (message.type === 'ack') {
            lastActivityTime = Date.now();
            if (typeof message.offset === 'number') {
                // 接收端確認寫入的位置有前進 = 檔案傳輸有實質進度
                if (message.offset > lastConfirmedAckOffset) markProgress();
                lastConfirmedAckOffset = message.offset;
                if (ackWindowResolver) {
                    const resolve = ackWindowResolver;
                    ackWindowResolver = null;
                    resolve();
                }
            }
            return;
        }

        // 【接收端 流量控制】發送端響應接收端的暫停/恢復請求
        if (message.type === 'flow-pause') {
            isFlowPausedByRemote = true;
            console.log('[發送端 流量控制] 接收端要求暫停，等待恢復信號...');
            return;
        }

        if (message.type === 'flow-resume') {
            isFlowPausedByRemote = false;
            if (flowResumeResolver) {
                const resolve = flowResumeResolver;
                flowResumeResolver = null;
                resolve();
            }
            console.log('[發送端 流量控制] 接收端已消化積壓，恢復傳輸');
            return;
        }

        if (message.type === 'resume-request') {
            console.log(`[發送端] 收到續傳請求 Offset: ${(message.receivedOffset / (1024 * 1024)).toFixed(2)} MB`);
            showToast(`正在從 ${(message.receivedOffset / (1024 * 1024)).toFixed(1)} MB 處恢復傳輸...`, 'info');

            state.stateChannel.send(JSON.stringify({
                type: 'resume-ack',
                startOffset: message.receivedOffset
            }));

            sendFileChunks(message.receivedOffset);
            return;
        }

        if (message.type === 'resume-ack') {
            // 寫入位置的校正改由 fileChannel 上的 resume-marker 負責 (與資料同一條有序串流，不會錯位)
            if (!opfsWorker) {
                createOPFSWorker(true);
            } else if (isWorkerReady) {
                drainRecvQueue();
            }
            return;
        }

        if (message.type === 'file-meta') {
            acquireWakeLock();

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
            lastRecvSpeedTime = performance.now();
            lastRecvSpeedBytes = state.receivedSize;
            smoothRecvSpeed = 0;
            recvQueue = [];
            inFlightBytes = 0;

            const isResume = state.receivedSize > 0;
            createOPFSWorker(isResume);

            if (isResume) {
                // 發送端此時可能已從 offset 0 開始送資料，續傳標記到達前收到的資料一律丟棄
                currentOffset = state.receivedSize;
                awaitingResumeMarker = true;
                state.stateChannel.send(JSON.stringify({
                    type: 'resume-request',
                    name: message.name,
                    receivedOffset: state.receivedSize
                }));
            } else {
                awaitingResumeMarker = false;
            }

            document.getElementById('incoming-file-box')?.classList.remove('hidden');
            const recvFileName = document.getElementById('recv-file-name');
            if (recvFileName) recvFileName.innerText = state.incomingFileInfo.name;
            updateRecvUi();
        } else if (message.type === 'file-end') {
            drainRecvQueue();
        }
    } catch (e) {
        console.error('[stateChannel] 控制訊息解析失敗:', e);
    }
}

function updateRecvUi() {
    if (state.incomingFileInfo && state.incomingFileInfo.size > 0) {
        const now = performance.now();
        const totalSize = state.incomingFileInfo.size;

        // 進度條：每 80ms 刷新一次以保持流暢視覺
        if (now - state.lastRecvUiTime > 80 || state.receivedSize >= totalSize) {
            state.lastRecvUiTime = now;
            const percent = Math.min(100, Math.round((state.receivedSize / totalSize) * 100));

            const recvPercent = document.getElementById('recv-percent');
            const recvProgressBar = document.getElementById('recv-progress-bar');
            if (recvPercent) recvPercent.innerText = `${percent}%`;
            if (recvProgressBar) recvProgressBar.style.width = `${percent}%`;

            // 速度與 ETA 計算：每 500ms (0.5秒) 採樣一次
            const speedElapsed = (now - lastRecvSpeedTime) / 1000;
            if (speedElapsed >= 0.5 || state.receivedSize >= totalSize) {
                const instantSpeed = (state.receivedSize - lastRecvSpeedBytes) / speedElapsed;
                // 平滑濾波：保留 85% 舊速率 + 15% 新速率
                smoothRecvSpeed = smoothRecvSpeed === 0 ? instantSpeed : smoothRecvSpeed * 0.85 + instantSpeed * 0.15;
                lastRecvSpeedBytes = state.receivedSize;
                lastRecvSpeedTime = now;

                // 更新接收端速度 (請確認 HTML 中是否有對應元素，如 #recv-speed)
                const recvSpeedText = document.getElementById('recv-speed');
                if (recvSpeedText) {
                    recvSpeedText.innerText = `${(smoothRecvSpeed / (1024 * 1024)).toFixed(2)} MB/s`;
                }

                // 更新接收端剩餘時間 (請確認 HTML 中是否有對應元素，如 #recv-time)
                const remainingBytes = Math.max(0, totalSize - state.receivedSize);
                const etaSeconds = smoothRecvSpeed > 0 ? remainingBytes / smoothRecvSpeed : 0;

                const recvTimeText = document.getElementById('recv-time');
                if (recvTimeText) {
                    recvTimeText.innerText = percent >= 100 ? '00:00' : formatEta(etaSeconds);
                }
            }
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

    // 【接收端 流量控制】積壓已消化，通知發送端恢復傳輸
    if (isPausedRemote && recvQueue.length <= RESUME_QUEUE_COUNT && inFlightBytes < MAX_IN_FLIGHT_BYTES) {
        isPausedRemote = false;
        try {
            state.stateChannel?.send(JSON.stringify({ type: 'flow-resume' }));
            console.log('[接收端 流量控制] 佇列已消化，恢復傳輸');
        } catch (e) { }
    }

    if (recvQueue.length > 100) {
        console.warn(`[接收端 佇列積壓] 隊列待處理封包: ${recvQueue.length} 個 | Worker 寫入中記憶體 (inFlight): ${(inFlightBytes / 1024).toFixed(1)} KB`);
    }

    const isTotalReceived = state.incomingFileInfo && state.receivedSize >= state.incomingFileInfo.size;
    if (isTotalReceived && recvQueue.length === 0 && inFlightBytes === 0 && opfsWorker && !isFinishSent) {
        console.log(`[接收端 準備收尾] 總接收已達標 (${(state.receivedSize / (1024 * 1024)).toFixed(2)} MB)，通知 Worker 執行 Finish...`);
        isFinishSent = true;
        opfsWorker.postMessage({ type: 'finish' });
    }

}

export function handleIncomingData(data) {
    recvQueue.push(data);
    lastActivityTime = Date.now();
    markProgress();

    // 【接收端流量控制】當佇列積壓過大時，通知發送端暫停，
    // 避免 iOS Safari 因記憶體壓力直接終止頁面導致「斷線」。
    if (!isPausedRemote && state.incomingFileInfo &&
        recvQueue.length > PAUSE_QUEUE_COUNT) {
        isPausedRemote = true;
        try {
            state.stateChannel?.send(JSON.stringify({ type: 'flow-pause' }));
            console.warn(`[接收端 流量控制] 佇列積壓 ${recvQueue.length} 個分塊，請求發送端暫停`);
        } catch (e) { }
    }

    if (isWorkerReady) {
        drainRecvQueue();
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

// 等待 DataChannel 開通 (逾時則放棄)
function waitForChannelOpen(ch, timeoutMs) {
    return new Promise((resolve) => {
        if (!ch || ch.readyState !== 'connecting') return resolve();
        const timer = setTimeout(done, timeoutMs);
        function done() {
            clearTimeout(timer);
            ch.removeEventListener('open', done);
            ch.removeEventListener('close', done);
            resolve();
        }
        ch.addEventListener('open', done);
        ch.addEventListener('close', done);
    });
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

    // 重開通道時，接收端的 resume-request 可能比本端 fileChannel 的 open 事件更早到達，稍候片刻
    if (state.fileChannel?.readyState === 'connecting') {
        await waitForChannelOpen(state.fileChannel, 10000);
    }

    if (!state.fileChannel || state.fileChannel.readyState !== 'open') {
        console.warn('[發送端] 通道尚未就緒，等待連線建立...');
        showToast('通道尚未建立，請確認 P2P 連線狀態', 'warning');
        return;
    }

    // 綁定本次 session 使用的通道；通道被替換或關閉時迴圈立即退出，絕不在新通道上以舊 offset 送資料
    const ch = state.fileChannel;
    const channelAlive = () => ch === state.fileChannel && ch.readyState === 'open';

    const mySessionId = ++currentTransferSessionId;
    isSendingInProgress = true;
    sendTransferActive = true;
    markProgress();

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
        console.log(`[發送端 開始新傳輸] 檔名: ${state.selectedFile.name}`);
        state.stateChannel.send(JSON.stringify({
            type: 'file-meta',
            name: state.selectedFile.name,
            size: state.selectedFile.size
        }));
    } else {
        console.log(`[發送端 斷點續傳] Session #${mySessionId} 從 ${(offset / (1024 * 1024)).toFixed(2)} MB 處接續傳輸`);
    }

    // 續傳時先在 fileChannel 上送出標記，接收端據此對齊寫入位置並丟棄標記前的舊資料
    if (typeof startOffset === 'number') {
        try {
            ch.send(JSON.stringify({ type: 'resume-marker', offset }));
        } catch (e) {
            console.warn('[發送端] resume-marker 發送失敗:', e);
        }
    }

    const CHUNK_SIZE_LOCAL = CHUNK_SIZE;
    const BLOCK_SIZE = 4 * 1024 * 1024; // 4MB 區塊搭配雙緩衝預讀 (Double Buffering)
    bufferLowResolver = null;
    ackWindowResolver = null;
    flowResumeResolver = null;
    let backpressureStallStartedAt = 0;
    let lastBackpressureBuffered = 0;
    let lastBackpressureAck = lastConfirmedAckOffset;
    let recoveryRequested = false;

    let lastUiUpdateTime = 0;
    let lastSpeedTime = performance.now();
    let lastSpeedBytes = offset;
    let smoothSpeed = 0;

    try {
        const totalSize = state.selectedFile.size;

        // 雙緩衝預讀輔助函式：在傳送當前區塊時，背景非同步讀取下一個 4MB 區塊，消除磁碟 I/O 等待空窗
        const readBlockAt = (start) => {
            if (start >= totalSize) return null;
            const end = Math.min(start + BLOCK_SIZE, totalSize);
            return state.selectedFile.slice(start, end).arrayBuffer();
        };

        let nextBlockPromise = readBlockAt(offset);

        while (offset < totalSize) {
            if (mySessionId !== currentTransferSessionId) {
                console.warn(`[發送端] Session #${mySessionId} 已過期，由新 Session 接手，退出迴圈`);
                return;
            }

            if (!channelAlive()) {
                console.warn(`[發送端 中斷暫停] 通道中斷，傳輸停在 ${(offset / (1024 * 1024)).toFixed(2)} MB`);
                isSendingInProgress = false;
                return;
            }

            const blockBuffer = await (nextBlockPromise || readBlockAt(offset));
            if (!blockBuffer) break;

            const blockLength = blockBuffer.byteLength;
            // 立即啟動下一個區塊的預讀
            nextBlockPromise = readBlockAt(offset + blockLength);

            let blockOffset = 0;
            let nextYieldAt = 1024 * 1024;

            while (blockOffset < blockLength) {
                if (mySessionId !== currentTransferSessionId) return;

                if (!channelAlive()) {
                    console.warn(`[發送端 中斷暫停] 通道中斷，傳輸停在 ${(offset / (1024 * 1024)).toFixed(2)} MB`);
                    isSendingInProgress = false;
                    return;
                }

                // 【ACK 窗口流控】使用事件驅動 Promise 等待 ACK 到達，不輪詢 setTimeout (避免背景分頁節流)
                while (offset - lastConfirmedAckOffset > MAX_UNACKED_WINDOW_BYTES) {
                    await new Promise((resolve) => {
                        ackWindowResolver = resolve;
                        setTimeout(() => {
                            if (ackWindowResolver === resolve) {
                                ackWindowResolver = null;
                                resolve();
                            }
                        }, 250);
                    });
                    if (mySessionId !== currentTransferSessionId) return;
                    if (!channelAlive()) {
                        isSendingInProgress = false;
                        return;
                    }
                }

                // 【接收端 流量控制】接收端佇列積壓時暫停發送，由 flow-resume 事件直接喚醒
                // 注意：此等待不計入 backpressure 停滯計時，避免誤觸重連
                while (isFlowPausedByRemote) {
                    await new Promise((resolve) => {
                        flowResumeResolver = resolve;
                        setTimeout(() => {
                            if (flowResumeResolver === resolve) {
                                flowResumeResolver = null;
                                resolve();
                            }
                        }, 250);
                    });
                    if (mySessionId !== currentTransferSessionId) return;
                    if (!channelAlive()) {
                        isSendingInProgress = false;
                        return;
                    }
                }

                if (ch.bufferedAmount > BUFFERED_HIGH_THRESHOLD) {
                    if (backpressureStallStartedAt === 0) {
                        backpressureStallStartedAt = performance.now();
                        lastBackpressureBuffered = ch.bufferedAmount;
                        lastBackpressureAck = lastConfirmedAckOffset;
                    }

                    while (ch.bufferedAmount > BUFFERED_LOW_THRESHOLD) {
                        await new Promise((resolve) => {
                            bufferLowResolver = resolve;
                            setTimeout(() => {
                                if (bufferLowResolver === resolve) {
                                    bufferLowResolver = null;
                                    resolve();
                                }
                            }, BACKPRESSURE_SAMPLE_INTERVAL_MS);
                        });
                        if (mySessionId !== currentTransferSessionId) return;
                        if (!channelAlive()) {
                            isSendingInProgress = false;
                            return;
                        }
                        const now = performance.now();
                        const currentBuffered = ch.bufferedAmount;
                        const currentAck = lastConfirmedAckOffset;
                        if (currentBuffered !== lastBackpressureBuffered || currentAck !== lastBackpressureAck) {
                            // 只要傳送緩衝有消化，代表底層網路仍在推進，同步刷新進度時間
                            if (currentBuffered < lastBackpressureBuffered) markProgress();
                            backpressureStallStartedAt = now;
                            recoveryRequested = false;
                            lastBackpressureBuffered = currentBuffered;
                            lastBackpressureAck = currentAck;
                        } else {
                            const stallDuration = now - backpressureStallStartedAt;
                            if (!recoveryRequested && stallDuration >= BACKPRESSURE_RECOVERY_DELAY_MS) {
                                const pcState = state.peerConnection?.connectionState;
                                const iceState = state.peerConnection?.iceConnectionState;
                                const controlOpen = state.stateChannel?.readyState === 'open';

                                recoveryRequested = true;

                                // 遵循 W3C 規範：背壓停滯僅嘗試無損的 In-Band ICE Restart，絕不在此處直接拆掉 PC
                                // (真正死透的連線會由 ICE disconnected 6s 逾時、connectionState=failed 或看門狗處理)
                                if (controlOpen && pcState === 'connected' && (iceState === 'connected' || iceState === 'completed')) {
                                    try {
                                        await triggerInBandRenegotiation();
                                    } catch (err) {}
                                }
                            }
                        }
                    }

                    backpressureStallStartedAt = 0;
                    lastBackpressureBuffered = 0;
                    lastBackpressureAck = lastConfirmedAckOffset;
                    recoveryRequested = false;
                }

                const nextBlockOffset = Math.min(blockOffset + CHUNK_SIZE_LOCAL, blockLength);
                const chunkSize = nextBlockOffset - blockOffset;
                const chunkView = new Uint8Array(blockBuffer, blockOffset, chunkSize);

                ch.send(chunkView);

                blockOffset += chunkSize;
                offset += chunkSize;
                totalSent = offset;

                // 【修復】每發送 1MB 透過 MessageChannel 零延遲讓出主執行緒，
                // 避免 UI 凍結，且在背景分頁時不會被 Chrome 的 setTimeout 1 秒節流卡住速率
                if (blockOffset >= nextYieldAt) {
                    await yieldToEventLoop();
                    nextYieldAt += 1024 * 1024;
                }

                const now = performance.now();
                if (now - lastUiUpdateTime > 80 || totalSent >= totalSize) {
                    lastUiUpdateTime = now;

                    const percent = Math.min(100, Math.round((totalSent / totalSize) * 100));
                    const progPercent = document.getElementById('progress-percent');
                    const progBar = document.getElementById('progress-bar');
                    if (progPercent) progPercent.innerText = `${percent}%`;
                    if (progBar) progBar.style.width = `${percent}%`;

                    const speedElapsed = (now - lastSpeedTime) / 1000;
                    if (speedElapsed >= 0.5 || totalSent >= totalSize) {
                        const instantSpeed = (totalSent - lastSpeedBytes) / speedElapsed;
                        smoothSpeed = smoothSpeed === 0 ? instantSpeed : smoothSpeed * 0.85 + instantSpeed * 0.15;
                        lastSpeedBytes = totalSent;
                        lastSpeedTime = now;

                        const speedText = document.getElementById('transfer-speed');
                        if (speedText) {
                            speedText.innerText = `${(smoothSpeed / (1024 * 1024)).toFixed(2)} MB/s`;
                        }

                        const remainingBytes = Math.max(0, totalSize - totalSent);
                        const etaSeconds = smoothSpeed > 0 ? remainingBytes / smoothSpeed : 0;

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

        if (state.stateChannel?.readyState === 'open') {
            state.stateChannel.send(JSON.stringify({ type: 'file-end' }));
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
        sendTransferActive = false;
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
        try { opfsWorker.terminate(); } catch (e) { }
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
    isFinishSent = false;
    inFlightBytes = 0;
    wakeTransferWaiters();
    if (forceDelete || isSuccess) {
        currentOffset = 0;
        lastAckedOffset = 0;
    }
}

export async function clearEntireOPFS() {
    if (!navigator.storage?.getDirectory) return;

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