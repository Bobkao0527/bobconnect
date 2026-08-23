/**
 * webrtc.js - WebRTC 檔案傳輸與 OPFS Worker 管理核心
 */
import { state } from './state.js';
import { showToast, triggerAutoDownload, acquireWakeLock, releaseWakeLock } from './ui.js';
import { handlePeerDisconnection } from './rtc-channel.js';

let currentDownloadId = null;
let currentTempFilename = null;
let opfsWorker = null;
let currentOffset = 0;

let recvQueue = [];
let isWorkerReady = false;
let fileEndPending = false;
let inFlightBytes = 0;
const MAX_IN_FLIGHT_BYTES = 16 * 1024 * 1024; // 16MB 接收併發窗口

let totalSent = 0;
const BUFFERED_LOW_THRESHOLD = 4 * 1024 * 1024;
const BUFFERED_HIGH_THRESHOLD = 12 * 1024 * 1024;
const MAX_UNACKED_WINDOW_BYTES = 32 * 1024 * 1024;

// 接收端 Cumulative ACK 機制
let lastAckedOffset = 0;
let ackFlushTimer = null;
const BATCH_ACK_INTERVAL = 2 * 1024 * 1024;

let recvPacketCount = 0;
let workerAckCount = 0;
let isFinishSent = false;

let isSendingInProgress = false;
let lastConfirmedAckOffset = 0;
let currentTransferSessionId = 0;

async function removeOPFSFile(filename) {
    if (!filename || !navigator.storage || !navigator.storage.getDirectory) return;
    try {
        const rootDir = await navigator.storage.getDirectory();
        await rootDir.removeEntry(filename);
        console.log(`[OPFS 清理] 暫存檔已安全刪除: ${filename}`);
    } catch (e) { }
}

function flushCumulativeAck() {
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

export function resetTransferState() {
    if (window.bufferLowResolver) {
        const resolve = window.bufferLowResolver;
        window.bufferLowResolver = null;
        resolve();
    }
    currentTransferSessionId++;
    isSendingInProgress = false;
    recvQueue = [];
    inFlightBytes = 0;
    totalSent = 0;
}

/**
 * 建立並初始化 OPFS Worker
 */
export function createOPFSWorker(isResume = false) {
    if (opfsWorker) {
        try { opfsWorker.terminate(); } catch (e) { }
        opfsWorker = null;
    }

    isWorkerReady = false;
    isFinishSent = false;
    fileEndPending = false;

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
        }
        else if (msg.type === 'write-ack') {
            workerAckCount++;
            const writtenSize = msg.size || 0;
            inFlightBytes = Math.max(0, inFlightBytes - writtenSize);
            state.receivedSize += writtenSize;

            if (state.receivedSize - lastAckedOffset >= BATCH_ACK_INTERVAL) {
                flushCumulativeAck();
            }

            updateRecvUi();
            drainRecvQueue();
        }
        else if (msg.type === 'finish-success') {
            releaseWakeLock();
            flushCumulativeAck();

            const { file, filename, tempFilename } = msg;
            const fileToDelete = tempFilename || currentTempFilename;
            const blobUrl = URL.createObjectURL(file);

            console.log(`[接收端 傳輸完成] 檔案大小: ${(file.size / (1024 * 1024)).toFixed(2)} MB`);

            cleanupOPFSWorker(true, false);

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
            }, 60000);
        }
        else if (msg.type === 'error') {
            releaseWakeLock();
            console.error('[接收端] Worker 錯誤:', msg.message);
            handlePeerDisconnection('接收端寫入錯誤: ' + msg.message, true);
        }
    };

    opfsWorker.onerror = (err) => {
        releaseWakeLock();
        console.error('[接收端 OPFS Worker 異常]:', err);
        handlePeerDisconnection('OPFS Worker 內部錯誤', true);
    };

    opfsWorker.postMessage({
        type: isResume ? 'init-resume' : 'init',
        downloadId: currentDownloadId,
        tempFilename: currentTempFilename,
        filename: state.incomingFileInfo?.name || 'downloaded_file',
        size: state.incomingFileInfo?.size || 0
    });

    if (ackFlushTimer) clearInterval(ackFlushTimer);
    ackFlushTimer = setInterval(() => {
        flushCumulativeAck();
    }, 200);

    return true;
}

export function updateRecvUi() {
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

export function drainRecvQueue() {
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

    const isTotalReceived = state.incomingFileInfo && state.receivedSize >= state.incomingFileInfo.size;
    if ((fileEndPending || isTotalReceived) && recvQueue.length === 0 && inFlightBytes === 0 && opfsWorker && !isFinishSent) {
        console.log(`[接收端 準備收尾] 總接收已達標 (${(state.receivedSize / (1024 * 1024)).toFixed(2)} MB)，通知 Worker 執行 Finish...`);
        isFinishSent = true;
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

            if (message.type === 'pong') return;

            if (message.type === 'peer-leave') {
                console.warn('[接收端] 收到對端主動離線通知');
                handlePeerDisconnection('對端已主動離開房間', true);
                return;
            }

            if (message.type === 'ack') {
                if (typeof message.offset === 'number') {
                    lastConfirmedAckOffset = message.offset;
                }
                return;
            }

            if (message.type === 'resume-request') {
                console.log(`[發送端] 收到續傳請求，對端已完成: ${(message.receivedOffset / (1024 * 1024)).toFixed(2)} MB`);
                showToast(`正在從 ${(message.receivedOffset / (1024 * 1024)).toFixed(1)} MB 處恢復傳輸...`, 'info');
                
                state.dataChannel.send(JSON.stringify({
                    type: 'resume-ack',
                    startOffset: message.receivedOffset
                }));

                sendFileChunks(message.receivedOffset);
                return;
            }

            if (message.type === 'resume-ack') {
                console.log(`[接收端] 發送端確認續傳 Offset: ${message.startOffset}`);
                currentOffset = message.startOffset;
                if (!opfsWorker) {
                    createOPFSWorker(true);
                } else if (isWorkerReady) {
                    drainRecvQueue();
                }
                return;
            }

            if (message.type === 'file-meta') {
                acquireWakeLock();
                recvPacketCount = 0;
                workerAckCount = 0;

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
                recvQueue = [];
                inFlightBytes = 0;

                const isResume = state.receivedSize > 0;
                createOPFSWorker(isResume);

                if (isResume) {
                    state.dataChannel.send(JSON.stringify({
                        type: 'resume-request',
                        name: message.name,
                        receivedOffset: state.receivedSize
                    }));
                }

                document.getElementById('incoming-file-box')?.classList.remove('hidden');
                const recvFileName = document.getElementById('recv-file-name');
                if (recvFileName) recvFileName.innerText = state.incomingFileInfo.name;
                updateRecvUi();

            } else if (message.type === 'file-end') {
                fileEndPending = true;
                drainRecvQueue();
            }
        } catch (e) {
            console.error('[接收端] 解析控制訊息失敗:', e);
        }
    } else {
        recvPacketCount++;
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

/**
 * 發送檔案分塊
 */
export async function sendFileChunks(startOffset = null) {
    if (!state.selectedFile) {
        showToast('未選擇檔案', 'error');
        return;
    }

    if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
        console.warn('[發送端] 通道尚未就緒，等待連線建立...');
        return;
    }

    const mySessionId = ++currentTransferSessionId;
    isSendingInProgress = true;

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
        state.dataChannel.send(JSON.stringify({
            type: 'file-meta',
            name: state.selectedFile.name,
            size: state.selectedFile.size
        }));
    }

    const CHUNK_SIZE = 64 * 1024;
    const BLOCK_SIZE = 12 * 1024 * 1024;
    window.bufferLowResolver = null;

    let lastUiUpdateTime = 0;
    let lastSpeedTime = performance.now();
    let lastSpeedBytes = offset;
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
        const totalSize = state.selectedFile.size;

        while (offset < totalSize) {
            if (mySessionId !== currentTransferSessionId) return;

            if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
                isSendingInProgress = false;
                return;
            }

            const blockEnd = Math.min(offset + BLOCK_SIZE, totalSize);
            const blockSlice = state.selectedFile.slice(offset, blockEnd);
            const blockBuffer = await blockSlice.arrayBuffer();

            let blockOffset = 0;
            const blockLength = blockBuffer.byteLength;

            while (blockOffset < blockLength) {
                if (mySessionId !== currentTransferSessionId) return;

                if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
                    isSendingInProgress = false;
                    return;
                }

                while (offset - lastConfirmedAckOffset > MAX_UNACKED_WINDOW_BYTES) {
                    await new Promise(resolve => setTimeout(resolve, 30));
                    if (mySessionId !== currentTransferSessionId) return;
                    if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
                        isSendingInProgress = false;
                        return;
                    }
                }

                if (state.dataChannel.bufferedAmount > BUFFERED_HIGH_THRESHOLD) {
                    let stagnationCount = 0;
                    let lastBufferedAmount = state.dataChannel.bufferedAmount;

                    while (state.dataChannel.bufferedAmount > BUFFERED_LOW_THRESHOLD) {
                        await new Promise((resolve) => {
                            window.bufferLowResolver = resolve;
                            setTimeout(() => {
                                if (window.bufferLowResolver === resolve) {
                                    window.bufferLowResolver = null;
                                    resolve();
                                }
                            }, 50);
                        });

                        if (mySessionId !== currentTransferSessionId) return;

                        if (!state.dataChannel || state.dataChannel.readyState !== 'open') {
                            isSendingInProgress = false;
                            return;
                        }

                        const currentBuffered = state.dataChannel.bufferedAmount;
                        if (currentBuffered >= lastBufferedAmount) {
                            stagnationCount++;
                            if (stagnationCount >= 100) {
                                handlePeerDisconnection('發送背壓停滯超時', false);
                                return;
                            }
                        } else {
                            stagnationCount = 0;
                            lastBufferedAmount = currentBuffered;
                        }
                    }
                }

                const nextBlockOffset = Math.min(blockOffset + CHUNK_SIZE, blockLength);
                const chunkSize = nextBlockOffset - blockOffset;
                const chunkView = new Uint8Array(blockBuffer, blockOffset, chunkSize);

                state.dataChannel.send(chunkView);

                blockOffset += chunkSize;
                offset += chunkSize;
                totalSent = offset;

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
        }

        if (mySessionId !== currentTransferSessionId) return;

        if (state.dataChannel && state.dataChannel.readyState === 'open') {
            state.dataChannel.send(JSON.stringify({ type: 'file-end' }));
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
 * 釋放 OPFS Worker 與內部資源
 */
export function cleanupOPFSWorker(isSuccess = false, forceDelete = false) {
    if (opfsWorker) {
        try {
            opfsWorker.terminate();
        } catch (e) { }
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
    fileEndPending = false;
    isFinishSent = false;
    inFlightBytes = 0;
    window.bufferLowResolver = null;
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