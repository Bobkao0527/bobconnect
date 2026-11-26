/**
 * opfs-worker.js - OPFS 高效零記憶體檔案寫入 Worker
 */

let accessHandle = null;
let fileHandle = null;
let rootDir = null;
let tempFilename = null;
let targetFilename = '';
let targetFileSize = 0;
let workerWriteCount = 0;
let pendingAckBytes = 0;
let ackDebounceTimer = null;
const ACK_BATCH_THRESHOLD = 512 * 1024; // 累積寫入 512KB 再回傳 ACK，減少跨線程 IPC 開銷
const STAGING_BUFFER_SIZE = 2 * 1024 * 1024; // 2MB 聚合寫入緩衝池，大幅減少系統呼叫次數
const stagingBuffer = new Uint8Array(STAGING_BUFFER_SIZE);

/**
 * 將單一連續 Uint8Array 寫入指定位移，並驗證是否完整寫入磁碟
 */
function writeBytesAt(view, targetOffset) {
    let bytesWritten = 0;
    while (bytesWritten < view.byteLength) {
        const subView = view.subarray(bytesWritten);
        const writeStart = performance.now();
        const written = accessHandle.write(
            subView,
            { at: targetOffset + bytesWritten }
        );
        const writeCost = performance.now() - writeStart;
        if (writeCost > 100) {
            console.warn('[OPFS SLOW WRITE]', {
                offset: targetOffset + bytesWritten,
                requestBytes: subView.byteLength,
                writtenBytes: written,
                costMs: writeCost
            });
        }
        if (written === 0) {
            throw new Error(
                `OPFS 寫入中斷：磁碟可能已滿 (已寫入 ${bytesWritten}/${view.byteLength} bytes)`
            );
        }
        bytesWritten += written;
    }
    return bytesWritten;
}

self.addEventListener('error', (err) => {
    console.error('[OPFS Worker 全域錯誤]:', err);
    self.postMessage({ type: 'error', message: err.message || 'Worker 內部例外錯誤' });
});

self.addEventListener('unhandledrejection', (event) => {
    console.error('[OPFS Worker 未捕捉 Promise 例外]:', event.reason);
    self.postMessage({ type: 'error', message: event.reason ? (event.reason.message || String(event.reason)) : '非預期異步錯誤' });
});

function flushAck() {
    if (ackDebounceTimer) {
        clearTimeout(ackDebounceTimer);
        ackDebounceTimer = null;
    }
    if (pendingAckBytes > 0) {
        self.postMessage({
            type: 'write-ack',
            size: pendingAckBytes
        });
        pendingAckBytes = 0;
    }
}

self.onmessage = async (event) => {
    const data = event.data;
    if (!data || !data.type) return;

    switch (data.type) {
        case 'init':
        case 'init-resume': {
            flushAck();
            const { downloadId, size, filename, tempFilename: customTempName } = data;
            const isResume = data.type === 'init-resume';
            targetFilename = filename || 'downloaded_file';
            targetFileSize = size || 0;
            workerWriteCount = 0;
            pendingAckBytes = 0;

            console.log(`[OPFS Worker] 收到初始化請求 (${data.type}): 檔名="${targetFilename}", 預期大小=${(size / (1024 * 1024)).toFixed(2)} MB`);

            try {
                if (navigator.storage && navigator.storage.persist) {
                    await navigator.storage.persist().catch(() => { });
                }

                if (navigator.storage && navigator.storage.estimate) {
                    try {
                        const { quota, usage } = await navigator.storage.estimate();
                        const available = quota - usage;
                        console.log(`[OPFS Worker 空間檢查] 磁碟可用: ${(available / (1024 * 1024)).toFixed(1)} MB / 需使用: ${(size / (1024 * 1024)).toFixed(1)} MB`);
                        if (size && available < size) {
                            throw new Error(`磁碟可用空間不足 (需要 ${(size / (1024 * 1024)).toFixed(1)} MB，可用 ${(available / (1024 * 1024)).toFixed(1)} MB)`);
                        }
                    } catch (quotaErr) {
                        if (quotaErr.message && quotaErr.message.includes('空間不足')) {
                            throw quotaErr;
                        }
                    }
                }

                rootDir = await navigator.storage.getDirectory();
                tempFilename = customTempName || `temp_${downloadId || Date.now()}.tmp`;

                // { create: true } 存在即開啟，不存在即建立，不破壞舊有檔案內容
                fileHandle = await rootDir.getFileHandle(tempFilename, { create: true });

                let retries = 8;
                while (retries > 0) {
                    try {
                        accessHandle = await fileHandle.createSyncAccessHandle();
                        break;
                    } catch (handleErr) {
                        retries--;
                        if (retries === 0) throw handleErr;
                        console.warn(`[OPFS Worker] createSyncAccessHandle 失敗，正在重試 (${retries} 次殘餘)...`, handleErr);
                        await new Promise(resolve => setTimeout(resolve, 250));
                    }
                }

                console.log(`[OPFS Worker] 句柄建立成功 (${isResume ? '續傳' : '全新'})，暫存檔名: ${tempFilename}`);
                self.postMessage({ type: 'init-success' });
            } catch (err) {
                console.error('[OPFS Worker] 初始化失敗:', err);
                await cleanup();
                self.postMessage({ type: 'error', message: err.message || 'OPFS 初始化失敗' });
            }
            break;
        }

        case 'write': {
            const writeMessageStart = performance.now();
            const { chunk, offset, size } = data;
            try {
                if (!accessHandle) {
                    throw new Error('OPFS 寫入句柄未就緒或已關閉');
                }

                workerWriteCount++;
                const view = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
                const bytesWritten = writeBytesAt(view, offset);

                const totalWriteCost = performance.now() - writeMessageStart;
                if (totalWriteCost > 100) {
                    console.warn('[OPFS WRITE MESSAGE SLOW]', {
                        offset,
                        size,
                        actualBytes: bytesWritten,
                        totalCostMs: totalWriteCost,
                        workerWriteCount
                    });
                }

                pendingAckBytes += (bytesWritten || size || view.byteLength);

                // 達到批次閾值即回報，否則啟動 10ms 保底定時器，避免零散封包未被確認
                if (pendingAckBytes >= ACK_BATCH_THRESHOLD) {
                    flushAck();
                } else if (!ackDebounceTimer) {
                    ackDebounceTimer = setTimeout(flushAck, 10);
                }
            } catch (err) {
                console.error(`[OPFS Worker] 寫入第 ${workerWriteCount} 塊時失敗 (offset: ${offset}):`, err);
                await cleanup();
                self.postMessage({ type: 'error', message: err.message || 'OPFS 寫入失敗' });
            }
            break;
        }

        case 'write-batch': {
            const writeMessageStart = performance.now();
            const { chunks, offset, size } = data;
            try {
                if (!accessHandle) {
                    throw new Error('OPFS 寫入句柄未就緒或已關閉');
                }

                workerWriteCount++;
                let currentWriteOffset = offset;
                let stagedLen = 0;
                let totalWritten = 0;

                if (Array.isArray(chunks)) {
                    for (let i = 0; i < chunks.length; i++) {
                        const buf = chunks[i];
                        const view = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
                        if (view.byteLength === 0) continue;

                        if (view.byteLength >= STAGING_BUFFER_SIZE) {
                            if (stagedLen > 0) {
                                const flushed = writeBytesAt(stagingBuffer.subarray(0, stagedLen), currentWriteOffset);
                                currentWriteOffset += flushed;
                                totalWritten += flushed;
                                stagedLen = 0;
                            }
                            const written = writeBytesAt(view, currentWriteOffset);
                            currentWriteOffset += written;
                            totalWritten += written;
                        } else if (stagedLen + view.byteLength > STAGING_BUFFER_SIZE) {
                            const flushed = writeBytesAt(stagingBuffer.subarray(0, stagedLen), currentWriteOffset);
                            currentWriteOffset += flushed;
                            totalWritten += flushed;
                            stagingBuffer.set(view, 0);
                            stagedLen = view.byteLength;
                        } else {
                            stagingBuffer.set(view, stagedLen);
                            stagedLen += view.byteLength;
                        }
                    }
                }

                if (stagedLen > 0) {
                    const flushed = writeBytesAt(stagingBuffer.subarray(0, stagedLen), currentWriteOffset);
                    currentWriteOffset += flushed;
                    totalWritten += flushed;
                    stagedLen = 0;
                }

                const totalWriteCost = performance.now() - writeMessageStart;
                if (totalWriteCost > 100) {
                    console.warn('[OPFS WRITE BATCH SLOW]', {
                        offset,
                        size,
                        actualBytes: totalWritten,
                        totalCostMs: totalWriteCost,
                        workerWriteCount
                    });
                }

                pendingAckBytes += (totalWritten || size || 0);

                if (pendingAckBytes >= ACK_BATCH_THRESHOLD) {
                    flushAck();
                } else if (!ackDebounceTimer) {
                    ackDebounceTimer = setTimeout(flushAck, 10);
                }
            } catch (err) {
                console.error(`[OPFS Worker] 批次寫入第 ${workerWriteCount} 次時失敗 (offset: ${offset}):`, err);
                await cleanup();
                self.postMessage({ type: 'error', message: err.message || 'OPFS 批次寫入失敗' });
            }
            break;
        }

        case 'close-only': {
            // 斷線重連或清理時僅刷新並安全釋放 SyncAccessHandle，保留暫存檔供續傳使用
            flushAck();
            if (accessHandle) {
                try {
                    accessHandle.flush();
                    accessHandle.close();
                } catch (e) { }
                accessHandle = null;
            }
            self.postMessage({ type: 'closed' });
            break;
        }

        case 'finish': {
            try {
                console.log(`[OPFS Worker] 收到結束請求，開始 Flush 與關閉檔案 (共寫入 ${workerWriteCount} 個分塊)...`);
                const startTime = performance.now();

                // 結算殘留的 ACK
                flushAck();

                if (accessHandle) {
                    if (targetFileSize > 0) {
                        try {
                            accessHandle.truncate(targetFileSize);
                        } catch (e) {
                            console.warn('[OPFS Worker] Truncate 失敗，使用原檔案大小:', e);
                        }
                    }
                    accessHandle.flush();
                    accessHandle.close();
                    accessHandle = null;
                }

                if (!fileHandle) throw new Error('OPFS 檔案控制桿不存在');

                const file = await fileHandle.getFile();
                const cost = (performance.now() - startTime).toFixed(1);
                console.log(`[OPFS Worker] 檔案封裝完成 (耗時 ${cost}ms)，實體大小: ${(file.size / (1024 * 1024)).toFixed(2)} MB`);

                self.postMessage({
                    type: 'finish-success',
                    file: file,
                    filename: targetFilename,
                    tempFilename: tempFilename
                });
            } catch (err) {
                console.error('[OPFS Worker] 結束寫入失敗:', err);
                await cleanup();
                self.postMessage({ type: 'error', message: err.message });
            }
            break;
        }

        case 'abort': {
            console.warn('[OPFS Worker] 收到 abort 指令，執行資源清理');
            if (ackDebounceTimer) clearTimeout(ackDebounceTimer);
            await cleanup();
            break;
        }

        default:
            break;
    }
};

async function cleanup() {
    if (accessHandle) {
        try {
            accessHandle.close();
        } catch (e) { }
        accessHandle = null;
    }
    if (rootDir && tempFilename) {
        try {
            await rootDir.removeEntry(tempFilename);
        } catch (e) { }
        tempFilename = null;
        fileHandle = null;
    }
}