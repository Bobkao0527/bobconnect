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
const ACK_BATCH_THRESHOLD = 256 * 1024; // 累積寫入 256KB 再回傳 ACK，減少 75% 的跨線程 IPC

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

                try {
                    fileHandle = await rootDir.getFileHandle(tempFilename, { create: !isResume });
                } catch (e) {
                    if (isResume) {
                        console.warn(`[OPFS Worker] 續傳檔案 ${tempFilename} 不存在，fallback 建立新檔`);
                        fileHandle = await rootDir.getFileHandle(tempFilename, { create: true });
                    } else {
                        throw e;
                    }
                }

                let retries = 5;
                while (retries > 0) {
                    try {
                        accessHandle = await fileHandle.createSyncAccessHandle();
                        break;
                    } catch (handleErr) {
                        retries--;
                        if (retries === 0) throw handleErr;
                        console.warn(`[OPFS Worker] createSyncAccessHandle 失敗，正在重試 (${retries} 次殘餘)...`, handleErr);
                        await new Promise(resolve => setTimeout(resolve, 200));
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
            const { chunk, offset, size } = data;
            try {
                if (!accessHandle) {
                    throw new Error('OPFS 寫入句柄未就緒或已關閉');
                }

                workerWriteCount++;
                const view = new Uint8Array(chunk);
                let bytesWritten = 0;
                
                while (bytesWritten < view.byteLength) {
                    const subView = view.subarray(bytesWritten);
                    const written = accessHandle.write(subView, { at: offset + bytesWritten });
                    if (written === 0) {
                        throw new Error(`OPFS 寫入中斷：磁碟可能已滿 (已寫入 ${bytesWritten}/${view.byteLength} bytes)`);
                    }
                    bytesWritten += written;
                }

                pendingAckBytes += (bytesWritten || size || view.byteLength);

                // 達到批次閾值即回報，否則啟動 20ms 保底定時器，避免零散封包未被確認
                if (pendingAckBytes >= ACK_BATCH_THRESHOLD) {
                    flushAck();
                } else if (!ackDebounceTimer) {
                    ackDebounceTimer = setTimeout(flushAck, 20);
                }
            } catch (err) {
                console.error(`[OPFS Worker] 寫入第 ${workerWriteCount} 塊時失敗 (offset: ${offset}):`, err);
                await cleanup();
                self.postMessage({ type: 'error', message: err.message || 'OPFS 寫入失敗' });
            }
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