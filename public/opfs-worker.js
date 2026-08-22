/**
 * opfs-worker.js - OPFS 高效零記憶體檔案寫入 Worker
 */

let accessHandle = null;
let fileHandle = null;
let rootDir = null;
let tempFilename = null;
let targetFilename = '';

self.addEventListener('error', (err) => {
    console.error('[OPFS Worker 全域錯誤]:', err);
    self.postMessage({ type: 'error', message: err.message || 'Worker 內部例外錯誤' });
});

self.addEventListener('unhandledrejection', (event) => {
    console.error('[OPFS Worker 未捕捉 Promise 例外]:', event.reason);
    self.postMessage({ type: 'error', message: event.reason ? (event.reason.message || String(event.reason)) : '非預期異步錯誤' });
});

self.onmessage = async (event) => {
    const data = event.data;
    if (!data || !data.type) return;

    switch (data.type) {
        case 'init': {
            const { downloadId, size, filename, tempFilename: customTempName } = data;
            targetFilename = filename || 'downloaded_file';

            try {
                if (navigator.storage && navigator.storage.persist) {
                    await navigator.storage.persist().catch(() => { });
                }

                if (navigator.storage && navigator.storage.estimate) {
                    try {
                        const { quota, usage } = await navigator.storage.estimate();
                        const available = quota - usage;
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

                fileHandle = await rootDir.getFileHandle(tempFilename, { create: true });
                accessHandle = await fileHandle.createSyncAccessHandle();

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

                const view = new Uint8Array(chunk);
                const written = accessHandle.write(view, { at: offset });

                const bytesWritten = (typeof written === 'number' && written > 0) ? written : (size || view.byteLength);
                self.postMessage({
                    type: 'write-ack',
                    size: bytesWritten
                });
            } catch (err) {
                console.error('[OPFS Worker] 寫入失敗:', err);
                await cleanup();
                self.postMessage({ type: 'error', message: err.message || 'OPFS 寫入失敗' });
            }
            break;
        }

        case 'finish': {
            try {
                if (accessHandle) {
                    accessHandle.flush(); // 僅在檔案傳輸結束時執行一次完整落盤
                    accessHandle.close(); // 關閉句柄解鎖
                    accessHandle = null;
                }

                if (!fileHandle) throw new Error('OPFS 檔案控制桿不存在');

                const file = await fileHandle.getFile();

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