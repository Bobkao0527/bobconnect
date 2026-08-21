/**
 * opfs-worker.js - OPFS 高效零記憶體檔案寫入 Worker (Safari 穩定優化版)
 */

let accessHandle = null;
let fileHandle = null;
let rootDir = null;
let tempFilename = null;
let targetFilename = '';
let bytesSinceLastFlush = 0;
const FLUSH_THRESHOLD = 32 * 1024 * 1024; // 調高至 32MB，降低 fsync 頻率

self.onmessage = async (event) => {
    const data = event.data;
    if (!data || !data.type) return;

    switch (data.type) {
        case 'init': {
            const { downloadId, size, filename } = data;
            targetFilename = filename || 'downloaded_file';
            bytesSinceLastFlush = 0;

            try {
                if (navigator.storage && navigator.storage.persist) {
                    await navigator.storage.persist();
                }

                if (navigator.storage && navigator.storage.estimate) {
                    const { quota, usage } = await navigator.storage.estimate();
                    const available = quota - usage;
                    if (size && available < size) {
                        throw new Error(`Safari 儲存空間配額不足 (需要 ${(size / (1024 * 1024)).toFixed(1)} MB，可用 ${(available / (1024 * 1024)).toFixed(1)} MB)`);
                    }
                }

                rootDir = await navigator.storage.getDirectory();
                tempFilename = `temp_${downloadId || Date.now()}_${Date.now()}.tmp`;
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
                accessHandle.write(view, { at: offset });

                bytesSinceLastFlush += view.byteLength;
                if (bytesSinceLastFlush >= FLUSH_THRESHOLD) {
                    accessHandle.flush();
                    bytesSinceLastFlush = 0;
                }

                self.postMessage({
                    type: 'write-ack',
                    offset: offset + view.byteLength,
                    size: size || view.byteLength
                });
            } catch (err) {
                console.error('[OPFS Worker] 寫入失敗:', err);
                const isQuota = err.name === 'QuotaExceededError' || (err.message && err.message.includes('quota'));
                const errMsg = isQuota 
                    ? '寫入中斷：已達到 Safari 瀏覽器單一站點儲存上限 (通常為 1GB)'
                    : (err.message || 'OPFS 寫入失敗');
                await cleanup();
                self.postMessage({ type: 'error', message: errMsg });
            }
            break;
        }

        case 'finish': {
            try {
                if (accessHandle) {
                    accessHandle.flush();
                    accessHandle.close();
                    accessHandle = null;
                    await new Promise((resolve) => setTimeout(resolve, 100));
                }

                if (!fileHandle) {
                    throw new Error('OPFS 檔案控制桿不存在');
                }

                const file = await fileHandle.getFile();
                const blobUrl = URL.createObjectURL(file);

                self.postMessage({
                    type: 'finish-success',
                    blobUrl: blobUrl,
                    filename: targetFilename
                });
            } catch (err) {
                console.error('[OPFS Worker] 結束寫入失敗:', err);
                await cleanup();
                self.postMessage({ type: 'error', message: err.message || 'OPFS 結束寫入處理解析失敗' });
            }
            break;
        }

        case 'abort': {
            await cleanup();
            break;
        }
    }
};

async function cleanup() {
    if (accessHandle) {
        try {
            accessHandle.close();
        } catch (e) {}
        accessHandle = null;
    }
    if (rootDir && tempFilename) {
        try {
            await rootDir.removeEntry(tempFilename);
        } catch (e) {}
        tempFilename = null;
        fileHandle = null;
    }
}