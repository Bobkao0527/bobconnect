/**
 * opfs-worker.js - OPFS 高效零記憶體檔案寫入 Worker
 * 採用 Origin Private File System (OPFS) 搭配 createSyncAccessHandle 實作硬碟直寫
 */

let accessHandle = null;
let fileHandle = null;
let rootDir = null;
let tempFilename = null;
let targetFilename = '';
let bytesSinceLastFlush = 0;
const FLUSH_THRESHOLD = 32 * 1024 * 1024; // 每 32MB 強制 flush 一次釋放 Safari 快取

self.onmessage = async (event) => {
    const data = event.data;
    if (!data || !data.type) return;

    switch (data.type) {
        case 'init': {
            const { downloadId, size, filename } = data;
            targetFilename = filename || 'downloaded_file';
            bytesSinceLastFlush = 0;

            try {
                // 嘗試請求持久化儲存 (防止 Safari 50MB 配額限制)
                if (navigator.storage && navigator.storage.persist) {
                    await navigator.storage.persist();
                }

                if (navigator.storage && navigator.storage.estimate) {
                    const { quota, usage } = await navigator.storage.estimate();
                    const available = quota - usage;
                    if (size && available < size) {
                        throw new Error(`儲存空間不足 (需要 ${(size / (1024 * 1024)).toFixed(1)} MB，剩餘可用 ${(available / (1024 * 1024)).toFixed(1)} MB)`);
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

                // 🚀【回傳 size 給主線程做 Backpressure 計算】
                self.postMessage({
                    type: 'write-ack',
                    offset: offset + view.byteLength,
                    size: size || view.byteLength
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
                    accessHandle.flush();
                    accessHandle.close();
                    accessHandle = null;
                    await new Promise((resolve) => setTimeout(resolve, 50));
                }

                if (!fileHandle) throw new Error('OPFS 檔案控制桿不存在');

                const file = await fileHandle.getFile();

                self.postMessage({
                    type: 'finish-success',
                    file: file,
                    filename: targetFilename,
                    tempFilename: tempFilename // 傳回檔名以便後續清理
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

        case 'delete-temp': {
            await cleanup();
            break;
        }

        default:
            console.warn('[OPFS Worker] 接收到未知的訊息類型:', data.type);
            break;
    }
};

async function cleanup() {
    if (accessHandle) {
        try {
            accessHandle.close();
        } catch (e) {
            console.warn('[OPFS Worker] 關閉 accessHandle 失敗:', e);
        }
        accessHandle = null;
    }
    if (rootDir && tempFilename) {
        try {
            await rootDir.removeEntry(tempFilename);
        } catch (e) {
            // 忽略刪除失敗 (例如檔案已被刪除)
        }
        tempFilename = null;
        fileHandle = null;
    }
}