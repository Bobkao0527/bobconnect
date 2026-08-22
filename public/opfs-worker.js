/**
 * opfs-worker.js - OPFS 高效零記憶體檔案寫入 Worker
 */

let accessHandle = null;
let fileHandle = null;
let rootDir = null;
let tempFilename = null;
let targetFilename = '';
let bytesSinceLastFlush = 0;
const FLUSH_THRESHOLD = 4 * 1024 * 1024;

self.onmessage = async (event) => {
    const data = event.data;
    if (!data || !data.type) return;

    switch (data.type) {
        case 'init': {
            const { downloadId, size, filename, tempFilename: customTempName } = data;
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
                        throw new Error(`儲存空間不足 (需要 ${(size / (1024 * 1024)).toFixed(1)} MB，剩餘可用 ${(available / (1024 * 1024)).toFixed(1)} MB)`);
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
                await cleanup();
                self.postMessage({ type: 'error', message: err.message || 'OPFS 寫入失敗' });
            }
            break;
        }

        case 'finish': {
            try {
                if (accessHandle) {
                    accessHandle.flush();
                    accessHandle.close(); // 🚀 關鍵：寫入完成後立刻關閉 handle 解鎖
                    accessHandle = null;
                }

                if (!fileHandle) throw new Error('OPFS 檔案控制桿不存在');

                const file = await fileHandle.getFile();

                self.postMessage({
                    type: 'finish-success',
                    file: file,
                    filename: targetFilename,
                    tempFilename: tempFilename // 回傳檔名給主線程刪除
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