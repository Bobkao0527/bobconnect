/**
 * opfs-worker.js - 處理 Safari/WebKit 專用之 OPFS 背景同步寫入
 */
let accessHandle = null;
let fileHandle = null;
let writeOffset = 0; // 🚀 紀錄硬碟寫入偏移量

self.onmessage = async (e) => {
    const { type, filename, chunk } = e.data;

    if (type === 'init') {
        try {
            writeOffset = 0; // 重置寫入偏移量
            const root = await navigator.storage.getDirectory();
            fileHandle = await root.getFileHandle(`temp_${Date.now()}_${filename}`, { create: true });
            // 🚀 Safari/WebKit 官方標準： Worker 同步 AccessHandle
            accessHandle = await fileHandle.createSyncAccessHandle();
            self.postMessage({ type: 'init-success' });
        } catch (err) {
            self.postMessage({ type: 'init-error', error: err.message });
        }
    } 
    else if (type === 'write') {
        if (accessHandle && chunk) {
            try {
                const buffer = new Uint8Array(chunk);
                // 🚀 必須指定 at 參數，否則預設會從 0 開始覆寫導致檔案損壞
                accessHandle.write(buffer, { at: writeOffset });
                writeOffset += buffer.byteLength;
            } catch (err) {
                self.postMessage({ type: 'error', error: 'OPFS 寫入失敗: ' + err.message });
            }
        }
    } 
    else if (type === 'close') {
        try {
            if (accessHandle) {
                accessHandle.flush();
                accessHandle.close();
                accessHandle = null;

                const file = await fileHandle.getFile();
                const url = URL.createObjectURL(file);
                self.postMessage({ type: 'complete', url });
            } else {
                throw new Error("AccessHandle 未初始化或已關閉");
            }
        } catch (err) {
            self.postMessage({ type: 'error', error: 'OPFS 封裝失敗: ' + err.message });
        }
    }
};