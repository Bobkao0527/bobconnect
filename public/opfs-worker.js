/**
 * opfs-worker.js - 處理 Safari/WebKit 專用之 OPFS 背景同步寫入
 */
let accessHandle = null;
let fileHandle = null;

self.onmessage = async (e) => {
    const { type, filename, chunk } = e.data;

    if (type === 'init') {
        try {
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
            accessHandle.write(new Uint8Array(chunk));
        }
    } 
    else if (type === 'close') {
        if (accessHandle) {
            accessHandle.flush();
            accessHandle.close();
            accessHandle = null;

            const file = await fileHandle.getFile();
            const url = URL.createObjectURL(file);
            self.postMessage({ type: 'complete', url });
        }
    }
};