/**
 * storage.js - 模組化本地儲存與垃圾回收 (IndexedDB + OPFS 雙軌清理架構)
 * 解決 OPFS 在 Safari/行動端的同步寫入鎖定與訊息過載問題，提供全平台 100% 相容的檔案暫存。
 */

const DB_NAME = 'BobConnect_Storage';
const DB_VERSION = 1;
const CHUNK_STORE = 'transfer_chunks';
const META_STORE = 'transfer_meta';

let dbInstance = null;

/**
 * 取得或建立單例 IndexedDB 連線
 */
export async function getDB() {
    if (dbInstance) return dbInstance;

    return new Promise((resolve, reject) => {
        const request = indexedDB.open(DB_NAME, DB_VERSION);

        request.onupgradeneeded = (event) => {
            const db = event.target.result;
            if (!db.objectStoreNames.contains(CHUNK_STORE)) {
                const chunkStore = db.createObjectStore(CHUNK_STORE, { keyPath: ['transferId', 'index'] });
                chunkStore.createIndex('transferId', 'transferId', { unique: false });
            }
            if (!db.objectStoreNames.contains(META_STORE)) {
                db.createObjectStore(META_STORE, { keyPath: 'transferId' });
            }
        };

        request.onsuccess = (event) => {
            dbInstance = event.target.result;
            dbInstance.onclose = () => {
                dbInstance = null;
            };
            resolve(dbInstance);
        };

        request.onerror = (event) => {
            console.error('[IndexedDB 開啟失敗]:', event.target.error);
            reject(event.target.error);
        };
    });
}

/**
 * 儲存單一或批次 Chunks
 * @param {string} transferId 傳輸識別碼
 * @param {Array<{ index: number, data: ArrayBuffer, offset: number, size: number }>} chunks 
 */
export async function saveChunks(transferId, chunks) {
    if (!chunks || chunks.length === 0) return;
    const db = await getDB();

    return new Promise((resolve, reject) => {
        const tx = db.transaction([CHUNK_STORE], 'readwrite');
        const store = tx.objectStore(CHUNK_STORE);

        for (const item of chunks) {
            store.put({
                transferId: transferId,
                index: item.index,
                offset: item.offset,
                size: item.size,
                data: item.data
            });
        }

        tx.oncomplete = () => resolve();
        tx.onerror = (e) => reject(e.target.error);
        tx.onabort = (e) => reject(e.target.error);
    });
}

/**
 * 儲存傳輸中繼資料 (Metadata)
 */
export async function saveMeta(transferId, meta) {
    const db = await getDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction([META_STORE], 'readwrite');
        const store = tx.objectStore(META_STORE);
        store.put({
            transferId: transferId,
            ...meta,
            timestamp: Date.now()
        });
        tx.oncomplete = () => resolve();
        tx.onerror = (e) => reject(e.target.error);
    });
}

/**
 * 將指定傳輸的所有 Chunks 循序組裝為單一 Blob
 * @param {string} transferId 
 * @param {string} mimeType 
 * @returns {Promise<Blob>}
 */
export async function assembleFile(transferId, mimeType = 'application/octet-stream') {
    const db = await getDB();

    return new Promise((resolve, reject) => {
        const tx = db.transaction([CHUNK_STORE], 'readonly');
        const store = tx.objectStore(CHUNK_STORE);
        const index = store.index('transferId');
        const range = IDBKeyRange.only(transferId);

        const chunks = [];
        const request = index.openCursor(range);

        request.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
                chunks.push(cursor.value);
                cursor.continue();
            } else {
                // 確保按照 index 順序排序
                chunks.sort((a, b) => a.index - b.index);
                const dataBuffers = chunks.map(c => c.data);
                const blob = new Blob(dataBuffers, { type: mimeType });
                resolve(blob);
            }
        };

        request.onerror = (e) => reject(e.target.error);
    });
}

/**
 * 🧹 垃圾回收 (GC)：刪除特定傳輸的所有暫存 Chunks 與中繼資料
 * @param {string} transferId 
 */
export async function deleteTransfer(transferId) {
    if (!transferId) return;
    try {
        const db = await getDB();
        return new Promise((resolve, reject) => {
            const tx = db.transaction([CHUNK_STORE, META_STORE], 'readwrite');
            const chunkStore = tx.objectStore(CHUNK_STORE);
            const metaStore = tx.objectStore(META_STORE);

            // 刪除 Meta
            metaStore.delete(transferId);

            // 透過 Index 遍歷刪除所有 Chunks
            const index = chunkStore.index('transferId');
            const range = IDBKeyRange.only(transferId);
            const request = index.openKeyCursor(range);

            request.onsuccess = (event) => {
                const cursor = event.target.result;
                if (cursor) {
                    chunkStore.delete(cursor.primaryKey);
                    cursor.continue();
                }
            };

            tx.oncomplete = () => {
                console.log(`[Storage GC] 已清理傳輸暫存: ${transferId}`);
                resolve();
            };
            tx.onerror = (e) => reject(e.target.error);
        });
    } catch (err) {
        console.warn(`[Storage GC] 清理傳輸 ${transferId} 失敗:`, err);
    }
}

/**
 * 🧹 垃圾回收 (GC)：全域清空所有 IndexedDB 與 OPFS 暫存
 * 用於頁面載入初始化或手動重置，防止累積未完成傳輸塞爆儲存空間
 */
export async function clearEntireStorage() {
    console.log('[Storage 清理] 正在執行 IndexedDB 與 OPFS 雙軌儲存空間全域清理...');

    // 1. 清理 IndexedDB
    try {
        if (dbInstance) {
            try { dbInstance.close(); } catch (e) {}
            dbInstance = null;
        }
        await new Promise((resolve) => {
            const req = indexedDB.deleteDatabase(DB_NAME);
            req.onsuccess = () => {
                console.log('[Storage GC] IndexedDB 資料庫已成功重置與清空');
                resolve();
            };
            req.onerror = () => {
                console.warn('[Storage GC] 刪除 IndexedDB 失敗');
                resolve();
            };
            req.onblocked = () => {
                console.warn('[Storage GC] 刪除 IndexedDB 受到阻塞');
                resolve();
            };
        });
    } catch (e) {
        console.warn('[Storage GC] IndexedDB 清理例外:', e);
    }

    // 2. 清理 OPFS (若瀏覽器支援)
    if (navigator.storage && navigator.storage.getDirectory) {
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
            console.log('[OPFS 清理] OPFS 空間已全數清空完成');
        } catch (err) {
            console.warn('[OPFS 清理] 執行失敗:', err);
        }
    }
}
