/**
 * storage.js - 模組化本地儲存與垃圾回收 (IndexedDB 複合主鍵 + Service Worker 串流架構)
 * 100% 循序保證、零記憶體堆疊、極速磁碟暫存與全自動垃圾回收 (GC)。
 */

const DB_NAME = 'BobConnect_Storage';
const DB_VERSION = 3;
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
            // 升級或建立 Chunk 儲存庫：以 [transferId, index] 作為複合主鍵，天生具備 index 遞增順序
            if (db.objectStoreNames.contains(CHUNK_STORE)) {
                db.deleteObjectStore(CHUNK_STORE);
            }
            db.createObjectStore(CHUNK_STORE, { keyPath: ['transferId', 'index'] });

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
 * 舊版相容介面：改為直接觸發 SW 串流下載
 * @param {string} transferId 
 * @param {string} fileName 
 * @param {number} fileSize 
 */
export async function assembleFile(transferId, fileName = 'downloaded_file', fileSize = 0) {
    await triggerStreamDownload(transferId, fileName, fileSize);
}

/**
 * 🧹 垃圾回收 (GC)：刪除特定傳輸的所有暫存 Chunks 與中繼資料
 * 使用主鍵範圍快速清理，無需遍歷次級索引
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

            // 透過複合主鍵範圍刪除所有關聯 Chunks
            const range = IDBKeyRange.bound([transferId, 0], [transferId, Infinity]);
            const request = chunkStore.openKeyCursor(range);

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

/**
 * 註冊 Service Worker 以支援大檔案串流下載
 */
export async function initServiceWorker() {
    if ('serviceWorker' in navigator) {
        try {
            const reg = await navigator.serviceWorker.register('./sw.js', { scope: './' });
            await navigator.serviceWorker.ready;
            console.log('[Service Worker] 串流下載服務已就緒 (Active)');
            return reg;
        } catch (err) {
            console.error('[Service Worker 註冊失敗]:', err);
        }
    }
}

/**
 * 透過 Service Worker 觸發模擬串流下載，避免組裝 Blob 導致記憶體溢出 (OOM)
 * @param {string} transferId 
 * @param {string} fileName 
 * @param {number} fileSize 
 */
export async function triggerStreamDownload(transferId, fileName = 'downloaded_file', fileSize = 0) {
    // 確保 Service Worker 就緒
    if ('serviceWorker' in navigator) {
        try {
            await navigator.serviceWorker.ready;
        } catch (e) {}
    }

    const downloadUrl = `./sw-download?transferId=${encodeURIComponent(transferId)}&name=${encodeURIComponent(fileName)}&size=${fileSize}`;
    
    const a = document.createElement('a');
    a.href = downloadUrl;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    setTimeout(() => {
        if (a.parentNode) {
            document.body.removeChild(a);
        }
    }, 1000);
}