/**
 * sw.js - Service Worker 模擬串流下載器 (複合主鍵嚴格循序版)
 */

const DB_NAME = 'BobConnect_Storage';
const DB_VERSION = 3; // 升級版號刷新 Schema
const CHUNK_STORE = 'transfer_chunks';

self.addEventListener('install', (event) => {
    self.skipWaiting();
});

self.addEventListener('activate', (event) => {
    event.waitUntil(self.clients.claim());
});

function openDB() {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(DB_NAME, DB_VERSION);
        req.onupgradeneeded = (event) => {
            const db = event.target.result;
            if (!db.objectStoreNames.contains(CHUNK_STORE)) {
                db.createObjectStore(CHUNK_STORE, { keyPath: ['transferId', 'index'] });
            }
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}

self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);

    if (url.pathname.endsWith('/sw-download')) {
        const transferId = url.searchParams.get('transferId');
        const fileName = url.searchParams.get('name') || 'downloaded_file';
        const fileSize = url.searchParams.get('size');

        if (!transferId) {
            event.respondWith(new Response('Missing transferId', { status: 400 }));
            return;
        }

        event.respondWith(handleStreamDownload(transferId, fileName, fileSize));
    }
});

async function handleStreamDownload(transferId, fileName, fileSize) {
    try {
        const db = await openDB();

        const stream = new ReadableStream({
            async start(controller) {
                try {
                    const tx = db.transaction(CHUNK_STORE, 'readonly');
                    const store = tx.objectStore(CHUNK_STORE);
                    
                    // 🚀 核心優化：直接使用複合主鍵範圍 [transferId, 0] ~ [transferId, Infinity]
                    // 保證游標 100% 依 index 嚴格遞增 (0, 1, 2...) 循序讀取，無需次級索引
                    const range = IDBKeyRange.bound([transferId, 0], [transferId, Infinity]);
                    const request = store.openCursor(range);

                    await new Promise((resolve, reject) => {
                        request.onsuccess = (event) => {
                            const cursor = event.target.result;
                            if (cursor) {
                                if (cursor.value && cursor.value.data) {
                                    controller.enqueue(new Uint8Array(cursor.value.data));
                                }
                                cursor.continue(); // 移動至下一個 chunk
                            } else {
                                resolve(); // 所有 chunks 讀取完畢
                            }
                        };
                        request.onerror = (e) => reject(e.target.error);
                        tx.onerror = (e) => reject(e.target.error);
                    });

                    controller.close();
                } catch (err) {
                    console.error('[SW Stream] 串流讀取失敗:', err);
                    controller.error(err);
                }
            }
        });

        const encodedFilename = encodeURIComponent(fileName);
        const headers = new Headers({
            'Content-Type': 'application/octet-stream',
            'Content-Disposition': `attachment; filename="${encodedFilename}"; filename*=UTF-8''${encodedFilename}`
        });

        if (fileSize && parseInt(fileSize) > 0) {
            headers.set('Content-Length', fileSize.toString());
        }

        return new Response(stream, { headers });
    } catch (err) {
        console.error('[SW Stream] 初始化串流失敗:', err);
        return new Response('Stream initialization error', { status: 500 });
    }
}