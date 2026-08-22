/**
 * sw.js - Service Worker 模擬串流下載器
 * 攔截 /sw-download 請求，從 IndexedDB 循序讀取 chunks 並透過 ReadableStream 串流輸出給瀏覽器下載，避免記憶體爆破 (OOM)。
 */

const DB_NAME = 'BobConnect_Storage';
const DB_VERSION = 1;
const CHUNK_STORE = 'transfer_chunks';

self.addEventListener('install', (event) => {
    self.skipWaiting();
});

self.addEventListener('activate', (event) => {
    event.waitUntil(self.clients.claim());
});

// 開啟 IndexedDB 連線
function openDB() {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(DB_NAME, DB_VERSION);
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}

// 根據 [transferId, index] 複合主鍵讀取單一 Chunk
function getChunk(db, transferId, chunkIndex) {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(CHUNK_STORE, 'readonly');
        const store = tx.objectStore(CHUNK_STORE);
        const req = store.get([transferId, chunkIndex]);
        req.onsuccess = () => resolve(req.result ? req.result.data : null);
        req.onerror = () => reject(req.error);
    });
}

self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);

    // 攔截模擬串流下載請求
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
        let chunkIndex = 0;

        // 建立 ReadableStream 實現 Zero-Copy 低記憶體串流
        const stream = new ReadableStream({
            async pull(controller) {
                try {
                    const chunkData = await getChunk(db, transferId, chunkIndex);
                    if (chunkData) {
                        controller.enqueue(new Uint8Array(chunkData));
                        chunkIndex++;
                    } else {
                        controller.close();
                    }
                } catch (err) {
                    console.error('[SW Stream] 讀取 Chunk 失敗:', err);
                    controller.error(err);
                }
            }
        });

        const encodedFilename = encodeURIComponent(fileName);
        const headers = new Headers({
            'Content-Type': 'application/octet-stream',
            'Content-Disposition': `attachment; filename="${encodedFilename}"; filename*=UTF-8''${encodedFilename}`
        });

        if (fileSize) {
            headers.set('Content-Length', fileSize.toString());
        }

        return new Response(stream, { headers });
    } catch (err) {
        console.error('[SW Stream] 初始化串流失敗:', err);
        return new Response('Stream initialization error', { status: 500 });
    }
}