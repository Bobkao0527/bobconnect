/**
 * sw.js - Service Worker 串流下載服務
 * 攔截特定下載請求，透過 ReadableStream 實現免記憶體暫存的檔案串流下載
 */
const downloadStreams = new Map();

self.addEventListener('install', () => {
    self.skipWaiting();
});

self.addEventListener('activate', (event) => {
    event.waitUntil(self.clients.claim());
});

self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);

    // 攔截 Service Worker 串流下載路徑
    if (url.pathname.endsWith('/sw-download')) {
        const downloadId = url.searchParams.get('id');
        const streamData = downloadStreams.get(downloadId);

        if (streamData) {
            const { stream, filename, size } = streamData;
            const headers = new Headers({
                'Content-Type': 'application/octet-stream',
                'Content-Disposition': `attachment; filename="${encodeURIComponent(filename)}"; filename*=UTF-8''${encodeURIComponent(filename)}`,
            });

            if (size) {
                headers.set('Content-Length', size.toString());
            }

            event.respondWith(new Response(stream, { headers }));
            downloadStreams.delete(downloadId);
        }
    }
});

self.addEventListener('message', (event) => {
    const { type, downloadId, filename, size, chunk } = event.data;

    if (type === 'init') {
        let controller;
        const stream = new ReadableStream({
            start(c) {
                controller = c;
            }
        });
        
        downloadStreams.set(downloadId, { stream, controller, filename, size });

        if (event.ports && event.ports[0]) {
            event.ports[0].postMessage({ type: 'init-success' });
        }
    } 
    else if (type === 'write') {
        const streamData = downloadStreams.get(downloadId);
        if (streamData && streamData.controller) {
            try {
                streamData.controller.enqueue(new Uint8Array(chunk));
            } catch (err) {
                console.error('[SW] 串流寫入失敗:', err);
            }
        }
    } 
    else if (type === 'close') {
        const streamData = downloadStreams.get(downloadId);
        if (streamData && streamData.controller) {
            try {
                streamData.controller.close();
            } catch (err) {
                console.error('[SW] 串流關閉失敗:', err);
            }
        }
    }
});