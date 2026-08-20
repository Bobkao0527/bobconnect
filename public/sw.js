/**
 * sw.js - Service Worker 串流下載服務 (修正版)
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

    if (url.pathname.endsWith('/sw-download') && event.request.method === 'GET') {
        const downloadId = url.searchParams.get('id');
        const streamData = downloadStreams.get(downloadId);

        if (streamData) {
            const { stream, filename, size } = streamData;

            const safeFilename = filename.replace(/"/g, '\\"');
            const encodedFilename = encodeURIComponent(filename);

            const headers = new Headers({
                'Content-Type': 'application/octet-stream',
                'Content-Disposition': `attachment; filename="${safeFilename}"; filename*=UTF-8''${encodedFilename}`,
            });

            if (size) {
                headers.set('Content-Length', size.toString());
            }

            event.respondWith(new Response(stream, { headers }));
        }
    }
});

self.addEventListener('message', (event) => {
    const { type, downloadId, filename, size, chunk } = event.data || {};

    if (type === 'init') {
        let controller;
        const port = event.ports && event.ports[0];

        // highWaterMark 維持 2MB，避免 iPad / iOS Safari 記憶體壓力
        const stream = new ReadableStream({
            start(c) {
                controller = c;
            },
            pull(c) {
                if (port) {
                    port.postMessage({
                        type: 'drain',
                        desiredSize: c.desiredSize
                    });
                }
            },
            cancel(reason) {
                console.warn('[SW] 串流被取消:', reason);

                if (port) {
                    try {
                        port.postMessage({
                            type: 'stream-cancel',
                            reason: String(reason)
                        });
                    } catch (e) {
                        // port 可能已關閉，忽略即可
                    }
                }

                downloadStreams.delete(downloadId);
            }
        }, new ByteLengthQueuingStrategy({ highWaterMark: 2 * 1024 * 1024 }));

        downloadStreams.set(downloadId, {
            stream,
            controller,
            filename,
            size,
            port
        });

        if (port) {
            port.postMessage({ type: 'init-success' });
        }
    }

    else if (type === 'write') {
        const streamData = downloadStreams.get(downloadId);

        if (streamData && streamData.controller) {
            try {
                streamData.controller.enqueue(new Uint8Array(chunk));

                if (streamData.port) {
                    streamData.port.postMessage({
                        type: 'write-ack',
                        desiredSize: streamData.controller.desiredSize
                    });
                }
            } catch (err) {
                console.error('[SW] 串流寫入失敗:', err);

                if (streamData.port) {
                    try {
                        streamData.port.postMessage({
                            type: 'stream-cancel',
                            reason: String(err)
                        });
                    } catch (e) {
                        // port 可能已關閉，忽略即可
                    }
                }
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
            } finally {
                downloadStreams.delete(downloadId);
            }
        }
    }
});