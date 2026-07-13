/**
 * r2.js - Cloudflare R2 傳輸模組
 * 處理高併發分塊上傳、自動降級、防呆重試及串流大檔案下載銷毀邏輯。
 * 🚀【進度同步優化】：在上傳過程中，透過 WebRTC dataChannel 即時同步進度與取消事件。
 * 🚀【按鈕優化】：行動端手動下載按鈕自動轉換為「儲存 / 分享檔案」文字。
 */
import { state } from './state.js';
import { showToast, triggerAutoDownload } from './ui.js';
import { getWorkerUrl } from './webrtc.js';

// 下載/上傳完成後銷毀 R2 的託管檔案
export async function destroyR2File(url) {
    try {
        await fetch(url, { method: 'DELETE', keepalive: true });
        console.log("[安全機制] R2 暫存大檔案已成功手動清理銷毀。");
    } catch (e) {
        console.error("手動銷毀 R2 檔案失敗:", e);
    }
}

// 串流下載 R2 大檔案並進行秒級銷毀
export async function downloadAndCleanR2(downloadUrl, filename, totalSize) {
    try {
        const response = await fetch(downloadUrl);
        if (!response.ok) throw new Error("讀取雲端大檔案出錯");

        const reader = response.body.getReader();
        const chunks = [];
        let loaded = 0;
        let lastUiTime = 0;

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            chunks.push(value);
            loaded += value.length;

            const now = Date.now();
            if (now - lastUiTime > 100 || loaded === totalSize) {
                lastUiTime = now;
                const percent = Math.min(100, Math.round((loaded / totalSize) * 100));
                document.getElementById('recv-percent').innerText = `${percent}%`;
                document.getElementById('recv-progress-bar').style.width = `${percent}%`;
            }
        }

        const blob = new Blob(chunks, { type: "application/octet-stream" });
        const localUrl = URL.createObjectURL(blob);
        triggerAutoDownload(localUrl, filename);

        chunks.length = 0; // 釋放記憶體

        document.getElementById('recv-percent').innerText = '100% (下載完成，雲端檔案已同步銷毀)';
        document.getElementById('recv-progress-bar').className = "bg-emerald-500 h-1 w-full";

        const btnManual = document.getElementById('btn-manual-download');
        if (btnManual) {
            // 🚀【體驗優化】：行動端按鈕顯示為「儲存 / 分享檔案」
            btnManual.innerText = state.localIsMobile ? '📤 儲存 / 分享檔案' : '📥 手動下載檔案';
            btnManual.onclick = () => triggerAutoDownload(localUrl, filename);
        }
        document.getElementById('manual-download-container').classList.remove('hidden');

        showToast(`「${filename}」下載成功！`, 'success');

        await destroyR2File(downloadUrl);

        setTimeout(() => {
            URL.revokeObjectURL(localUrl);
        }, 30000);

    } catch (err) {
        console.error("R2 下載或銷毀大檔案時發生錯誤:", err);
        showToast("下載失敗或該大檔案已被銷毀", "error");
    }
}

// R2 多線程分塊上傳
export async function sendViaR2Multipart() {
    const btnSend = document.getElementById('btn-send');
    if (btnSend) {
        btnSend.disabled = true;
        btnSend.classList.add('opacity-40', 'cursor-not-allowed');
    }

    document.getElementById('progress-status').innerText = '正在向雲端要求建立 R2 通道...';
    
    // 🚀【同步上傳進度】：通知接收端，大檔案準備上傳至 R2
    if (state.dataChannel && state.dataChannel.readyState === 'open') {
        state.dataChannel.send(JSON.stringify({
            type: 'file-r2-upload-start',
            name: state.selectedFile.name,
            size: state.selectedFile.size
        }));
    }

    const workerUrl = getWorkerUrl();
    const filenameEncoded = encodeURIComponent(state.selectedFile.name);

    try {
        const initRes = await fetch(`${workerUrl}/r2-multipart/init/${state.roomId}/${filenameEncoded}`, { method: 'POST' });
        if (!initRes.ok) {
            const errData = await initRes.json().catch(() => ({}));
            if (errData.error === "CONCURRENCY_LOCKED" || errData.error === "R2_LIMIT_EXCEEDED" || errData.message) {
                throw new Error(errData.message);
            }
            throw new Error("無法初始化分塊上傳");
        }
        const { uploadId, directUpload } = await initRes.json();

        const totalSize = state.selectedFile.size;
        const partSize = 10 * 1024 * 1024; // 10MB 分塊
        const totalParts = Math.ceil(totalSize / partSize);
        const uploadedParts = [];
        const startTime = Date.now();
        let lastUiUpdateTime = 0;

        let currentConcurrencyLimit = directUpload ? 5 : 3;
        let activeDirectUpload = directUpload;
        
        document.getElementById('progress-status').innerText = activeDirectUpload 
            ? `正在與 R2 建立極速直連通道 (${currentConcurrencyLimit} 執行緒並行)...` 
            : `正在進行極速 R2 並行傳輸 (代理優化模式)...`;

        const partLoadedBytes = {};
        
        const updateOverallProgress = () => {
            const currentLoaded = Object.values(partLoadedBytes).reduce((a, b) => a + b, 0);
            const now = Date.now();
            if (now - lastUiUpdateTime > 100 || currentLoaded === totalSize) {
                lastUiUpdateTime = now;

                const percent = Math.min(100, Math.round((currentLoaded / totalSize) * 100));
                document.getElementById('progress-percent').innerText = `${percent}%`;
                document.getElementById('progress-bar').style.width = `${percent}%`;

                const elapsedTime = (now - startTime) / 1000;
                const speedBytes = currentLoaded / elapsedTime;
                const speedMB = (speedBytes / (1024 * 1024)).toFixed(2);
                document.getElementById('transfer-speed').innerText = `${speedMB} MB/s`;

                if (speedBytes > 0) {
                    const remainingBytes = totalSize - currentLoaded;
                    const remainingSeconds = Math.max(0, Math.round(remainingBytes / speedBytes));
                    const mins = Math.floor(remainingSeconds / 60);
                    const secs = remainingSeconds % 60;
                    document.getElementById('transfer-time').innerText = `${mins}:${secs < 10 ? '0' : ''}${secs}`;
                }

                // 🚀【同步上傳進度】：即時發送最新進度給接收端
                if (state.dataChannel && state.dataChannel.readyState === 'open') {
                    state.dataChannel.send(JSON.stringify({
                        type: 'file-r2-upload-progress',
                        percent: percent
                    }));
                }
            }
        };

        let nextPartToUpload = 1;
        const uploadErrors = [];

        const uploadWorker = async () => {
            while (nextPartToUpload <= totalParts && uploadErrors.length === 0) {
                const currentPart = nextPartToUpload++;
                const offset = (currentPart - 1) * partSize;
                const chunk = state.selectedFile.slice(offset, Math.min(offset + partSize, totalSize));
                const chunkData = await chunk.arrayBuffer();

                let attempts = 0;
                let success = false;
                let uploadedPart = null;

                while (attempts < 2 && !success && uploadErrors.length === 0) {
                    attempts++;
                    const attemptDirect = (activeDirectUpload && attempts === 1);
                    let targetUrl = "";

                    try {
                        if (attemptDirect) {
                            const presignRes = await fetch(`${workerUrl}/r2-multipart/presign/${state.roomId}/${filenameEncoded}?uploadId=${uploadId}&partNumber=${currentPart}`);
                            if (!presignRes.ok) throw new Error("直連認證失效");
                            const presignData = await presignRes.json();
                            targetUrl = presignData.uploadUrl;
                        } else {
                            targetUrl = `${workerUrl}/r2-multipart/upload/${state.roomId}/${filenameEncoded}?uploadId=${uploadId}&partNumber=${currentPart}`;
                        }

                        uploadedPart = await new Promise((resolve, reject) => {
                            const xhr = new XMLHttpRequest();
                            xhr.open('PUT', targetUrl, true);

                            if (!attemptDirect) {
                                xhr.setRequestHeader('Content-Type', 'application/octet-stream');
                            }

                            xhr.upload.addEventListener('progress', (e) => {
                                if (e.lengthComputable) {
                                    partLoadedBytes[currentPart] = e.loaded;
                                    updateOverallProgress();
                                }
                            });

                            xhr.onload = () => {
                                if (xhr.status === 200) {
                                    const etag = xhr.getResponseHeader('ETag') || "";
                                    resolve({
                                        partNumber: currentPart,
                                        etag: etag.replace(/"/g, "")
                                    });
                                } else {
                                    reject(new Error(`HTTP ${xhr.status}`));
                                }
                            };
                            xhr.onerror = () => reject(new Error("網路連線或 CORS 阻擋錯誤"));
                            xhr.send(chunkData);
                        });

                        success = true;
                    } catch (err) {
                        if (attemptDirect) {
                            console.warn(`[自動防呆] 分塊 ${currentPart} 直連上傳失敗。立即無感降級至代理模式重試...`);
                            activeDirectUpload = false;
                            const statusLabel = document.getElementById('progress-status');
                            if (statusLabel) statusLabel.innerText = `直連通道異常，已自動降級至代理通道傳輸中...`;
                            continue;
                        } else {
                            uploadErrors.push(err);
                            throw err;
                        }
                    }
                }

                if (success && uploadedPart) {
                    uploadedParts.push({
                        partNumber: uploadedPart.partNumber,
                        etag: uploadedPart.etag
                    });
                }
            }
        };

        const pool = [];
        const threads = Math.min(currentConcurrencyLimit, totalParts);
        for (let i = 0; i < threads; i++) {
            pool.push(uploadWorker());
        }

        await Promise.all(pool);

        if (uploadErrors.length > 0) throw uploadErrors[0];

        document.getElementById('progress-status').innerText = '正在拼裝合併大檔案中...';
        const sortedParts = uploadedParts.slice().sort((a, b) => a.partNumber - b.partNumber);
        const completeRes = await fetch(`${workerUrl}/r2-multipart/complete/${state.roomId}/${filenameEncoded}?uploadId=${uploadId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(sortedParts)
        });

        if (!completeRes.ok) {
            const errorText = await completeRes.text().catch(() => '無法讀取伺服器錯誤');
            throw new Error(`雲端檔案合併失敗: ${errorText}`);
        }

        document.getElementById('progress-status').innerText = '託管成功，同步信令中...';

        const downloadUrl = `${workerUrl}/r2/${state.roomId}/${filenameEncoded}`;
        const r2Message = {
            type: 'file-r2',
            name: state.selectedFile.name,
            size: state.selectedFile.size,
            downloadUrl: downloadUrl
        };
        state.dataChannel.send(JSON.stringify(r2Message));

        document.getElementById('progress-status').innerText = '傳送成功！對方已啟動高速 R2 串流下載';
        showToast('大檔案託管完成，已同步對方啟動 R2 安全下載路徑！', 'success');

    } catch (err) {
        console.error("R2 分塊上傳失敗:", err);
        document.getElementById('progress-status').innerText = err.message || '大檔案分塊上傳失敗';
        showToast(`傳送暫停: ${err.message}`, 'error');

        // 🚀【同步取消】：萬一上傳中斷或報錯，通知接收端收回等待狀態
        if (state.dataChannel && state.dataChannel.readyState === 'open') {
            state.dataChannel.send(JSON.stringify({
                type: 'file-r2-upload-cancel'
            }));
        }
    } finally {
        if (btnSend) {
            btnSend.disabled = false;
            btnSend.classList.remove('opacity-40', 'cursor-not-allowed');
        }
    }
}