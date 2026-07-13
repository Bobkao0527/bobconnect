/**
 * scanner.js - 相機掃描模組
 * 包裝 getUserMedia 權限申請，並使用外部 jsQR 函式庫進行即時畫布格點解碼。
 */
import { state } from './state.js';
import { showToast } from './ui.js';
import { initJoinerWithRoom } from './webrtc.js';

// 啟動相機掃描
export async function startScanner() {
    const modal = document.getElementById('scanner-modal');
    const video = document.getElementById('scan-video');
    if (!modal || !video) return;

    try {
        state.videoStream = await navigator.mediaDevices.getUserMedia({ 
            video: { facingMode: 'environment' } 
        });
        video.srcObject = state.videoStream;
        video.setAttribute('playsinline', true);
        video.play();
        
        modal.classList.remove('hidden');
        state.scanAnimationId = requestAnimationFrame(tickScanner);
    } catch (e) {
        showToast('無法開啟相機，請檢查權限設定', 'error');
    }
}

// 停止相機掃描
export function stopScanner() {
    const modal = document.getElementById('scanner-modal');
    if (modal) modal.classList.add('hidden');
    
    if (state.scanAnimationId) cancelAnimationFrame(state.scanAnimationId);
    if (state.videoStream) {
        state.videoStream.getTracks().forEach(track => track.stop());
        state.videoStream = null;
    }
}

// 掃描偵測迴圈
export function tickScanner() {
    const video = document.getElementById('scan-video');
    const canvas = document.getElementById('scan-canvas');
    if (!video || !canvas) return;

    const context = canvas.getContext('2d');

    if (video.readyState === video.HAVE_ENOUGH_DATA) {
        canvas.width = video.videoWidth;
        canvas.height = video.videoHeight;
        context.drawImage(video, 0, 0, canvas.width, canvas.height);
        
        const imageData = context.getImageData(0, 0, canvas.width, canvas.height);
        
        // 使用 window.jsQR 全域變數
        if (typeof jsQR !== 'undefined') {
            const code = jsQR(imageData.data, imageData.width, imageData.height, {
                versionAttempts: "dontInvert",
            });
            
            if (code) {
                stopScanner();
                try {
                    const url = new URL(code.data);
                    const rId = url.searchParams.get('room');
                    if (rId) {
                        showToast('解鎖對齊成功！一鍵握手中...', 'success');
                        state.roomId = rId;
                        state.isHost = false;
                        initJoinerWithRoom();
                    } else {
                        showToast('無效的對接二維碼', 'error');
                    }
                } catch (e) {
                    showToast('讀取失敗：無效格式', 'error');
                }
                return;
            }
        }
    }
    state.scanAnimationId = requestAnimationFrame(tickScanner);
}