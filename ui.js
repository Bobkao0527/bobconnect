/**
 * ui.js - UI 輔助與環境警示模組
 * 負責 Toast 提示、內嵌瀏覽器攔截、QR Code 生成及自動下載調用。
 * 🚀【行動端 OOM 與跳轉優化】：避免大檔案擠爆 iOS 記憶體，並防止 Safari 導航遺失。
 */
import { state } from './state.js';

// 內嵌瀏覽器 (LINE, FB, Google App) 偵測
export function checkInAppBrowser() {
    const ua = navigator.userAgent || navigator.vendor || window.opera;
    const isInApp = (ua.indexOf("FBAN") > -1) || 
                    (ua.indexOf("FBAV") > -1) || 
                    (ua.indexOf("Line") > -1) || 
                    (ua.indexOf("GSA") > -1) || 
                    (ua.indexOf("Messenger") > -1) ||
                    (ua.indexOf("Instagram") > -1);
    
    if (isInApp) {
        const overlay = document.getElementById('inapp-warning-overlay');
        if (overlay) overlay.classList.remove('hidden');
    }
}

// 顯示自訂 Toast
export function showToast(msg, type = 'info') {
    const toast = document.getElementById('alert-toast');
    const toastMsg = document.getElementById('toast-msg');
    if (!toast || !toastMsg) return;

    toastMsg.innerText = msg;
    toast.className = `fixed bottom-6 right-6 z-50 p-4 rounded-xl border shadow-2xl flex items-center justify-between transition-all duration-300 max-w-sm w-full`;
    
    if (type === 'success') {
        toast.classList.add('bg-emerald-950/90', 'border-emerald-500/40', 'text-emerald-300');
    } else if (type === 'error') {
        toast.classList.add('bg-rose-950/90', 'border-rose-500/40', 'text-rose-300');
    } else {
        toast.classList.add('bg-slate-900/90', 'border-slate-800', 'text-slate-300');
    }
    toast.classList.remove('hidden');
    
    setTimeout(hideToast, 5000); 
}

// 隱藏 Toast
export function hideToast() {
    const toast = document.getElementById('alert-toast');
    if (toast) toast.classList.add('hidden');
}

// 產生 QR Code
export function generateQRCode(canvasId, text) {
    if (typeof QRious !== 'undefined') {
        new QRious({
            element: document.getElementById(canvasId),
            value: text,
            size: 200,
            level: 'M'
        });
    }
}

// 狀態燈與文字更新
export function updateStatus(text, color) {
    const indicator = document.getElementById('status-indicator');
    const statusText = document.getElementById('status-text');
    if (!statusText || !indicator) return;

    statusText.innerText = text;
    indicator.className = "w-1.5 h-1.5 rounded-full";
    
    if (color === 'green') {
        indicator.classList.add('bg-emerald-500', 'animate-pulse');
        statusText.className = "text-[10px] font-medium tracking-widest text-emerald-400";
    } else if (color === 'yellow') {
        indicator.classList.add('bg-yellow-500', 'animate-pulse');
        statusText.className = "text-[10px] font-medium tracking-widest text-yellow-400";
    } else {
        indicator.classList.add('bg-red-500');
        statusText.className = "text-[10px] font-medium tracking-widest text-red-400";
    }
}

// 🚀【行動端終極安全優化】：智慧過濾並喚起分享選單或原生背景下載
export async function triggerAutoDownload(url, filename, fileSize = 0) {
    // 🚀【iOS OOM 防閃退】：如果檔案大於 50MB，絕對不能使用 JS fetch 載入記憶體！
    // 否則 iOS Safari 分頁會直接因為記憶體不足閃退，或者無法成功分享（iOS 分享檔案有嚴格大小限制）。
    const sizeLimit = 50 * 1024 * 1024; // 50MB 限制
    
    if (state.localIsMobile && fileSize < sizeLimit && navigator.canShare && navigator.share) {
        try {
            const response = await fetch(url);
            const blob = await response.blob();
            const file = new File([blob], filename, { type: blob.type || 'application/octet-stream' });

            if (navigator.canShare({ files: [file] })) {
                await navigator.share({
                    files: [file],
                    title: filename,
                    text: `Bob 檔案傳輸系統成功接收：${filename}`
                });
                return; // 分享成功後直接返回
            }
        } catch (err) {
            console.warn("[系統提示] 原生分享未成功，將降級走瀏覽器預設下載機制。", err);
        }
    }

    // 🖥️ 電腦端或大檔案（>50MB）的行動端原生下載路徑
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    
    // 🚀【Safari 跨網域跳轉防護】：
    // iOS 跨網域下載不支援 a.download 屬性。如果設為 _self，Safari 會直接導航離開當前 App 網頁。
    // 強制使用 _blank 會開啟一個新分頁。Safari 偵測到 Worker 回傳的 Content-Disposition 附件標頭後，
    // 會彈出原生下載選單，並在背景下載檔案，同時「完美保留」原來的傳輸分頁與 WebRTC 連線！
    a.target = '_blank'; 
    document.body.appendChild(a);
    a.click();
    
    setTimeout(() => {
        document.body.removeChild(a);
    }, 150);
}