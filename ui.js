/**
 * ui.js - UI 輔助與環境警示模組
 * 負責 Toast 提示、內嵌瀏覽器攔截、QR Code 生成及自動下載調用。
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

// 物理下載觸發器 (避免行動端打開新分頁而導致 WebRTC 被背景掛起)
export function triggerAutoDownload(url, filename) {
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.target = state.localIsMobile ? '_self' : '_blank'; 
    document.body.appendChild(a);
    a.click();
    
    setTimeout(() => {
        document.body.removeChild(a);
    }, 150);
}