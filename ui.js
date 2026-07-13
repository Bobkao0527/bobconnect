/**
 * ui.js - UI 輔助與環境警示模組
 * 負責 Toast 提示、內嵌瀏覽器攔截、QR Code 生成及自動下載調用。
 * 🚀【行動端優化】：下載器支援行動端自動轉換為 Web Share API 原生分享選單。
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

// 🚀【行動端終極優化】：自動判斷並呼叫 Web Share API 分享選單
export async function triggerAutoDownload(url, filename) {
    // 偵測是否為行動裝置，且瀏覽器支援完整的檔案分享 API (如 iOS Safari, Android Chrome)
    if (state.localIsMobile && navigator.canShare && navigator.share) {
        try {
            // 從 Local Blob URL 提取 Blob 二進位數據並包裝成 File 物件
            const response = await fetch(url);
            const blob = await response.blob();
            const file = new File([blob], filename, { type: blob.type || 'application/octet-stream' });

            // 確認該檔案格式在系統安全允許的分享範圍內 (iOS/Android 支援絕大多數影音、相片、PDF)
            if (navigator.canShare({ files: [file] })) {
                await navigator.share({
                    files: [file],
                    title: filename,
                    text: `Bob 檔案傳輸系統成功接收：${filename}`
                });
                return; // 喚起分享成功，直接跳出不執行下方 traditional anchor 下載
            }
        } catch (err) {
            // 如果是因為「非使用者手勢自動觸發」或「使用者手動按取消」，則優雅降級走 traditional 下載
            console.warn("[系統提示] 原生分享未被手勢啟動或已被取消，改走瀏覽器預設存檔機制。", err);
        }
    }

    // 🖥️ 電腦端或降級方案：傳統的 a 標籤點擊下載
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