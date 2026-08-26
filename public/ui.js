/**
 * ui.js - UI 輔助與環境警示模組
 * 負責 Toast 提示、內嵌瀏覽器攔截、QR Code 生成及自動下載調用。
 * 🚀【更新】：新增 triggerAppleGlow 方法，用於在直連開通瞬間觸發全螢幕智慧霓虹流光。
 */

// 內嵌瀏覽器 (LINE, FB, Google App) 偵測
export function checkInAppBrowser() {
    const ua = navigator.userAgent || navigator.vendor || window.opera;
    const isInApp = (ua.indexOf("FBAN") > -1) ||
        (ua.indexOf("FBAV") > -1) ||
        (ua.indexOf("Line") > -1) ||
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
        setPairedUIState(true);
    } else if (color === 'yellow') {
        indicator.classList.add('bg-yellow-500', 'animate-pulse');
        statusText.className = "text-[10px] font-medium tracking-widest text-yellow-400";
    } else {
        indicator.classList.add('bg-red-500');
        statusText.className = "text-[10px] font-medium tracking-widest text-red-400";
        setPairedUIState(false);
    }
}

// 🚀 切換配對前後的 UI 按鈕顯示狀態
export function setPairedUIState(isPaired) {
    const manualPairingBtn = document.getElementById('btn-manual-pairing');
    const pairedActions = document.getElementById('paired-actions');

    if (manualPairingBtn && pairedActions) {
        if (isPaired) {
            manualPairingBtn.classList.add('hidden');
            pairedActions.classList.remove('hidden');
        } else {
            manualPairingBtn.classList.remove('hidden');
            pairedActions.classList.add('hidden');
        }
    }
}

// 🚀【彩色流光邊框控制器】：高階 Apple Intelligence 動態漸層淡入淡出
export function triggerAppleGlow() {
    const glow = document.getElementById('intel-glow');
    if (!glow) return;

    // 清除舊有的 active，重新觸發
    glow.classList.remove('active');
    void glow.offsetWidth; // 🚀 強制觸發重繪 (Reflow) 以確保動畫能重複被執行

    glow.classList.add('active');

    // 流光持續 1 秒後，優雅淡出
    setTimeout(() => {
        glow.classList.remove('active');
    }, 1000);
}

// 螢幕防休眠鎖 (Wake Lock)
let wakeLockSentinel = null;

export async function acquireWakeLock() {
    if ('wakeLock' in navigator) {
        try {
            if (!wakeLockSentinel) {
                wakeLockSentinel = await navigator.wakeLock.request('screen');
                wakeLockSentinel.addEventListener('release', () => {
                    wakeLockSentinel = null;
                    updateWakeLockButtonState(false);
                });
                console.log('[系統狀態] 螢幕保持常亮 (Wake Lock 已啟用)');
                updateWakeLockButtonState(true);
                return true;
            }
        } catch (err) {
            console.warn('[系統狀態] 無法取得 Wake Lock:', err);
            return false;
        }
    }
    return false;
}

export function releaseWakeLock() {
    if (wakeLockSentinel) {
        try {
            wakeLockSentinel.release();
        } catch (err) { }
        wakeLockSentinel = null;
        console.log('[系統狀態] 螢幕常亮鎖已安全釋放');
        updateWakeLockButtonState(false);
    }
}

// 專為 Safari 手勢觸發設計的 Wake Lock 切換函式
export async function toggleWakeLock() {
    if (wakeLockSentinel) {
        releaseWakeLock();
        showToast('已關閉螢幕長亮', 'info');
    } else {
        const success = await acquireWakeLock();
        if (success) {
            showToast('已成功保持螢幕長亮', 'success');
        } else {
            showToast('無法啟用螢幕長亮（不支援或未允許權限）', 'error');
        }
    }
}

// 更新 Wake Lock 按鈕點亮/熄滅狀態
function updateWakeLockButtonState(isActive) {
    const btn = document.getElementById('btn-toggle-wakelock');
    if (!btn) return;

    if (isActive) {
        btn.className = "px-3.5 py-1.5 bg-emerald-950/60 border border-emerald-500/40 text-emerald-300 text-[11px] font-medium rounded-xl transition-all shadow-sm flex items-center gap-1.5";
        btn.innerHTML = '✨ 螢幕常亮中';
    } else {
        btn.className = "px-3.5 py-1.5 bg-slate-900 hover:bg-slate-800 border border-slate-800 hover:border-slate-700 text-slate-300 hover:text-white text-[11px] font-medium rounded-xl transition-all shadow-sm flex items-center gap-1.5";
        btn.innerHTML = '💡 保持螢幕長亮';
    }
}

// 行動端原生分享或背景傳統下載
export function triggerAutoDownload(url, filename, fileSize = 0) {
    const a = document.createElement('a');

    a.href = url;
    a.download = filename || 'downloaded_file';
    a.rel = 'noopener';
    a.style.position = 'fixed';
    a.style.left = '-9999px';
    a.style.width = '1px';
    a.style.height = '1px';
    a.style.opacity = '0';

    document.body.appendChild(a);

    // Safari / WebKit：
    // 不要在建立 Blob URL 的同一個同步執行階段立即 click。
    setTimeout(() => {
        try {
            a.click();
            console.log('[下載] 已觸發 Safari Blob URL 下載');
        } catch (err) {
            console.error('[下載] 自動下載觸發失敗:', err);
        }
    }, 100);

    // 自動下載的 anchor 本身可以清掉，
    // 但不要在這裡 revokeObjectURL(url)。
    setTimeout(() => {
        a.remove();
    }, 1500);

    // 手動下載按鈕共用同一個仍有效的 Blob URL
    const manualContainer = document.getElementById('manual-download-container');
    const manualBtn = document.getElementById('btn-manual-download');

    if (manualContainer && manualBtn) {
        manualContainer.classList.remove('hidden');

        manualBtn.onclick = () => {
            const manualLink = document.createElement('a');

            manualLink.href = url;
            manualLink.download = filename || 'downloaded_file';
            manualLink.rel = 'noopener';
            manualLink.style.position = 'fixed';
            manualLink.style.left = '-9999px';

            document.body.appendChild(manualLink);

            setTimeout(() => {
                try {
                    manualLink.click();
                    console.log('[下載] 手動重試下載');
                } catch (err) {
                    console.error('[下載] 手動下載失敗:', err);
                }

                setTimeout(() => {
                    manualLink.remove();
                }, 1000);
            }, 50);
        };
    }
}