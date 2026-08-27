/**
 * app.js - 模組進入點
 * 綁定檔案拖曳 UI 事件，並將 initHost、startJoinerScanner 等模組化函式
 * 掛載至全域 window，確保與 HTML 中的 inline onclick 屬性 100% 相容。
 * 🚀【智慧自動對齊優化】：若網址沒有帶房間號參數，自動向背景探測同 IP 下 60s 內房間並自動連線！
 */
import { state } from './state.js';
import { checkInAppBrowser, hideToast, toggleWakeLock, showToast} from './ui.js';
import { initHost, initJoinerWithRoom, sendFileChunks, evaluateFileRouting, probeIPv4, clearEntireOPFS } from './webrtc.js';
import { startScanner, stopScanner, openPairingModal, closePairingModal, setupPinInputs, autoCheckNearbyRooms } from './scanner.js';

async function handleClearOPFS() {
    try {
        await clearEntireOPFS();
        showToast('OPFS 快取已成功清空', 'success');
    } catch (err) {
        console.error('[OPFS] 清空失敗:', err);
        showToast('清空 OPFS 快取失敗', 'error');
    }
}

// 全域暴露機制
window.initHost = initHost;
window.initJoinerWithRoom = initJoinerWithRoom;
window.startJoinerScanner = startScanner;
window.stopScanner = stopScanner;
window.openPairingModal = openPairingModal;
window.closePairingModal = closePairingModal;
window.sendFileChunks = sendFileChunks;
window.hideToast = hideToast;
window.toggleWakeLock = toggleWakeLock;
window.handleClearOPFS = handleClearOPFS;

function setupButtonHandlers() {
    document.getElementById('btn-role-host')?.addEventListener('click', initHost);
    document.getElementById('btn-role-joiner')?.addEventListener('click', startScanner);
    document.getElementById('btn-manual-pairing')?.addEventListener('click', openPairingModal);
    document.getElementById('btn-clear-opfs')?.addEventListener('click', handleClearOPFS);
    document.getElementById('btn-toggle-wakelock')?.addEventListener('click', toggleWakeLock);
    document.getElementById('btn-send')?.addEventListener('click', sendFileChunks);
    document.getElementById('btn-hide-toast')?.addEventListener('click', hideToast);
    document.getElementById('btn-open-pairing-from-scanner')?.addEventListener('click', openPairingModal);
    document.getElementById('btn-stop-scanner')?.addEventListener('click', stopScanner);
    document.getElementById('btn-close-pairing')?.addEventListener('click', closePairingModal);
    document.getElementById('tab-lan-btn')?.addEventListener('click', () => switchPairingTab('lan'));
    document.getElementById('tab-pin-btn')?.addEventListener('click', () => switchPairingTab('pin'));
    document.getElementById('btn-scan-nearby')?.addEventListener('click', scanNearbyRooms);
}

// 頁面初始化
window.addEventListener('DOMContentLoaded', async () => {
    setupButtonHandlers();
    checkInAppBrowser(); // 檢查內嵌瀏覽器防暴走

    // 🚀【強制 IPv4 探針】：取得公網 IPv4 確保能進行同網域精準匹配
    // await 探針以確保下一步自動探測時能夾帶最穩定的 IP 金鑰
    await probeIPv4();
    clearEntireOPFS();

    // 解析路徑自動加入
    const urlParams = new URLSearchParams(window.location.search);
    const rId = urlParams.get('room');
    if (rId) {
        state.roomId = rId;
        state.isHost = false;
        initJoinerWithRoom(rId);
    } else {
        autoCheckNearbyRooms();
    }

    // 拖曳區事件註冊
    setupDragAndDrop();

    // 初始化 PIN 碼多欄位輸入傾聽器
    setupPinInputs();
});

function setupDragAndDrop() {
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');

    if (dropZone && fileInput) {
        dropZone.addEventListener('click', () => fileInput.click());

        dropZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            dropZone.classList.add('border-blue-500', 'bg-slate-950/90');
        });

        dropZone.addEventListener('dragleave', () => {
            dropZone.classList.remove('border-blue-500', 'bg-slate-950/90');
        });

        dropZone.addEventListener('drop', (e) => {
            e.preventDefault();
            dropZone.classList.remove('border-blue-500', 'bg-slate-950/90');
            if (e.dataTransfer.files.length > 0) {
                handleFileSelect(e.dataTransfer.files[0]);
            }
        });

        fileInput.addEventListener('change', (e) => {
            if (e.target.files.length > 0) {
                handleFileSelect(e.target.files[0]);
            }
        });
    }
}

function handleFileSelect(file) {
    state.selectedFile = file;

    let sizeStr = file.size + ' Bytes';
    if (file.size > 1024 * 1024 * 1024) {
        sizeStr = (file.size / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
    } else if (file.size > 1024 * 1024) {
        sizeStr = (file.size / (1024 * 1024)).toFixed(2) + ' MB';
    } else if (file.size > 1024) {
        sizeStr = (file.size / 1024).toFixed(2) + ' KB';
    }

    document.getElementById('file-info-container').classList.remove('hidden');
    document.getElementById('info-file-name').innerText = file.name;
    document.getElementById('info-file-size').innerText = sizeStr;

    document.getElementById('progress-percent').innerText = '0%';
    document.getElementById('progress-bar').style.width = '0%';
    const speedText = document.getElementById('transfer-speed');
    const timeText = document.getElementById('transfer-time');
    if (speedText) speedText.innerText = `0.00 MB/s`;
    if (timeText) timeText.innerText = `--:--`;

    evaluateFileRouting(file);
}