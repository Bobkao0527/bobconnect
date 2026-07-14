/**
 * app.js - 模組進入點
 * 綁定檔案拖曳 UI 事件，並將 initHost、startJoinerScanner 等模組化函式
 * 掛載至全域 window，確保與 HTML 中的 inline onclick 屬性 100% 相容。
 */
import { state } from './state.js';
import { checkInAppBrowser, hideToast } from './ui.js';
import { initHost, initJoinerWithRoom, sendFileChunks, evaluateFileRouting, probeIPv4 } from './webrtc.js';
import { startScanner, stopScanner, openPairingModal, closePairingModal, setupPinInputs } from './scanner.js';

// 全域暴露機制
window.initHost = initHost;
window.startJoinerScanner = startScanner;
window.stopScanner = stopScanner;
window.openPairingModal = openPairingModal;
window.closePairingModal = closePairingModal;
window.sendFileChunks = sendFileChunks;
window.hideToast = hideToast;

// 頁面初始化
window.addEventListener('DOMContentLoaded', async () => {
    checkInAppBrowser(); // 檢查內嵌瀏覽器防暴走
    
    // 🚀【強製 IPv4 探針】：在背景默默執行探針，精準取得對外公網 IPv4
    // 採用 async 非阻塞，確保前端 UI 在 0ms 延遲下立即可用！
    probeIPv4();

    // 解析路徑自動加入
    const urlParams = new URLSearchParams(window.location.search);
    const rId = urlParams.get('room');
    if (rId) {
        state.roomId = rId;
        state.isHost = false;
        initJoinerWithRoom();
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