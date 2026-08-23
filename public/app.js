/**
 * app.js - 模組進入點
 */
import { state } from './state.js';
import { checkInAppBrowser, hideToast } from './ui.js';
import { initHost, initJoinerWithRoom, probeIPv4 } from './rtc-channel.js';
import { sendFileChunks, evaluateFileRouting, clearEntireOPFS } from './webrtc.js';
// 修正：補上 switchPairingTab 與 scanNearbyRooms 匯入
import { startScanner, stopScanner, openPairingModal, closePairingModal, setupPinInputs, autoCheckNearbyRooms, switchPairingTab, scanNearbyRooms } from './scanner.js';

// 全域暴露機制 (供 HTML onclick 觸發)
window.initHost = initHost;
window.initJoinerWithRoom = initJoinerWithRoom;
window.startJoinerScanner = startScanner;
window.stopScanner = stopScanner;
window.openPairingModal = openPairingModal;
window.closePairingModal = closePairingModal;
window.sendFileChunks = sendFileChunks;
window.hideToast = hideToast;
window.switchPairingTab = switchPairingTab;
window.scanNearbyRooms = scanNearbyRooms;

// 頁面初始化
window.addEventListener('DOMContentLoaded', async () => {
    checkInAppBrowser();

    await probeIPv4();
    clearEntireOPFS();

    const urlParams = new URLSearchParams(window.location.search);
    const rId = urlParams.get('room');
    if (rId) {
        state.roomId = rId;
        state.isHost = false;
        initJoinerWithRoom(rId); // 修正：傳入 rId
    } else {
        autoCheckNearbyRooms();
    }

    setupDragAndDrop();
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