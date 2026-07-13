/**
 * scanner.js - 相機與手動/同網域配對模組
 * 包裝 getUserMedia 權限，並提供「手動配對碼輸入」與「同網域自動偵測 (2位數驗證)」的降級替代方案。
 */
import { state } from './state.js';
import { showToast } from './ui.js';
import { initJoinerWithRoom, getWorkerUrl } from './webrtc.js';

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
        console.warn("無法開啟相機，引導至手動配對選單", e);
        showToast('無法開啟相機，已自動切換至手動配對模式', 'info');
        openPairingModal();
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

// --- 🚀 新增：開啟手動配對與局域網自動偵測 Modal ---
export function openPairingModal() {
    stopScanner(); // 若相機開著先關閉
    const modal = document.getElementById('pairing-modal');
    if (modal) modal.classList.remove('hidden');
    switchPairingTab('lan'); // 預設開啟自動偵測頁籤
}

export function closePairingModal() {
    const modal = document.getElementById('pairing-modal');
    if (modal) modal.classList.add('hidden');
    resetPairingInputs();
}

// 切換手動配對 Modal 內的頁籤 (LAN偵測 / PIN輸入)
export function switchPairingTab(tab) {
    const tabLanBtn = document.getElementById('tab-lan-btn');
    const tabPinBtn = document.getElementById('tab-pin-btn');
    const viewLan = document.getElementById('view-pairing-lan');
    const viewPin = document.getElementById('view-pairing-pin');

    if (tab === 'lan') {
        tabLanBtn.className = "flex-1 py-2 text-center text-xs font-bold border-b-2 border-emerald-500 text-emerald-400";
        tabPinBtn.className = "flex-1 py-2 text-center text-xs font-bold border-b border-slate-900 text-slate-500 hover:text-slate-300";
        viewLan.classList.remove('hidden');
        viewPin.classList.add('hidden');
        scanNearbyRooms();
    } else {
        tabPinBtn.className = "flex-1 py-2 text-center text-xs font-bold border-b-2 border-blue-500 text-blue-400";
        tabLanBtn.className = "flex-1 py-2 text-center text-xs font-bold border-b border-slate-900 text-slate-500 hover:text-slate-300";
        viewPin.classList.remove('hidden');
        viewLan.classList.add('hidden');
    }
}

// 偵測附近同公網 IP 的 active rooms
async function scanNearbyRooms() {
    const statusText = document.getElementById('lan-scan-status');
    const roomListContainer = document.getElementById('lan-room-list');
    const verificationBox = document.getElementById('lan-verification-box');

    statusText.innerText = "正在搜尋附近 WiFi 的發起端...";
    roomListContainer.innerHTML = "";
    verificationBox.classList.add('hidden');

    const workerUrl = getWorkerUrl();

    try {
        // 向 Worker 請求目前相同 IP 的所有 active rooms (若 Worker 暫不支援，會拋出異常)
        const res = await fetch(`${workerUrl}/rooms-by-ip`);
        if (!res.ok) throw new Error("Worker 尚未實現 /rooms-by-ip");

        const data = await res.json();
        const rooms = data.rooms || [];

        if (rooms.length === 0) {
            statusText.innerText = "目前附近沒有正在發起傳輸的裝置。";
            roomListContainer.innerHTML = `
                <div class="text-[11px] text-slate-600 text-center py-4">
                    請確認發起端已點擊「發起傳輸」，或改用 PIN 配對碼。
                </div>
            `;
        } else if (rooms.length === 1) {
            // 最完美情況：同個 IP 底下只有一台，免輸入驗證碼，直接接通！
            const targetRoom = rooms[0];
            statusText.innerText = "成功偵測到附近裝置！";
            roomListContainer.innerHTML = `
                <button onclick="connectNearbyRoom('${targetRoom}')" class="w-full py-3 bg-emerald-600/20 hover:bg-emerald-600/40 border border-emerald-500/30 text-emerald-300 text-xs font-bold rounded-xl transition flex items-center justify-center gap-2">
                    <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span>
                    點擊一鍵連線
                </button>
            `;
        } else {
            // 複數情況（多台並行）：記錄房號，並亮出 2 位數驗證碼輸入框進行過濾
            state.nearbyRooms = rooms; 
            statusText.innerText = `偵測到 ${rooms.length} 個附近裝置，請輸入驗證碼：`;
            verificationBox.classList.remove('hidden');
            setupVerificationInputs();
        }

    } catch (err) {
        console.error("LAN 自動偵測失敗:", err);
        statusText.innerText = "此網路環境不支援自動偵測，請直接輸入 PIN 碼。";
        roomListContainer.innerHTML = `
            <button onclick="switchPairingTab('pin')" class="w-full py-2.5 bg-slate-900 hover:bg-slate-800 text-slate-300 text-xs font-bold rounded-xl transition border border-slate-800">
                👉 切換至輸入 PIN 配對碼
            </button>
        `;
    }
}

// 串接同網域直接連線
window.connectNearbyRoom = function(roomId) {
    closePairingModal();
    showToast('同網域極速接通中...', 'success');
    state.roomId = roomId;
    state.isHost = false;
    initJoinerWithRoom();
}

// 設定 2 位數驗證碼輸入框（Option A 的並行過濾）
function setupVerificationInputs() {
    const inputs = document.querySelectorAll('.lan-verify-input');
    inputs.forEach((input, index) => {
        input.value = "";
        input.addEventListener('input', (e) => {
            const val = e.target.value;
            if (val.length > 0) {
                if (index < inputs.length - 1) {
                    inputs[index + 1].focus();
                } else {
                    // 輸入完第 2 碼，立即進行驗證對齊
                    verifyAndConnectNearby();
                }
            }
        });
        input.addEventListener('keydown', (e) => {
            if (e.key === 'Backspace' && input.value.length === 0 && index > 0) {
                inputs[index - 1].focus();
            }
        });
    });
    inputs[0].focus();
}

// 比對 2 位數驗證碼，在多個 Room 中找出正確的連線
function verifyAndConnectNearby() {
    const inputs = document.querySelectorAll('.lan-verify-input');
    const enteredCode = Array.from(inputs).map(i => i.value).join('');
    if (enteredCode.length < 2) return;

    // 比對目前 IP 內所有 Room ID 的最後兩碼
    const matchedRoom = state.nearbyRooms.find(r => r.endsWith(enteredCode));

    if (matchedRoom) {
        connectNearbyRoom(matchedRoom);
    } else {
        showToast('驗證碼錯誤，請重新確認發起端畫面！', 'error');
        inputs.forEach(i => i.value = "");
        inputs[0].focus();
    }
}

// 設定 6 位數 PIN 碼手動輸入框
export function setupPinInputs() {
    const inputs = document.querySelectorAll('.pin-input');
    inputs.forEach((input, index) => {
        input.value = "";
        input.addEventListener('input', (e) => {
            const val = e.target.value;
            // 過濾非數字
            input.value = val.replace(/[^0-9]/g, '');
            
            if (input.value.length > 0) {
                if (index < inputs.length - 1) {
                    inputs[index + 1].focus();
                } else {
                    // 輸入滿 6 碼，自動扣動扳機連線！
                    submitPinPairing();
                }
            }
        });

        input.addEventListener('keydown', (e) => {
            if (e.key === 'Backspace' && input.value.length === 0 && index > 0) {
                inputs[index - 1].focus();
            }
        });
    });
}

// 提交 6 位數 PIN 碼進行 WebRTC 連線
function submitPinPairing() {
    const inputs = document.querySelectorAll('.pin-input');
    const pin = Array.from(inputs).map(i => i.value).join('');
    if (pin.length < 6) {
        showToast('請完整輸入 6 位數配對碼', 'error');
        return;
    }

    closePairingModal();
    showToast(`正在連線配對碼: ${pin.slice(0,3)} ${pin.slice(3)}`, 'success');
    state.roomId = pin;
    state.isHost = false;
    initJoinerWithRoom();
}

function resetPairingInputs() {
    document.querySelectorAll('.pin-input').forEach(i => i.value = "");
    document.querySelectorAll('.lan-verify-input').forEach(i => i.value = "");
}

// 暴露全域給 HTML onclick 使用
window.switchPairingTab = switchPairingTab;
window.closePairingModal = closePairingModal;
window.openPairingModal = openPairingModal;
window.scanNearbyRooms = scanNearbyRooms;