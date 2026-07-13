/**
 * webrtc.js - WebRTC 直連通道與信令核心
 * 負責連線握手、ICE 收集、連線狀態維護，並在連線開通時自我清理雲端信令空間。
 * 🚀【優化】：改自 state.js 導入 getWorkerUrl，破除循環依賴。
 */
import { state, getWorkerUrl } from './state.js';
import { showToast, updateStatus, generateQRCode, triggerAutoDownload } from './ui.js';
import { downloadAndCleanR2, destroyR2File, sendViaR2Multipart } from './r2.js';

// 連線成功即銷毀雲端信令房間
export async function destroyCloudRoom() {
    if (!state.roomId) return;
    const workerUrl = getWorkerUrl();
    try {
        await fetch(`${workerUrl}/room/${state.roomId}`, { method: 'DELETE', keepalive: true });
        console.log(`[安全機制] 房間 ${state.roomId} 的雲端信令已成功手動銷毀。`);
    } catch (e) {
        console.error("手動銷毀房間失敗:", e);
    }
}

// 發起者流程 (Host)
export async function initHost() {
    state.isHost = true;
    
    // Room ID 升級為 6 位數純數字 PIN 碼！
    const numPin = Math.floor(100000 + Math.random() * 900000).toString();
    state.roomId = numPin;

    document.getElementById('setup-view').classList.add('hidden');
    document.getElementById('signaling-box').classList.remove('hidden');
    document.getElementById('host-panel').classList.remove('hidden');

    const formattedPin = `${numPin.slice(0, 3)} ${numPin.slice(3)}`;
    const pinDisplay = document.getElementById('host-pin-display');
    if (pinDisplay) {
        pinDisplay.innerText = formattedPin;
    }

    const githubPagesOrigin = "https://bobkao0527.github.io/bobconnect/";
    const joinUrl = `${githubPagesOrigin}?room=${state.roomId}`;
    generateQRCode('qr-canvas', joinUrl);

    try {
        state.peerConnection = new RTCPeerConnection(state.rtcConfig);

        state.dataChannel = state.peerConnection.createDataChannel('fileTransfer', { ordered: true });
        state.dataChannel.bufferedAmountLowThreshold = 131072; // 128KB Low threshold
        setupDataChannelListeners();

        state.peerConnection.onicecandidate = async (event) => {
            if (!event.candidate) {
                const offer = state.peerConnection.localDescription;
                const workerUrl = getWorkerUrl();
                
                try {
                    const res = await fetch(`${workerUrl}/room/${state.roomId}`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ offer })
                    });
                    if (res.ok) {
                        updateStatus('信令伺服器就緒，等候接入', 'yellow');
                        startCheckingForAnswer();
                    } else {
                        throw new Error();
                    }
                } catch (err) {
                    showToast('無法與信令伺服器同步', 'error');
                }
            }
        };

        const offer = await state.peerConnection.createOffer();
        await state.peerConnection.setLocalDescription(offer);
        updateStatus('正在同步中轉服務...', 'yellow');

    } catch (e) {
        console.error(e);
        showToast('初始化失敗: ' + e.message, 'error');
    }
}

// 輪詢檢查接收端 Answer
export function startCheckingForAnswer() {
    if (state.answerCheckInterval) clearInterval(state.answerCheckInterval);
    const workerUrl = getWorkerUrl();

    state.answerCheckInterval = setInterval(async () => {
        try {
            const res = await fetch(`${workerUrl}/room/${state.roomId}`);
            if (res.ok) {
                const data = await res.json();
                if (data && data.answer) {
                    clearInterval(state.answerCheckInterval);
                    state.answerCheckInterval = null;
                    
                    await state.peerConnection.setRemoteDescription(new RTCSessionDescription(data.answer));
                    showToast('成功對齊傳輸描述，建立直連中！', 'success');
                }
            }
        } catch (e) {
            console.error(e);
        }
    }, 1500);
}

// 接收端：自動對接
export async function initJoinerWithRoom() {
    document.getElementById('setup-view').classList.add('hidden');
    document.getElementById('signaling-box').classList.remove('hidden');
    document.getElementById('joiner-panel').classList.remove('hidden');
    updateStatus('正在提取信令鑰匙...', 'yellow');

    const workerUrl = getWorkerUrl();

    try {
        const res = await fetch(`${workerUrl}/room/${state.roomId}`);
        if (!res.ok) throw new Error('提取信令失敗');
        const data = await res.json();
        if (!data || !data.offer) throw new Error('連線可能超時或失效，請發起端重新產生');

        state.peerConnection = new RTCPeerConnection(state.rtcConfig);

        state.peerConnection.ondatachannel = (event) => {
            state.dataChannel = event.channel;
            state.dataChannel.bufferedAmountLowThreshold = 131072;
            setupDataChannelListeners();
        };

        state.peerConnection.onicecandidate = async (event) => {
            if (!event.candidate) {
                const answer = state.peerConnection.localDescription;
                try {
                    const postRes = await fetch(`${workerUrl}/room/${state.roomId}`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ answer })
                    });
                    if (postRes.ok) {
                        updateStatus('密鑰交換完畢，等待通道開放', 'yellow');
                    }
                } catch (err) {
                    showToast('回應金鑰失敗', 'error');
                }
            }
        };

        await state.peerConnection.setRemoteDescription(new RTCSessionDescription(data.offer));
        const answer = await state.peerConnection.createAnswer();
        await state.peerConnection.setLocalDescription(answer);

    } catch (e) {
        showToast(e.message, 'error');
        updateStatus('連線建立中斷', 'red');
    }
}

// 設定 DataChannel 監聽
export function setupDataChannelListeners() {
    state.dataChannel.onopen = () => {
        updateStatus('P2P 直連已開通 (加密)', 'green');
        showToast('極速 P2P 連結就緒', 'success');
        
        state.dataChannel.send(JSON.stringify({
            type: 'device-info',
            isMobile: state.localIsMobile
        }));

        destroyCloudRoom();

        document.getElementById('setup-view').classList.add('hidden');
        document.getElementById('signaling-box').classList.add('hidden');
        
        const tfPanel = document.getElementById('transfer-panel');
        tfPanel.classList.remove('hidden');
        tfPanel.style.opacity = '1';
    };

    state.dataChannel.onclose = () => {
        updateStatus('連線已被安全釋放', 'red');
        showToast('連結已釋放斷開', 'error');
        document.getElementById('transfer-panel').classList.add('hidden');
        document.getElementById('setup-view').classList.remove('hidden');
    };

    state.dataChannel.onmessage = (event) => {
        handleIncomingData(event.data);
    };
}

// 處理接收資料流
export function handleIncomingData(data) {
    if (typeof data === 'string') {
        try {
            const message = JSON.parse(data);
            
            if (message.type === 'device-info') {
                state.remoteIsMobile = message.isMobile;
                console.log(`[設備對齊] 對方是否為行動裝置: ${state.remoteIsMobile}`);
                if (state.selectedFile) {
                    evaluateFileRouting(state.selectedFile);
                }
                return;
            }

            if (message.type === 'file-r2-upload-start') {
                state.incomingFileInfo = {
                    name: message.name,
                    size: message.size
                };
                document.getElementById('incoming-file-box').classList.remove('hidden');
                document.getElementById('incoming-status-title').innerHTML = `
                    <span class="inline-block w-1.5 h-1.5 rounded-full bg-indigo-400 animate-pulse"></span>
                    對方正將大檔案上傳至雲端通道...
                `;
                document.getElementById('recv-file-name').innerText = `[上傳中] ${state.incomingFileInfo.name}`;
                document.getElementById('recv-percent').innerText = '0%';
                document.getElementById('recv-progress-bar').style.width = '0%';
                document.getElementById('recv-progress-bar').className = "bg-gradient-to-r from-indigo-500 to-purple-500 h-1";
                document.getElementById('manual-download-container').classList.add('hidden');
                return;
            }

            if (message.type === 'file-r2-upload-progress') {
                const percent = message.percent;
                document.getElementById('recv-percent').innerText = `對方已上傳 ${percent}%`;
                document.getElementById('recv-progress-bar').style.width = `${percent}%`;
                return;
            }

            if (message.type === 'file-r2-upload-cancel') {
                document.getElementById('incoming-file-box').classList.add('hidden');
                showToast('對方雲端上傳已中斷或取消。', 'error');
                return;
            }

            if (message.type === 'file-meta') {
                state.incomingFileInfo = {
                    name: message.name,
                    size: message.size
                };
                state.receiveBuffer = [];
                state.receivedSize = 0;
                state.lastRecvUiTime = 0;

                document.getElementById('incoming-file-box').classList.remove('hidden');
                document.getElementById('incoming-status-title').innerHTML = `
                    <span class="inline-block w-1.5 h-1.5 rounded-full bg-emerald-400 animate-ping"></span>
                    正在接收檔案...
                `;
                document.getElementById('recv-file-name').innerText = state.incomingFileInfo.name;
                document.getElementById('recv-percent').innerText = '0%';
                document.getElementById('recv-progress-bar').style.width = '0%';
                document.getElementById('recv-progress-bar').className = "bg-gradient-to-r from-emerald-500 to-teal-400 h-1";
                document.getElementById('manual-download-container').classList.add('hidden');
            
            } else if (message.type === 'file-end') {
                const blob = new Blob(state.receiveBuffer);
                state.receiveBuffer = []; // 清空緩衝
                
                const url = URL.createObjectURL(blob);
                triggerAutoDownload(url, state.incomingFileInfo.name);

                const btnManual = document.getElementById('btn-manual-download');
                if (btnManual) {
                    btnManual.innerText = state.localIsMobile ? '📤 儲存 / 分享檔案' : '📥 手動下載檔案';
                    btnManual.onclick = () => triggerAutoDownload(url, state.incomingFileInfo.name);
                }
                document.getElementById('manual-download-container').classList.remove('hidden');

                document.getElementById('recv-percent').innerText = '100% (已完成)';
                document.getElementById('recv-progress-bar').style.width = '100%';
                document.getElementById('recv-progress-bar').className = "bg-emerald-500 h-1 w-full";
                showToast(`「${state.incomingFileInfo.name}」傳輸完成！`, 'success');
                
                setTimeout(() => {
                    URL.revokeObjectURL(url);
                }, 30000);
            
            } else if (message.type === 'file-r2') {
                state.incomingFileInfo = {
                    name: message.name,
                    size: message.size
                };

                document.getElementById('incoming-file-box').classList.remove('hidden');

                if (state.localIsMobile) {
                    document.getElementById('incoming-status-title').innerHTML = `
                        <span class="inline-block w-1.5 h-1.5 rounded-full bg-emerald-400 animate-ping"></span>
                        行動端直連下載就緒
                    `;
                    document.getElementById('recv-file-name').innerText = `[直連下載] ${state.incomingFileInfo.name}`;
                    document.getElementById('recv-percent').innerText = '100% (已喚起瀏覽器原生下載)';
                    document.getElementById('recv-progress-bar').style.width = '100%';
                    document.getElementById('recv-progress-bar').className = "bg-emerald-500 h-1";

                    triggerAutoDownload(message.downloadUrl, state.incomingFileInfo.name);

                    setTimeout(() => {
                        destroyR2File(message.downloadUrl);
                    }, 8000);

                    const btnManual = document.getElementById('btn-manual-download');
                    if (btnManual) {
                        btnManual.innerText = '📤 儲存 / 分享檔案';
                        btnManual.onclick = () => triggerAutoDownload(message.downloadUrl, state.incomingFileInfo.name);
                    }
                    document.getElementById('manual-download-container').classList.remove('hidden');
                    
                    showToast(`已成功喚起原生下載，檔案將自動銷毀。`, 'success');

                } else {
                    document.getElementById('incoming-status-title').innerHTML = `
                        <span class="inline-block w-1.5 h-1.5 rounded-full bg-blue-400 animate-ping"></span>
                        正在進行 R2 串流下載...
                    `;
                    document.getElementById('recv-file-name').innerText = `[R2 高速模式] ${state.incomingFileInfo.name}`;
                    document.getElementById('recv-percent').innerText = '0%';
                    document.getElementById('recv-progress-bar').style.width = '0%';
                    document.getElementById('recv-progress-bar').className = "bg-blue-500 h-1";

                    downloadAndCleanR2(message.downloadUrl, state.incomingFileInfo.name, state.incomingFileInfo.size);
                }
            }
        } catch (e) {
            console.error(e);
        }
    } else {
        // 標準 WebRTC 寫入
        state.receiveBuffer.push(data);
        state.receivedSize += data.byteLength;

        if (state.incomingFileInfo) {
            const now = Date.now();
            if (now - state.lastRecvUiTime > 100 || state.receivedSize >= state.incomingFileInfo.size) {
                state.lastRecvUiTime = now;
                const percent = Math.min(100, Math.round((state.receivedSize / state.incomingFileInfo.size) * 100));
                document.getElementById('recv-percent').innerText = `${percent}%`;
                document.getElementById('recv-progress-bar').style.width = `${percent}%`;
            }
        }
    }
}

// 智慧分流引擎評估
export function evaluateFileRouting(file) {
    const atLeastOneMobile = state.localIsMobile || state.remoteIsMobile;
    const size512MB = 512 * 1024 * 1024;
    const size9GB = 9 * 1024 * 1024 * 1024;
    const btnSend = document.getElementById('btn-send');

    if (!btnSend) return;
    btnSend.disabled = false;
    btnSend.classList.remove('opacity-40', 'cursor-not-allowed');

    if (atLeastOneMobile) {
        if (file.size > size9GB) {
            state.useR2 = false;
            document.getElementById('progress-status').innerText = '大於 9GB 限額，無法發送';
            document.getElementById('progress-bar').style.width = '0%';
            btnSend.disabled = true;
            btnSend.classList.add('opacity-40', 'cursor-not-allowed');
            showToast('檔案大於 9GB，已安全封鎖。', 'error');
        } else if (file.size > size512MB) {
            state.useR2 = true;
            document.getElementById('progress-status').innerText = '檔案超過 512MB，已自動套用 R2 模式';
            document.getElementById('progress-bar').className = "bg-gradient-to-r from-blue-500 to-indigo-500 h-1 w-0 transition-all duration-75";
            showToast('已啟用 R2 零內存下載防護！', 'success');
        } else {
            state.useR2 = false;
            document.getElementById('progress-status').innerText = '隨時可以發送 (P2P 模式)';
            document.getElementById('progress-bar').className = "bg-gradient-to-r from-blue-500 to-emerald-400 h-1 w-0 transition-all duration-75";
        }
    } else {
        state.useR2 = false;
        document.getElementById('progress-status').innerText = '雙端皆為電腦，啟用無限 P2P 高速傳輸';
        document.getElementById('progress-bar').className = "bg-gradient-to-r from-blue-500 to-emerald-400 h-1 w-0 transition-all duration-75";
    }
}

// 發送端：決定並執行傳輸
export async function sendFileChunks() {
    if (!state.selectedFile || !state.dataChannel || state.dataChannel.readyState !== 'open') {
        showToast('通道尚未連線或未選擇檔案', 'error');
        return;
    }

    if (state.useR2) {
        sendViaR2Multipart();
        return;
    }

    const btnSend = document.getElementById('btn-send');
    if (btnSend) {
        btnSend.disabled = true;
        btnSend.classList.add('opacity-40', 'cursor-not-allowed');
    }

    const meta = {
        type: 'file-meta',
        name: state.selectedFile.name,
        size: state.selectedFile.size
    };
    state.dataChannel.send(JSON.stringify(meta));

    const chunkSize = 65536; // 64KB 切片
    let offset = 0;
    const startTime = Date.now();
    let lastUiUpdateTime = 0;

    document.getElementById('progress-status').innerText = '發送中...';
    state.dataChannel.bufferedAmountLowThreshold = 131072;

    try {
        while (offset < state.selectedFile.size) {
            if (state.dataChannel.bufferedAmount > 262144) {
                await new Promise(resolve => {
                    state.dataChannel.onbufferedamountlow = () => {
                        state.dataChannel.onbufferedamountlow = null;
                        resolve();
                    };
                });
            }

            const slice = state.selectedFile.slice(offset, offset + chunkSize);
            const buffer = await slice.arrayBuffer();

            state.dataChannel.send(buffer);
            offset += buffer.byteLength;

            const now = Date.now();
            if (now - lastUiUpdateTime > 100 || offset >= state.selectedFile.size) {
                lastUiUpdateTime = now;

                const percent = Math.min(100, Math.round((offset / state.selectedFile.size) * 100));
                document.getElementById('progress-percent').innerText = `${percent}%`;
                document.getElementById('progress-bar').style.width = `${percent}%`;

                const elapsedTime = (now - startTime) / 1000;
                const speedBytes = offset / elapsedTime;
                const speedMB = (speedBytes / (1024 * 1024)).toFixed(2);
                document.getElementById('transfer-speed').innerText = `${speedMB} MB/s`;

                if (speedBytes > 0) {
                    const remainingBytes = state.selectedFile.size - offset;
                    const remainingSeconds = Math.max(0, Math.round(remainingBytes / speedBytes));
                    const mins = Math.floor(remainingSeconds / 60);
                    const secs = remainingSeconds % 60;
                    document.getElementById('transfer-time').innerText = `${mins}:${secs < 10 ? '0' : ''}${secs}`;
                }
            }
        }

        state.dataChannel.send(JSON.stringify({ type: 'file-end' }));
        document.getElementById('progress-status').innerText = '傳送完畢';
        showToast('檔案已完全投遞給接收端！', 'success');

    } catch (e) {
        console.error("傳輸發生嚴重錯誤:", e);
        showToast('傳輸中斷，請重試。', 'error');
    } finally {
        if (btnSend) {
            btnSend.disabled = false;
            btnSend.classList.remove('opacity-40', 'cursor-not-allowed');
        }
    }
}