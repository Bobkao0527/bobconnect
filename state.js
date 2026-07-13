/**
 * state.js - 全域狀態管理中心
 * 集中管理 WebRTC、R2 傳輸、相機掃描等所有共享狀態，並提供底層公用配置，避免模組間的循環引用。
 */
export const state = {
    peerConnection: null,
    dataChannel: null,
    selectedFile: null,
    receiveBuffer: [],
    receivedSize: 0,
    incomingFileInfo: null,
    
    roomId: null,
    isHost: false,
    answerCheckInterval: null,

    // 局域網多端偵測過濾儲存槽
    nearbyRooms: [],

    // 設備特徵識別 (智慧分流核心)
    localIsMobile: /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent),
    remoteIsMobile: false, 
    useR2: false,

    // 相機與 UI 更新節流計時器
    videoStream: null,
    scanAnimationId: null,
    lastRecvUiTime: 0,

    rtcConfig: {
        iceServers: [
            { urls: 'stun:stun.l.google.com:19302' },
            { urls: 'stun:stun1.l.google.com:19302' }
        ]
    }
};

// 🚀【架構優化】：將此設定移至底層 state.js，徹底切斷 webrtc.js ↔ r2.js 的循環依賴鏈
export function getWorkerUrl() {
    return "https://bobconnect.bobkao0527.workers.dev";
}

export default state;