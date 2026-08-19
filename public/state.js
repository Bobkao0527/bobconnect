export const state = {
    peerConnection: null,
    dataChannel: null,
    selectedFile: null,
    receiveBuffer: [],
    receivedSize: 0,
    incomingFileInfo: null,

    opfsWritable: null,
    opfsFileHandle: null,
    
    roomId: null,
    isHost: false,

    ws: null,
    pendingOffer: null,

    // 局域網多端偵測過濾儲存槽
    nearbyRooms: [],
    
    // 前端探針取得之真實公網 IPv4
    localIPv4: null,

    // 設備特徵識別
    localIsMobile: /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent),
    remoteIsMobile: false,

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

export function getWorkerUrl() {
    return `${window.location.origin}/api`;
}

export default state;