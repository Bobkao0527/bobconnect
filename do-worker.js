// do-worker.js (免 Storage 費用，純 WebSocket Attachment 記憶體架構)
export class SignalRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/^\/api/, "") || "/";

    if (path === "/rooms-by-ip") {
      return this.handleRegistry(request, path);
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const roomId = path.split("/")[2];
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);

    if (roomId) {
      server.serializeAttachment({ roomId, sessionToken: roomId });

      // 🚀 Joiner / 重連者加入時，比對 roomId 或 sessionToken
      for (const socket of this.state.getWebSockets()) {
        if (socket !== server && socket.readyState === 1) {
          try {
            const meta = socket.deserializeAttachment();
            const isMatch = meta && (meta.roomId === roomId || meta.sessionToken === roomId);
            if (isMatch && meta.offer) {
              server.send(JSON.stringify({
                type: "offer",
                offer: meta.offer,
                roomId: meta.roomId,
                sessionToken: meta.sessionToken
              }));

              // 🚀 補發所有快取在 Attachment 裡的 Candidates
              if (Array.isArray(meta.candidates)) {
                for (const cand of meta.candidates) {
                  server.send(JSON.stringify({
                    type: "candidate",
                    candidate: cand,
                    roomId: roomId
                  }));
                }
              }
              break;
            }
          } catch (e) {}
        }
      }
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  // 🚀 核心：利用 WebSocket Attachment 在休眠喚醒時即時還原活躍房間列表
  async handleRegistry(request, path) {
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, X-Client-IPv4",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    const getIpKey = (req) => {
      const clientIpv4 = req.headers.get("X-Client-IPv4");
      if (clientIpv4) return `ipv4:${clientIpv4}`;
      const clientIp = req.headers.get("CF-Connecting-IP") || "127.0.0.1";
      if (clientIp.includes(":")) {
        const segments = clientIp.split(":");
        return `ipv6:${segments.slice(0, 4).join(":")}`;
      }
      return `ipv4:${clientIp}`;
    };

    const ipKey = getIpKey(request);

    // GET /rooms-by-ip (搜尋同 IP 房間)
    if (request.method === "GET" && path === "/rooms-by-ip") {
      const validRooms = [];
      
      // 遍歷所有『目前仍活著』的 WebSocket 連線
      for (const ws of this.state.getWebSockets()) {
        try {
          if (ws.readyState === 1) {
            const roomMeta = ws.deserializeAttachment();
            if (roomMeta && roomMeta.ipKey === ipKey && roomMeta.roomId) {
              validRooms.push(roomMeta.roomId);
            }
          }
        } catch (e) {
          // 忽略未綁定 attachment 的普通連線
        }
      }
      return Response.json({ rooms: validRooms }, { headers: corsHeaders });
    }
    return new Response("Not Found", { status: 404, headers: corsHeaders });
  }

  // 🚀 Hibernation API 訊息事件處理器
  async webSocketMessage(ws, message) {
    try {
      const data = JSON.parse(message);
      
      if (data.type === "offer") {
        const ipKey = data.ipKey || "ipv4:127.0.0.1";
        const roomId = data.roomId;
        const sessionToken = data.sessionToken || roomId;
        
        if (roomId) {
          const existingMeta = ws.deserializeAttachment() || {};
          const updatedMeta = {
            ...existingMeta,
            roomId: roomId,
            sessionToken: sessionToken,
            ipKey: ipKey,
            offer: data.offer,
            candidates: existingMeta.candidates || [],
            timestamp: Date.now()
          };
          ws.serializeAttachment(updatedMeta);

          // 🚀 廣播 Offer 給對端
          this.broadcast(ws, data);

          // 🚀 若在發送 Offer 前已有快取的 Candidates，一併補發給 Joiner
          if (updatedMeta.candidates.length > 0) {
            for (const cand of updatedMeta.candidates) {
              this.broadcast(ws, {
                type: "candidate",
                candidate: cand,
                roomId: roomId
              });
            }
          }
        }
      } else if (data.type === "candidate") {
        // 🚀 將接收到的 Candidate 寫入該 WebSocket 的 Attachment 快取中
        try {
          const meta = ws.deserializeAttachment() || {};
          if (meta.roomId) {
            meta.candidates = meta.candidates || [];
            meta.candidates.push(data.candidate);
            ws.serializeAttachment(meta);
          }
        } catch (e) {}

        this.broadcast(ws, data);
      } else if (data.type === "answer") {
        this.broadcast(ws, data);
      }
    } catch (e) {
      console.error("[DO Message Error]", e);
    }
  }

  // 🚀 當客戶端斷線時，通知同房間對端
  async webSocketClose(ws, code, reason, wasClean) {
    try {
      const meta = ws.deserializeAttachment();
      if (meta && meta.roomId) {
        this.broadcast(ws, { type: "peer-disconnected", roomId: meta.roomId });
      }
    } catch (e) {}
  }

  broadcast(sender, data) {
    const senderMeta = sender.deserializeAttachment();
    const targetRoomId = data.roomId || (senderMeta ? senderMeta.roomId : null);
    const targetSessionToken = data.sessionToken || (senderMeta ? senderMeta.sessionToken : null);
    const msg = JSON.stringify(data);

    for (const socket of this.state.getWebSockets()) {
      if (socket !== sender && socket.readyState === 1) {
        const socketMeta = socket.deserializeAttachment();
        
        // 🚀 修改：精準匹配 PIN 碼或長金鑰
        const isMatchRoom = targetRoomId && socketMeta && socketMeta.roomId === targetRoomId;
        const isMatchToken = targetSessionToken && socketMeta && socketMeta.sessionToken === targetSessionToken;

        if (isMatchRoom || isMatchToken) {
          socket.send(msg);
        }
      }
    }
  }
}

export default {
  async fetch(request, env) {
    return new Response("BobConnect Signal DO Worker Running");
  }
};