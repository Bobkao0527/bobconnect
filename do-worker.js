// do-worker.js (免 Storage 費用，純 WebSocket Attachment 記憶體架構)
const MAX_TOTAL_CONNECTIONS = 500;
const MAX_CONNECTIONS_PER_IP = 20;
const MAX_CONNECTIONS_PER_ROOM = 3;
// 放寬至 60：ICE candidate 初次湧入 (多 STUN × 多網卡 × IPv4/IPv6) 可能瞬間超過 30 則而被誤關
const MAX_MESSAGES_PER_SECOND = 60;
const MAX_MESSAGE_BYTES = 256 * 1024;
const MAX_CANDIDATES_PER_CONNECTION = 200;
const ROOM_LOOKUP_LIMIT = 30;
const ROOM_LOOKUP_WINDOW_MS = 60 * 1000;
// 放寬至 40：同區網兩端共用公網 IP，斷線重連時雙方各算一次，10 次/分鐘極易觸發 429 使重連卡死
const CONNECTION_ATTEMPT_LIMIT = 40;
const CONNECTION_ATTEMPT_WINDOW_MS = 60 * 1000;

export class SignalRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.rateLimits = new Map();

    // 🚀 WebSocket 保活：前端每 30 秒送 "ping"，由邊緣直接回 "pong"，不會喚醒 DO (避免 Cloudflare 100 秒閒置斷線)
    try {
      this.state.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    } catch (e) {
      console.warn("[DO] setWebSocketAutoResponse 不可用，改由 webSocketMessage 回應 ping", e);
    }
  }

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/^\/api/, "") || "/";
    const requestOrigin = request.headers.get("Origin");
    const isWebSocket = request.headers.get("Upgrade")?.toLowerCase() === "websocket";

    if ((requestOrigin && requestOrigin !== url.origin) || (isWebSocket && requestOrigin !== url.origin)) {
      return new Response("Forbidden", { status: 403, headers: { "Cache-Control": "no-store" } });
    }

    if (path === "/rooms-by-ip") {
      return this.handleRegistry(request, path);
    }

    if (!isWebSocket) {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const roomId = path.startsWith("/ws/") ? path.slice(4) : null;
    if (!roomId) {
      return new Response("Room ID Required", { status: 400 });
    }

    const ipKey = this.getClientIp(request);
    const discoveryKey = this.getDiscoveryKey(request);
    if (this.countActiveSockets() >= MAX_TOTAL_CONNECTIONS) {
      return new Response("Too Many Connections", { status: 429 });
    }
    if (this.countSocketsByIp(ipKey) >= MAX_CONNECTIONS_PER_IP) {
      return new Response("Too Many Connections From This IP", { status: 429 });
    }
    if (!this.consumeRateLimit(`connect:${ipKey}`, CONNECTION_ATTEMPT_LIMIT, CONNECTION_ATTEMPT_WINDOW_MS)) {
      return new Response("Connection Rate Limit Exceeded", { status: 429 });
    }
    if (this.countSocketsByRoom(roomId) >= MAX_CONNECTIONS_PER_ROOM) {
      return new Response("Room Is Full", { status: 409 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);

    if (roomId) {
      server.serializeAttachment({ roomId, sessionToken: roomId, ipKey, discoveryKey });

      // 🚀 Joiner / 重連者加入時，比對 roomId 或 sessionToken
      for (const socket of this.state.getWebSockets()) {
        if (socket !== server && socket.readyState === 1) {
          try {
            const meta = socket.deserializeAttachment();
            const isMatch = meta && (meta.roomId === roomId || meta.sessionToken === roomId);
            if (isMatch && meta.offer && !meta.offerConsumed) {
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
    const requestOrigin = new URL(request.url).origin;
    const corsHeaders = {
      "Access-Control-Allow-Origin": requestOrigin,
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, X-Client-IPv4",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    const ipKey = this.getClientIp(request);
    const discoveryKey = this.getDiscoveryKey(request);

    // GET /rooms-by-ip (搜尋同 IP 房間)
    if (request.method === "GET" && path === "/rooms-by-ip") {
      if (!this.consumeRateLimit(`lookup:${ipKey}`, ROOM_LOOKUP_LIMIT, ROOM_LOOKUP_WINDOW_MS)) {
        return Response.json({ error: "Rate limit exceeded" }, { status: 429, headers: corsHeaders });
      }

      const validRooms = [];
      
      // 遍歷所有『目前仍活著』的 WebSocket 連線
      for (const ws of this.state.getWebSockets()) {
        try {
          if (ws.readyState === 1) {
            const roomMeta = ws.deserializeAttachment();
            if (roomMeta && roomMeta.discoveryKey === discoveryKey && roomMeta.roomId) {
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
      const messageText = typeof message === "string"
        ? message
        : new TextDecoder().decode(message);
      if (new TextEncoder().encode(messageText).byteLength > MAX_MESSAGE_BYTES) {
        ws.close(1009, "Message Too Large");
        return;
      }

      // 保活 ping 的保底處理 (auto-response 不可用時)
      if (messageText === "ping") {
        ws.send("pong");
        return;
      }

      const meta = ws.deserializeAttachment() || {};
      const now = Date.now();
      const messageWindowStart = meta.messageWindowStart || now;
      const messageCount = now - messageWindowStart >= 1000 ? 1 : (meta.messageCount || 0) + 1;
      const updatedRateMeta = now - messageWindowStart >= 1000
        ? { ...meta, messageWindowStart: now, messageCount }
        : { ...meta, messageWindowStart, messageCount };
      if (messageCount > MAX_MESSAGES_PER_SECOND) {
        ws.serializeAttachment(updatedRateMeta);
        ws.close(1008, "Message Rate Limit Exceeded");
        return;
      }
      ws.serializeAttachment(updatedRateMeta);

      const data = JSON.parse(messageText);
      
      if (data.type === "offer") {
        const ipKey = updatedRateMeta.ipKey;
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
            // 標記此 Offer 尚未被消費；收到對應 Answer 後會標為已消費，避免重連 Joiner 拿到過期描述
            offerConsumed: false,
            // 新 offer 代表新一代 ICE，舊 candidates 已失效；清空也避免長連線累積到上限被關閉
            candidates: [],
            timestamp: Date.now()
          };
          ws.serializeAttachment(updatedMeta);

          // 🚀 關閉同一 session 中已被取代的舊 Host socket (殘留的舊 offer 會讓重連的 Joiner 拿到過期描述而握手失敗)
          for (const socket of this.state.getWebSockets()) {
            if (socket === ws || socket.readyState !== 1) continue;
            try {
              const otherMeta = socket.deserializeAttachment();
              const isSameSession = otherMeta && otherMeta.offer &&
                (otherMeta.sessionToken === sessionToken || otherMeta.roomId === roomId);
              if (isSameSession) {
                socket.close(1000, "Superseded");
              }
            } catch (e) {}
          }

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
            if (meta.candidates.length >= MAX_CANDIDATES_PER_CONNECTION) {
              ws.close(1008, "Too Many ICE Candidates");
              return;
            }
            meta.candidates.push(data.candidate);
            ws.serializeAttachment(meta);
          }
        } catch (e) {}

        this.broadcast(ws, data);
      } else if (data.type === "answer") {
        // 補齊 Joiner 端 socket 的 roomId / sessionToken，確保後續雙向重連廣播可精準命中
        const targetRoomId = data.roomId || updatedRateMeta.roomId;
        const targetSessionToken = data.sessionToken || updatedRateMeta.sessionToken;
        ws.serializeAttachment({
          ...updatedRateMeta,
          ...(targetRoomId ? { roomId: targetRoomId } : {}),
          ...(targetSessionToken ? { sessionToken: targetSessionToken } : {})
        });

        // 將同會話中 Host 的 Offer 標記為已消費並清空暫存 candidates，防止重連時補發過期舊 Offer
        for (const socket of this.state.getWebSockets()) {
          if (socket === ws || socket.readyState !== 1) continue;
          try {
            const otherMeta = socket.deserializeAttachment();
            const isSameSession = otherMeta && otherMeta.offer &&
              ((targetRoomId && otherMeta.roomId === targetRoomId) ||
                (targetSessionToken && otherMeta.sessionToken === targetSessionToken));
            if (isSameSession) {
              socket.serializeAttachment({
                ...otherMeta,
                offerConsumed: true,
                candidates: []
              });
            }
          } catch (e) {}
        }

        this.broadcast(ws, data);
      } else if (data.type === "request-offer") {
        // Joiner 先重連上線時主動請求 Host 產生新 Offer
        const targetRoomId = data.roomId || updatedRateMeta.roomId;
        const targetSessionToken = data.sessionToken || updatedRateMeta.sessionToken;
        if (targetRoomId || targetSessionToken) {
          ws.serializeAttachment({
            ...updatedRateMeta,
            ...(targetRoomId ? { roomId: targetRoomId } : {}),
            ...(targetSessionToken ? { sessionToken: targetSessionToken } : {})
          });
        }
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

  getClientIp(request) {
    const clientIp = request.headers.get("CF-Connecting-IP") || "unknown";
    if (clientIp.includes(":")) {
      const segments = clientIp.split(":");
      return `ipv6:${segments.slice(0, 4).join(":")}`;
    }
    return `ipv4:${clientIp}`;
  }

  getDiscoveryKey(request) {
    const clientIpv4 = request.headers.get("X-Client-IPv4");
    if (/^\d{1,3}(\.\d{1,3}){3}$/.test(clientIpv4 || "")) {
      return `ipv4:${clientIpv4}`;
    }
    return this.getClientIp(request);
  }

  countActiveSockets() {
    return this.state.getWebSockets().filter((socket) => socket.readyState === 1).length;
  }

  countSocketsByIp(ipKey) {
    return this.state.getWebSockets().filter((socket) => {
      if (socket.readyState !== 1) return false;
      try {
        return socket.deserializeAttachment()?.ipKey === ipKey;
      } catch (e) {
        return false;
      }
    }).length;
  }

  countSocketsByRoom(roomId) {
    return this.state.getWebSockets().filter((socket) => {
      if (socket.readyState !== 1) return false;
      try {
        return socket.deserializeAttachment()?.roomId === roomId;
      } catch (e) {
        return false;
      }
    }).length;
  }

  consumeRateLimit(key, limit, windowMs) {
    const now = Date.now();
    const current = this.rateLimits.get(key);
    if (!current || now - current.startedAt >= windowMs) {
      this.rateLimits.set(key, { startedAt: now, count: 1 });
      return true;
    }
    if (current.count >= limit) return false;
    current.count += 1;
    return true;
  }
}

export default {
  async fetch() {
    return new Response("Not Found", { status: 404 });
  }
};