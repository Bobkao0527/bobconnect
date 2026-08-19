// do-worker.js
export class SignalRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.offer = null;
    // 🚀【DO 記憶體】用於 GLOBAL_REGISTRY 儲存活躍房間 { offer, ipKey, timestamp }
    this.rooms = new Map();
  }

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/^\/api/, "") || "/";

    // 🚀 1. 如果是區域網 IP 註冊與配對請求，直接由 DO 記憶體處理
    if (path.startsWith("/room/") || path === "/rooms-by-ip") {
      return this.handleRegistry(request, path);
    }

    // 🚀 2. 原有 WebSocket 信令邏輯
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // 改用休眠 API：當客戶端連線掛著時，DO 會自動休眠而不消耗 GB-sec
    this.state.acceptWebSocket(server);

    if (this.offer) {
      server.send(JSON.stringify({ type: "offer", offer: this.offer }));
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  // 🚀【新增】DO 記憶體區域網房間註冊/查詢處理器 (取代 KV)
  async handleRegistry(request, path) {
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
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
    const now = Date.now();

    // 自動清理超過 60 秒的舊房間（記憶體即時清掃）
    for (const [id, room] of this.rooms.entries()) {
      if (now - room.timestamp > 60000) {
        this.rooms.delete(id);
      }
    }

    // POST /room/:roomId (建立/更新房間)
    if (request.method === "POST" && path.startsWith("/room/")) {
      const roomId = path.split("/")[2];
      if (!roomId) return Response.json({ error: "Missing Room ID" }, { status: 400, headers: corsHeaders });
      try {
        const body = await request.json();
        this.rooms.set(roomId, {
          offer: body.offer,
          ipKey: ipKey,
          timestamp: now
        });
        return Response.json({ success: true }, { headers: corsHeaders });
      } catch (e) {
        return Response.json({ error: "Invalid JSON" }, { status: 400, headers: corsHeaders });
      }
    }

    // DELETE /room/:roomId (銷毀房間)
    if (request.method === "DELETE" && path.startsWith("/room/")) {
      const roomId = path.split("/")[2];
      if (roomId) {
        this.rooms.delete(roomId);
      }
      return Response.json({ success: true }, { headers: corsHeaders });
    }

    // GET /rooms-by-ip (搜尋同 IP 房間)
    if (request.method === "GET" && path === "/rooms-by-ip") {
      const validRooms = [];
      for (const [id, room] of this.rooms.entries()) {
        if (room.ipKey === ipKey && room.offer && (now - room.timestamp < 60000)) {
          validRooms.push(id);
        }
      }
      return Response.json({ rooms: validRooms }, { headers: corsHeaders });
    }

    return new Response("Not Found", { status: 404, headers: corsHeaders });
  }

  // 🚀 Hibernation API 專用訊息事件處理器
  async webSocketMessage(ws, message) {
    try {
      const data = JSON.parse(message);
      if (data.type === "offer") {
        this.offer = data.offer;
        this.broadcast(ws, data);
      } else if (data.type === "answer" || data.type === "candidate") {
        this.broadcast(ws, data);
      }
    } catch (e) {
      console.error("[DO Message Error]", e);
    }
  }

  async webSocketClose(ws, code, reason, wasClean) {
    ws.close(code, "Closed");
  }

  broadcast(sender, data) {
    const msg = JSON.stringify(data);
    for (const socket of this.state.getWebSockets()) {
      if (socket !== sender && socket.readyState === 1) {
        socket.send(msg);
      }
    }
  }
}

export default {
  async fetch(request, env) {
    return new Response("BobConnect Signal DO Worker Running");
  }
};