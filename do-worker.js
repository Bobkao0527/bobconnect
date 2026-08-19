// do-worker.js
export class SignalRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.offer = null;
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // 🚀 改用休眠 API：當客戶端連線掛著時，DO 會自動休眠而不消耗 GB-sec
    this.state.acceptWebSocket(server);

    if (this.offer) {
      server.send(JSON.stringify({ type: "offer", offer: this.offer }));
    }

    return new Response(null, { status: 101, webSocket: client });
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