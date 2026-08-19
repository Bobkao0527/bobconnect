// do-worker.js
export class SignalRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sockets = new Set();
    this.offer = null;
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    server.accept();
    this.sockets.add(server);

    // 若先前已發送過 Offer，Joiner 連上線時立刻推播
    if (this.offer) {
      server.send(JSON.stringify({ type: "offer", offer: this.offer }));
    }

    server.addEventListener("message", (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.type === "offer") {
          this.offer = data.offer;
          this.broadcast(server, data);
        } else if (data.type === "answer" || data.type === "candidate") {
          this.broadcast(server, data);
        }
      } catch (e) {
        console.error("[DO Message Error]", e);
      }
    });

    const cleanup = () => {
      this.sockets.delete(server);
      if (this.sockets.size === 0) {
        this.offer = null;
      }
    };

    server.addEventListener("close", cleanup);
    server.addEventListener("error", cleanup);

    return new Response(null, { status: 101, webSocket: client });
  }

  broadcast(sender, data) {
    const msg = JSON.stringify(data);
    for (const socket of this.sockets) {
      if (socket !== sender && socket.readyState === 1) { // 1 = OPEN
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