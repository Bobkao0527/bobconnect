// 檔名functions/api/[[path]].js
export async function onRequest(context) {
  const { request, env } = context;

  const url = new URL(request.url);
  const path = url.pathname.replace(/^\/api/, "") || "/";
  const requestOrigin = request.headers.get("Origin");
  const isWebSocket = request.headers.get("Upgrade")?.toLowerCase() === "websocket";

  if ((requestOrigin && requestOrigin !== url.origin) || (isWebSocket && requestOrigin !== url.origin)) {
    return new Response("Forbidden", { status: 403, headers: { "Cache-Control": "no-store" } });
  }

  const corsHeaders = {
    "Access-Control-Allow-Origin": url.origin,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-Client-IPv4",
  };

  if (request.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  if (!env.SIGNAL_ROOM) {
    return Response.json({ error: "SIGNAL_ROOM Durable Object Binding Missing" }, { status: 500, headers: corsHeaders });
  }

  // 統一由 GLOBAL_REGISTRY DO 實例維護全域 WebSocket 記憶體與同 IP 查詢
  const registryId = env.SIGNAL_ROOM.idFromName("GLOBAL_REGISTRY");
  const registryDO = env.SIGNAL_ROOM.get(registryId);

  if (path === "/rooms-by-ip" || path.startsWith("/ws/")) {
    return registryDO.fetch(request);
  }

  return Response.json({
    status: "active",
    message: "Bob WebRTC (Durable Object) Signal Server is running smoothly.",
    architecture: "Pure DO + WebRTC P2P"
  }, { headers: corsHeaders });
}