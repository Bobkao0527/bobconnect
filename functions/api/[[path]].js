// 專為 WebRTC 握手與 R2 多分塊（Multipart）大檔案傳輸設計的信令/中轉伺服器 (Pages Functions 版)
// 支援 R2 直連 S3 預簽名金鑰 (Presigned URLs) 與高併發直連機制，並防呆降級至 ArrayBuffer 優化代理模式
// 檔名functions/api/[[path]].js

// 智慧 IP 區段提取器
function getIpKey(request) {
  const clientIpv4 = request.headers.get("X-Client-IPv4");
  if (clientIpv4) {
    return `room:ip:ipv4:${clientIpv4}`;
  }

  const clientIp = request.headers.get("CF-Connecting-IP") || "127.0.0.1";
  if (clientIp.includes(":")) {
    const segments = clientIp.split(":");
    const subnet64 = segments.slice(0, 4).join(":");
    return `room:ip:ipv6:${subnet64}`;
  }
  return `room:ip:ipv4:${clientIp}`;
}

// 🚀 Cloudflare Pages Functions 入口點
export async function onRequest(context) {
  const { request, env } = context;

  const url = new URL(request.url);
  const path = url.pathname.replace(/^\/api/, "") || "/";

  const corsHeaders = {
    "Access-Control-Allow-Origin": "*",
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