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
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-Client-IPv4",
  };

  if (request.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  // 🚀 1. WebRTC DO 記憶體註冊與同 IP 搜尋 (轉交給 GLOBAL_REGISTRY DO 記憶體處理，不經由 KV)
  if (path.startsWith("/room/") || path === "/rooms-by-ip") {
    if (!env.SIGNAL_ROOM) {
      return Response.json({ error: "SIGNAL_ROOM Durable Object Binding Missing" }, { status: 500, headers: corsHeaders });
    }
    const registryId = env.SIGNAL_ROOM.idFromName("GLOBAL_REGISTRY");
    const registryDO = env.SIGNAL_ROOM.get(registryId);
    return registryDO.fetch(request);
  }

  // 🚀 2. WebSocket DO 信令路由 (`/api/ws/:roomId`)
  if (path.startsWith("/ws/")) {
    const roomId = path.split("/")[2];
    if (!roomId) {
      return new Response("Missing Room ID", { status: 400, headers: corsHeaders });
    }

    if (!env.SIGNAL_ROOM) {
      return new Response(
        JSON.stringify({ error: "SIGNAL_ROOM Durable Object Binding Missing" }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const id = env.SIGNAL_ROOM.idFromName(roomId);
    const roomDO = env.SIGNAL_ROOM.get(id);
    return roomDO.fetch(request);
  }

  const r2 = env.BC_R2;

  const r2AccountId = env.R2_ACCOUNT_ID;
  const r2AccessKeyId = env.R2_ACCESS_KEY_ID;
  const r2SecretAccessKey = env.R2_SECRET_ACCESS_KEY;
  const r2BucketName = env.R2_BUCKET_NAME;

  const isDirectUploadConfigured = !!(r2AccountId && r2AccessKeyId && r2SecretAccessKey && r2BucketName);

  const UPLOAD_LIMIT = 900000;
  globalThis.uploadCount = globalThis.uploadCount || 0;

  const isR2UploadRoute = path.startsWith("/r2-multipart/init/") || 
                           path.startsWith("/r2-multipart/upload/") || 
                           path.startsWith("/r2-multipart/presign/") || 
                           path.startsWith("/r2-multipart/complete/") ||
                           (request.method === "PUT" && path.startsWith("/r2/"));

  if (globalThis.uploadCount >= UPLOAD_LIMIT && isR2UploadRoute) {
    return new Response(
      JSON.stringify({
        error: "R2_LIMIT_EXCEEDED",
        message: "R2 上傳額度已達上限，系統已自動關閉 R2 功能。"
      }),
      { status: 403, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }

  // --- R2 相關路由 (改為記憶體鎖，完全移除非必要的 KV 呼叫) ---
  if (request.method === "POST" && path.startsWith("/r2-multipart/init/")) {
    if (!r2) {
      return Response.json({ error: "R2 綁定遺失" }, { status: 500, headers: corsHeaders });
    }
    const parts = path.split("/");
    const roomId = parts[3];
    const filename = decodeURIComponent(parts.slice(4).join("/"));
    const key = `r2:${roomId}:${filename}`;

    const now = Date.now();
    if (globalThis.currentUploadingRoom && globalThis.currentUploadingRoom.roomId !== roomId) {
      if (now - globalThis.currentUploadingRoom.timestamp < 10 * 60 * 1000) {
        return Response.json({
          error: "CONCURRENCY_LOCKED",
          message: "系統忙碌中：目前已有其他大檔案正在寫入雲端，請稍候。"
        }, { status: 409, headers: corsHeaders });
      }
    }
    globalThis.currentUploadingRoom = { roomId, timestamp: now };

    try {
      const multipart = await r2.createMultipartUpload(key, {
        httpMetadata: { contentType: "application/octet-stream" }
      });
      
      return Response.json({ 
        uploadId: multipart.uploadId,
        directUpload: isDirectUploadConfigured
      }, { headers: corsHeaders });
    } catch (e) {
      globalThis.currentUploadingRoom = null;
      return Response.json({ error: e.message }, { status: 500, headers: corsHeaders });
    }
  }

  if (request.method === "GET" && path.startsWith("/r2-multipart/presign/")) {
    if (!isDirectUploadConfigured) {
      return Response.json({ error: "未設定 R2 S3 直連環境變數" }, { status: 400, headers: corsHeaders });
    }
    const parts = path.split("/");
    const roomId = parts[3];
    const filename = decodeURIComponent(parts.slice(4).join("/"));
    const key = `r2:${roomId}:${filename}`;

    const uploadId = url.searchParams.get("uploadId");
    const partNumber = parseInt(url.searchParams.get("partNumber"));

    if (!uploadId || !partNumber) {
      return Response.json({ error: "缺少參數" }, { status: 400, headers: corsHeaders });
    }

    try {
      const uploadUrl = await generatePresignedUrl({
        accountId: r2AccountId,
        accessKeyId: r2AccessKeyId,
        secretAccessKey: r2SecretAccessKey,
        bucket: r2BucketName,
        key: key,
        uploadId: uploadId,
        partNumber: partNumber
      });

      return Response.json({ uploadUrl }, { headers: corsHeaders });
    } catch (e) {
      return Response.json({ error: e.message }, { status: 500, headers: corsHeaders });
    }
  }

  if (request.method === "PUT" && path.startsWith("/r2-multipart/upload/")) {
    if (!r2) {
      return Response.json({ error: "R2 綁定遺失" }, { status: 500, headers: corsHeaders });
    }
    const parts = path.split("/");
    const roomId = parts[3];
    const filename = decodeURIComponent(parts.slice(4).join("/"));
    const key = `r2:${roomId}:${filename}`;

    const uploadId = url.searchParams.get("uploadId");
    const partNumber = parseInt(url.searchParams.get("partNumber"));

    if (!uploadId || !partNumber) {
      return Response.json({ error: "缺少參數" }, { status: 400, headers: corsHeaders });
    }

    try {
      const multipart = r2.resumeMultipartUpload(key, uploadId);
      const buffer = await request.arrayBuffer();
      const part = await multipart.uploadPart(partNumber, buffer);

      return Response.json({ partNumber: part.partNumber, etag: part.etag }, { headers: corsHeaders });
    } catch (e) {
      return Response.json({ error: e.message }, { status: 500, headers: corsHeaders });
    }
  }

  if (request.method === "POST" && path.startsWith("/r2-multipart/complete/")) {
    if (!r2) {
      return Response.json({ error: "R2 綁定遺失" }, { status: 500, headers: corsHeaders });
    }
    const parts = path.split("/");
    const roomId = parts[3];
    const filename = decodeURIComponent(parts.slice(4).join("/"));
    const key = `r2:${roomId}:${filename}`;

    const uploadId = url.searchParams.get("uploadId");
    if (!uploadId) {
      return Response.json({ error: "缺少 uploadId" }, { status: 400, headers: corsHeaders });
    }

    try {
      const uploadedParts = await request.json(); 
      const multipart = r2.resumeMultipartUpload(key, uploadId);
      
      await multipart.complete(uploadedParts);

      if (globalThis.currentUploadingRoom && globalThis.currentUploadingRoom.roomId === roomId) {
        globalThis.currentUploadingRoom = null;
      }
      globalThis.uploadCount = (globalThis.uploadCount || 0) + uploadedParts.length;

      return Response.json({ success: true, message: "檔案合併完成" }, { headers: corsHeaders });
    } catch (e) {
      return Response.json({ error: e.message }, { status: 500, headers: corsHeaders });
    }
  }

  if (request.method === "GET" && path.startsWith("/r2/")) {
    if (!r2) {
      return new Response("R2 Storage is not configured", { status: 500, headers: corsHeaders });
    }
    const parts = path.split("/");
    const roomId = parts[2];
    const filename = decodeURIComponent(parts.slice(3).join("/"));
    const key = `r2:${roomId}:${filename}`;

    try {
      const object = await r2.get(key);
      if (!object) {
        return new Response("File not found or expired", { status: 404, headers: corsHeaders });
      }

      const headers = new Headers(corsHeaders);
      headers.set("Content-Type", "application/octet-stream");
      headers.set("Content-Disposition", `attachment; filename*=UTF-8''${encodeURIComponent(filename)}`);
      
      return new Response(object.body, { headers });
    } catch (e) {
      return new Response("Error streaming file: " + e.message, { status: 500, headers: corsHeaders });
    }
  }

  if (request.method === "DELETE" && path.startsWith("/r2/")) {
    const parts = path.split("/");
    const roomId = parts[2];
    const filename = decodeURIComponent(parts.slice(3).join("/"));
    const key = `r2:${roomId}:${filename}`;

    if (r2) {
      await r2.delete(key);
    }

    if (globalThis.currentUploadingRoom && globalThis.currentUploadingRoom.roomId === roomId) {
      globalThis.currentUploadingRoom = null;
    }

    return Response.json({ success: true, message: "R2 檔案已抹除，併行鎖已釋放" }, { headers: corsHeaders });
  }

  return Response.json({
    status: "active",
    message: "Bob WebRTC (Durable Object) & R2 Multi-part Server is running smoothly.",
    kv: "REMOVED (In-Memory DO Storage Active)",
    r2: r2 ? "CONNECTED" : "OFFLINE",
    directUpload: isDirectUploadConfigured,
    uploadUsage: `${globalThis.uploadCount || 0} / ${UPLOAD_LIMIT}`
  }, { headers: corsHeaders });
}

// SHA-256 / HMAC 輔助函式與 R2 S3 預簽名 URL 生成器（保持原樣）
async function hmac(key, data) {
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    typeof key === "string" ? new TextEncoder().encode(key) : key,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signature = await crypto.subtle.sign(
    "HMAC",
    cryptoKey,
    typeof data === "string" ? new TextEncoder().encode(data) : data
  );
  return new Uint8Array(signature);
}

async function sha256(data) {
  const hash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(data));
  return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, "0")).join("");
}

function encodeCanonicalUri(path) {
  return path.split('/').map((segment, idx) => {
    if (idx === 0) return '';
    return encodeURIComponent(segment).replace(/%2F/g, '/');
  }).join('/');
}

async function generatePresignedUrl({
  accountId,
  accessKeyId,
  secretAccessKey,
  bucket,
  key,
  uploadId,
  partNumber,
  expiresIn = 3600
}) {
  const region = "auto";
  const host = `${bucket}.${accountId}.r2.cloudflarestorage.com`;
  const urlPath = encodeCanonicalUri(`/${key}`);
  
  const amzDate = new Date().toISOString().replace(/[:-]/g, "").split(".")[0] + "Z";
  const dateStamp = amzDate.substr(0, 8);
  const credentialScope = `${dateStamp}/${region}/s3/aws4_request`;
  
  const queryParams = {
    "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
    "X-Amz-Credential": `${accessKeyId}/${credentialScope}`,
    "X-Amz-Date": amzDate,
    "X-Amz-Expires": expiresIn.toString(),
    "X-Amz-SignedHeaders": "host",
    "X-Amz-Content-Sha256": "UNSIGNED-PAYLOAD",
    "partNumber": partNumber.toString(),
    "uploadId": uploadId
  };
  
  const canonicalQueryString = Object.keys(queryParams)
    .sort()
    .map(q => `${encodeURIComponent(q)}=${encodeURIComponent(queryParams[q])}`)
    .join("&");
    
  const canonicalHeaders = `host:${host}\n`;
  const signedHeaders = "host";
  const payloadHash = "UNSIGNED-PAYLOAD";
  
  const canonicalRequest = [
    "PUT",
    urlPath,
    canonicalQueryString,
    canonicalHeaders,
    signedHeaders,
    payloadHash
  ].join("\n");
  
  const canonicalRequestHash = await sha256(canonicalRequest);
  
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    amzDate,
    credentialScope,
    canonicalRequestHash
  ].join("\n");
  
  const kDate = await hmac("AWS4" + secretAccessKey, dateStamp);
  const kRegion = await hmac(kDate, region);
  const kService = await hmac(kRegion, "s3");
  const kSigning = await hmac(kService, "aws4_request");
  
  const signatureBytes = await hmac(kSigning, stringToSign);
  const signature = Array.from(signatureBytes).map(b => b.toString(16).padStart(2, "0")).join("");
  
  return `https://${host}${urlPath}?${canonicalQueryString}&X-Amz-Signature=${signature}`;
}