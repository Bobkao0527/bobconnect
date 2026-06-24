// 這是專為 WebRTC 握手與 R2 多分塊（Multipart）大檔案傳輸設計的信令/中轉伺服器
// 支援 R2 直連 S3 預簽名金鑰 (Presigned URLs) 與高併發直連機制，並防呆降級至 ArrayBuffer 優化代理模式

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    globalThis.rooms = globalThis.rooms || new Map();

    const kv = env.BOB_CONNECT_KV;
    const r2 = env.BOB_CONNECT_R2;

    // 取得 S3 預簽名所需之環境變數（若無配置則自動無縫降級至記憶體優化代理模式）
    const r2AccountId = env.R2_ACCOUNT_ID;
    const r2AccessKeyId = env.R2_ACCESS_KEY_ID;
    const r2SecretAccessKey = env.R2_SECRET_ACCESS_KEY;
    const r2BucketName = env.R2_BUCKET_NAME;

    const isDirectUploadConfigured = !!(r2AccountId && r2AccessKeyId && r2SecretAccessKey && r2BucketName);

    // 🛡️ 1. 荷包安全閥門
    const UPLOAD_LIMIT = 900000;
    let currentUploadCount = 0;

    if (kv) {
      const countVal = await kv.get("system:upload_count");
      currentUploadCount = countVal ? parseInt(countVal, 10) : 0;
    }

    const isR2UploadRoute = path.startsWith("/r2-multipart/init/") || 
                             path.startsWith("/r2-multipart/upload/") || 
                             path.startsWith("/r2-multipart/presign/") || 
                             path.startsWith("/r2-multipart/complete/") ||
                             (request.method === "PUT" && path.startsWith("/r2/"));

    if (currentUploadCount >= UPLOAD_LIMIT && isR2UploadRoute) {
      return new Response(
        JSON.stringify({
          error: "R2_LIMIT_EXCEEDED",
          message: "R2 上傳額度已達上限，系統已自動關閉 R2 功能。"
        }),
        { status: 403, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // --- R2.1: 初始化多分塊上傳 (POST /r2-multipart/init/:roomId/:filename) ---
    if (request.method === "POST" && path.startsWith("/r2-multipart/init/")) {
      if (!r2) {
        return Response.json({ error: "R2 綁定遺失" }, { status: 500, headers: corsHeaders });
      }
      const parts = path.split("/");
      const roomId = parts[3];
      const filename = decodeURIComponent(parts.slice(4).join("/"));
      const key = `r2:${roomId}:${filename}`;

      // 🛡️ 併行鎖機制
      if (kv) {
        const currentLock = await kv.get("system:current_uploading_room");
        if (currentLock && currentLock !== roomId) {
          return Response.json({
            error: "CONCURRENCY_LOCKED",
            message: "系統忙碌中：目前已有其他大檔案正在寫入雲端，請稍候。"
          }, { status: 409, headers: corsHeaders });
        }
        await kv.put("system:current_uploading_room", roomId, { expirationTtl: 600 });
      } else {
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
      }

      try {
        const multipart = await r2.createMultipartUpload(key, {
          httpMetadata: { contentType: "application/octet-stream" }
        });
        
        // 告知前端目前是否支援「直連 R2」模式
        return Response.json({ 
          uploadId: multipart.uploadId,
          directUpload: isDirectUploadConfigured
        }, { headers: corsHeaders });
      } catch (e) {
        if (kv) {
          await kv.delete("system:current_uploading_room");
        } else {
          globalThis.currentUploadingRoom = null;
        }
        return Response.json({ error: e.message }, { status: 500, headers: corsHeaders });
      }
    }

    // --- 🚀 R2.1.5: 產生分塊預簽名直連 URL (GET /r2-multipart/presign/:roomId/:filename) ---
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
        // 純 JS 輕量化 S3 V4 簽名邏輯，實現 0 外部依賴生成直連連結
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

    // --- R2.2: 降級備用：接收單個分塊代理 (PUT /r2-multipart/upload/:roomId/:filename) ---
    // 🚀【極速代理優化】：改用 ArrayBuffer 載入記憶體，徹底解決 Stream 造成 Edge V8 引擎調度阻塞的問題！
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
        // 核心改動：將 request.body 轉為 ArrayBuffer 後寫入，效率飆升
        const buffer = await request.arrayBuffer();
        const part = await multipart.uploadPart(partNumber, buffer);

        return Response.json({ partNumber: part.partNumber, etag: part.etag }, { headers: corsHeaders });
      } catch (e) {
        return Response.json({ error: e.message }, { status: 500, headers: corsHeaders });
      }
    }

    // --- R2.3: 完成多分塊上傳並合併 (POST /r2-multipart/complete/:roomId/:filename) ---
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

        if (kv) {
          const currentLock = await kv.get("system:current_uploading_room");
          if (currentLock === roomId) {
            await kv.delete("system:current_uploading_room");
          }
          
          const freshCount = (await kv.get("system:upload_count").then(v => v ? parseInt(v, 10) : 0)) + uploadedParts.length;
          await kv.put("system:upload_count", freshCount.toString());
        } else {
          if (globalThis.currentUploadingRoom && globalThis.currentUploadingRoom.roomId === roomId) {
            globalThis.currentUploadingRoom = null;
          }
        }

        return Response.json({ success: true, message: "檔案合併完成" }, { headers: corsHeaders });
      } catch (e) {
        return Response.json({ error: e.message }, { status: 500, headers: corsHeaders });
      }
    }

    // --- R2.4: 下載檔案 (GET /r2/:roomId/:filename) ---
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

    // --- R2.5: DELETE /r2/:roomId/:filename (銷毀 R2 託管大檔案) ---
    if (request.method === "DELETE" && path.startsWith("/r2/")) {
      if (r2) {
        const parts = path.split("/");
        const roomId = parts[2];
        const filename = decodeURIComponent(parts.slice(3).join("/"));
        const key = `r2:${roomId}:${filename}`;
        await r2.delete(key);
      }

      if (kv) {
        const currentLock = await kv.get("system:current_uploading_room");
        if (currentLock === roomId) {
          await kv.delete("system:current_uploading_room");
        }
      } else {
        if (globalThis.currentUploadingRoom && globalThis.currentUploadingRoom.roomId === roomId) {
          globalThis.currentUploadingRoom = null;
        }
      }

      return Response.json({ success: true, message: "R2 檔案已抹除，併行鎖已釋放" }, { headers: corsHeaders });
    }

    // --- WebRTC 1: POST /room/:roomId (交換 Offer/Answer) ---
    if (request.method === "POST" && path.startsWith("/room/")) {
      const roomId = path.split("/")[2];
      if (!roomId) return Response.json({ error: "Missing Room ID" }, { status: 400, headers: corsHeaders });

      try {
        const body = await request.json();
        const now = Date.now();

        if (kv) {
          const existingRaw = await kv.get(`room:${roomId}`);
          const currentData = existingRaw ? JSON.parse(existingRaw) : {};
          const updatedData = { ...currentData, ...body, timestamp: now };
          await kv.put(`room:${roomId}`, JSON.stringify(updatedData), { expirationTtl: 600 });
        } else {
          const currentData = globalThis.rooms.get(roomId) || {};
          globalThis.rooms.set(roomId, { ...currentData, ...body, timestamp: now });
        }
        return Response.json({ success: true }, { headers: corsHeaders });
      } catch (e) {
        return Response.json({ error: "Invalid JSON" }, { status: 400, headers: corsHeaders });
      }
    }

    // --- WebRTC 2: GET /room/:roomId (取得信令資料) ---
    if (request.method === "GET" && path.startsWith("/room/")) {
      const roomId = path.split("/")[2];
      let data = null;
      if (kv) {
        const rawData = await kv.get(`room:${roomId}`);
        data = rawData ? JSON.parse(rawData) : null;
      } else {
        data = globalThis.rooms.get(roomId) || null;
      }
      return Response.json(data, { headers: corsHeaders });
    }

    // --- WebRTC 3: DELETE /room/:roomId (銷毀連線房間) ---
    if (request.method === "DELETE" && path.startsWith("/room/")) {
      const roomId = path.split("/")[2];
      if (kv) {
        await kv.delete(`room:${roomId}`);
      } else {
        globalThis.rooms.delete(roomId);
      }
      return Response.json({ success: true }, { headers: corsHeaders });
    }

    return new Response(
      `Bob WebRTC & R2 Multi-part Server is active.\nKV: ${kv ? "ON" : "OFF"}\nR2: ${r2 ? "ON" : "OFF"}\nDirect Upload Support: ${isDirectUploadConfigured ? "ENABLED (Presigned)" : "DISABLED (Proxy-only)"}\nUpload Operations Checked: ${currentUploadCount} / ${UPLOAD_LIMIT}`, 
      { headers: { ...corsHeaders, "Content-Type": "text/plain" } }
    );
  }
};

// --- 🔒 Web Crypto API HMAC-SHA256 簽名輔助函式 ---
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

// --- 🌐 R2 S3 預簽名 URL 生成器 ---
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
  
  // 參數排序，S3 要求排序必須完全一致
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
  
  // 簽名推導
  const kDate = await hmac("AWS4" + secretAccessKey, dateStamp);
  const kRegion = await hmac(kDate, region);
  const kService = await hmac(kRegion, "s3");
  const kSigning = await hmac(kService, "aws4_request");
  
  const signatureBytes = await hmac(kSigning, stringToSign);
  const signature = Array.from(signatureBytes).map(b => b.toString(16).padStart(2, "0")).join("");
  
  return `https://${host}${urlPath}?${canonicalQueryString}&X-Amz-Signature=${signature}`;
}