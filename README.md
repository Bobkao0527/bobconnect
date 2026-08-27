# BobConnect

> [English Version](README.en.md)

BobConnect 是一個以 WebRTC 為核心的瀏覽器點對點檔案傳輸工具。檔案內容會直接在兩個瀏覽器之間傳送，Cloudflare Durable Object 只負責配對與 WebRTC signaling，不會中轉檔案內容。

## AI 協作者

- GPT 5.6 Luna
- GPT 5.3
- Gemini 3.7 flash
- Gemini 3.6 flash
- Ox-Alpha(GLM 5.3 flash)

## 功能

- QR Code、六位數 PIN 配對
- 同公網 IPv4 的附近裝置偵測
- WebRTC DataChannel 加密點對點傳輸
- 大檔案分塊、流量控制與斷線續傳
- OPFS 暫存接收檔案，降低瀏覽器記憶體壓力
- 支援行動裝置相機掃描與 Wake Lock
- Durable Object signaling 連線與訊息限制
- Content Security Policy 與同源 API 保護

## 架構

```text
Browser A                         Browser B
    |                                 |
    |------ WebRTC DataChannel -------|
    |                                 |
    +---------- Pages frontend -------+
                    |
              /api WebSocket
                    |
             Pages Function
                    |
          SIGNAL_ROOM binding
                    |
          Cloudflare Worker
                    |
        Durable Object SignalRoom
```

- `public/`：前端 HTML、CSS、JavaScript 與 OPFS Worker
- `functions/api/[[path]].js`：Cloudflare Pages Function API 入口
- `do-worker.js`：獨立部署的 Cloudflare Worker 與 Durable Object class
- `makedo.toml`：Durable Object Worker 的 Wrangler 設定

前端使用目前頁面的相對 `/api` 路徑，因此每個部署都會連到該部署自己的 Pages Function 與 Durable Object。

## 部署需求

- Cloudflare 帳號
- Node.js 與 npm
- Wrangler CLI
- 一個 Cloudflare Pages 專案
- Pages Function 的 `SIGNAL_ROOM` Durable Object binding

不需要把 Cloudflare API Token、R2 secret、密碼或其他憑證放進原始碼。檔案傳輸目前不使用 R2。

## 部署自己的版本

### 1. 取得原始碼

```bash
git clone <your-repository-url>
cd bobconnect
```

### 2. 安裝並登入 Wrangler

```bash
npm install -g wrangler
wrangler login
```

也可以不安裝全域 CLI，改用：

```bash
npx wrangler login
```

### 3. 部署 Durable Object Worker

```bash
npx wrangler deploy -c makedo.toml
```

這會建立或更新：

- Worker：`bobconnect-do`
- Durable Object class：`SignalRoom`
- Durable Object binding：`SIGNAL_ROOM`

第一次部署若遇到 Durable Object migration 問題，請確認 `makedo.toml` 的 migration 已套用，且 Cloudflare 帳號與部署環境一致。

### 4. 設定 Pages 的 Durable Object binding

在 Cloudflare Dashboard 開啟自己的 Pages 專案：

1. 進入 **Settings**。
2. 開啟 **Functions** 或 **Bindings** 設定。
3. 新增 Durable Object binding。
4. Binding name 設為 `SIGNAL_ROOM`。
5. 指向自己部署的 `SignalRoom` Durable Object namespace／Worker。
6. 儲存設定，並確認 Production 環境已設定，不只是 Preview 環境。

Binding 名稱必須完全是 `SIGNAL_ROOM`，因為 Pages Function 會讀取 `env.SIGNAL_ROOM`。

### 5. 部署 Pages

在 Pages 專案的 Git 設定中：

- **Framework preset**：選擇 None 或 Static HTML
- **Build command**：`npm run build`
- **Build output directory**：`public`

repository 已包含建置後的 `public/tailwind.css`，因此即使直接提供 `public/` 也能正常顯示。Pages 仍會先安裝 `package.json` 中的建置依賴，再執行 Tailwind CSS 建置；如果使用自訂 CI，請確保部署前已執行：

```bash
npm install
npm run build
```

將 repository 連到 Pages 後執行部署。Pages Function 會從 repository 根目錄的 `functions/` 自動部署，前端則從 `public/` 提供。

如果使用 Wrangler 或其他 CI 部署 Pages，請依 Cloudflare Pages 的部署方式設定；重點是必須同時包含 `public/` 與 `functions/`。

### 6. 測試

部署完成後，用兩個不同的瀏覽器或裝置開啟自己的 Pages 網址，依序測試：

1. QR Code 配對。
2. 六位數 PIN 配對。
3. 同公網 IPv4 的附近裝置偵測。
4. 小檔案傳輸。
5. 較大檔案傳輸。
6. 中斷網路後重新連線與續傳。
7. 行動裝置相機掃描與檔案下載。

瀏覽器 Console 若出現以下訊息，可依此判斷：

- `403`：通常是來源檢查或部署入口設定問題。
- `409`：指定房間已達連線上限。
- `429`：連線、查詢或 signaling 訊息超過限制。
- `Refused to connect`：CSP 的 `connect-src` 或網域設定問題。
- `/api` 回傳 500：通常是 Pages 的 `SIGNAL_ROOM` binding 未設定或名稱錯誤。

## 安全與公開 repository 注意事項

以下檔案與資料不可提交：

```text
.env
.env.*
.dev.vars
.wrangler/
Cloudflare API token
R2 access key / secret key
私鑰與服務帳號檔案
```

`.gitignore` 已排除常見的本機環境與 Wrangler 檔案，但仍應在公開前檢查 Git 歷史是否曾經提交過祕密。

正式服務網址不需要寫進前端。前端使用同源 `/api`，其他使用者 fork 後應部署自己的 Worker、Durable Object 與 Pages 專案。

## 限制與注意事項

- WebRTC 檔案傳輸是瀏覽器對瀏覽器的 P2P 傳輸；若網路環境無法建立直連，未配置 TURN server 時可能無法連線。
- IPv4 附近偵測會使用瀏覽器探測到的公網 IPv4；若探測服務不可用，請改用 QR Code 或 PIN。
- Durable Object 的限制保護的是 signaling 連線，不會限制 WebRTC `fileChannel` 的檔案資料。
- Tailwind CSS 會在部署時本地建置；QRious 與 jsQR 目前仍由固定版本的外部 CDN 載入，若要提高供應鏈安全，建議日後也將它們納入本地建置。
- Cloudflare 的方案配額、Durable Object 限制與 Pages 設定可能變更，部署時請以 Cloudflare 官方文件為準。

## 本機檢查

修改 HTML class 或 CSS 後，重新產生並提交 `public/tailwind.css`：

```bash
npm run build
```

修改 JavaScript 後可執行：

```bash
node --input-type=module --check < public/app.js
node --input-type=module --check < public/scanner.js
node --input-type=module --check < public/webrtc.js
node --input-type=module --check < do-worker.js
```

## 授權

本專案採用 [MIT License](LICENSE)。
