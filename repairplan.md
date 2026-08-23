# WebRTC 傳輸卡住 — 根因分析與修復計畫

## Log 分析結論

從 [斷線紀錄log.txt](file:///Users/bobkao/Desktop/Code/VS-Code/bobconnect/%E6%96%B7%E7%B7%9A%E7%B4%80%E9%8C%84log.txt) 可觀察到：

| 指標 | 發送端 (快照 1→2) | 接收端 (快照 1→2) |
|---|---|---|
| `dcBytesSent` | 10,092,930 → 10,092,945 (+15 bytes) | 2,582 → 2,597 (+15 bytes) |
| `dcBytesReceived` | 2,582 → 2,597 (+15 bytes) | 9,765,040 → 9,765,040 (**0 bytes**) |
| `dcMessagesSent` | 175 → 176 (+1) | 91 → 92 (+1) |
| `dcMessagesReceived` | 91 → 92 (+1) | 156 → 156 (**0 messages**) |
| `bufferedKB` | **320.2 KB** → **320.2 KB** (堆積未消化) | 0 | 
| `dcState` / `iceState` | open / connected | open / connected |

> [!CAUTION]
> **根因：發送端 buffered 高達 320KB 卡住不動，但接收端 `dcBytesReceived` 完全停滯 — 表示資料確實被送出（pair bytes 持續增加）但 DataChannel 層面接收端沒有新的 message 回調觸發。這代表傳輸底層仍活躍，但應用層因「背壓 + ACK 死鎖」而完全停止流動。**

---

## 根因分析：五大 Bug

### Bug 1：背壓等待超時 50ms 解除 — 造成假醒與飢餓循環（🔴 致命）
**位置**：[webrtc.js L1122-L1140](file:///Users/bobkao/Desktop/Code/VS-Code/bobconnect/public/webrtc.js#L1122-L1140)

發送端在 `bufferedAmount > 256KB` 時進入背壓等待，但 `setTimeout(50ms)` 會無條件解除 Promise。若緩衝區始終 > 256KB，迴圈會每 50ms 輪詢一次 → 白白耗用 CPU，但不推進任何資料。更糟的是，若 `onbufferedamountlow` 事件因瀏覽器實現差異從未觸發，整個發送迴圈就永遠在這裡空轉。

**Log 佐證**：發送端 `bufferedKB = 320.2` 持續不降，表示資料堆積在 SCTP 緩衝區但底層無法排空。

### Bug 2：ACK 死鎖 — 接收端 ACK 依賴 Worker 回覆，但 Worker 卡在 inFlight 限制（🔴 致命）
**位置**：[webrtc.js L778](file:///Users/bobkao/Desktop/Code/VS-Code/bobconnect/public/webrtc.js#L778), [L939-L943](file:///Users/bobkao/Desktop/Code/VS-Code/bobconnect/public/webrtc.js#L939-L943)

接收端的 ACK 流程：`收到 chunk → 進入 recvQueue → drainRecvQueue (inFlightBytes < 8MB) → Worker write → Worker write-ack → receivedSize += → 觸發 flushCumulativeAck`。

問題是：如果接收端已收到的 chunk 全被 drain 到 worker，但 worker 回覆 write-ack 前新的 chunk 持續湧入 → `inFlightBytes` 達到 8MB 上限 → 停止 drain → ACK 不再發送 → 發送端也停下 → **雙向死鎖**。

### Bug 3：看門狗 15 秒判定太長 + 只在傳輸中判定，忽略背壓卡死場景（🟡 重要）
**位置**：[webrtc.js L340](file:///Users/bobkao/Desktop/Code/VS-Code/bobconnect/public/webrtc.js#L340)

發送端在背壓空轉時仍會更新 `lastActivityTime`（因為 ping/pong 仍在收發），所以看門狗永遠不會觸發重連。背壓空轉場景中，心跳讓看門狗永遠重置，形成「活連線但死傳輸」的現象。

### Bug 4：重連後 OPFS Worker 未重建 SyncAccessHandle — 續傳寫入必定失敗（🔴 致命）
**位置**：[webrtc.js L998-L1001](file:///Users/bobkao/Desktop/Code/VS-Code/bobconnect/public/webrtc.js#L998-L1001)

重連收到 `file-meta`，若 `isSameFile` 判定為真，直接走 else 分支：`isWorkerReady = true; drainRecvQueue()`。但 `handlePeerDisconnection` 中已呼叫 `cleanupOPFSWorker(false, true)` 把 Worker 終止了。所以 `opfsWorker` 為 `null`，`drainRecvQueue` 中 `if (!isWorkerReady || !opfsWorker) return;` 直接退出 — **續傳完全無效**。

### Bug 5：`cleanupOPFSWorker` 於斷線時 forceDelete=true 刪除暫存檔 — 續傳資料全毀（🔴 致命）
**位置**：[webrtc.js L140](file:///Users/bobkao/Desktop/Code/VS-Code/bobconnect/public/webrtc.js#L140)

`handlePeerDisconnection` 中致命斷線路徑呼叫 `cleanupOPFSWorker(false, true)`，`forceDelete=true` 會刪除 OPFS 暫存檔。但如果後續還要續傳，已寫入的進度全部丟失 — `state.receivedSize` 仍記錄已收字節數，重連後會從錯誤的 offset 開始寫入一個全新空白檔案。

---

## 提議修復方案

### [webrtc.js](file:///Users/bobkao/Desktop/Code/VS-Code/bobconnect/public/webrtc.js) — 修復 5 個問題

#### 修復 1：背壓等待改為事件驅動 + 超時保護（消除空轉與假醒）
- 將 50ms 輪詢超時拉長到 **500ms**（純容錯用），主要依靠 `onbufferedamountlow` 事件觸發
- 加入 **傳輸停滯偵測**：在背壓等待迴圈中檢查 `bufferedAmount` 是否有實際下降，若連續多次輪詢無變化，主動觸發重連

#### 修復 2：ACK 與接收端流控解耦 — 避免死鎖
- ACK 改為基於 `receivedSize` 的純計數器，不與 `inFlightBytes` 綁定
- `drainRecvQueue` 中增加 **雜湊式即時 ACK 旁路**：即使 inFlight 已滿，仍可透過定時器定期 flush ACK
- 增加 ACK 定時強制刷新 (每 200ms) 作為兜底

#### 修復 3：看門狗增加「傳輸進度停滯」偵測
- 除了 `lastActivityTime` 時間判定外，增加 **位元組進度判定**：若 `totalSent` / `receivedSize` 連續 10 秒未增長，即使 ping/pong 正常也視為卡死
- 看門狗超時從 15 秒降至 **12 秒**

#### 修復 4：重連續傳正確重建 OPFS Worker
- `handlePeerDisconnection` 非致命斷線時，僅 terminate Worker 但 **不刪除暫存檔**
- `file-meta` 接收到 `isSameFile` 時，重新建立 Worker 並 init（使用相同的 `currentTempFilename`）
- OPFS Worker 增加 `init-resume` 訊息類型，開啟已存在的暫存檔而非覆蓋

#### 修復 5：斷線時分級清理資源
- 非致命斷線（期待重連）：只釋放 WebRTC/WS 連線，保留 OPFS 暫存檔與進度
- 致命斷線（使用者離開 / 超過最大重試）：完整清理含 OPFS 暫存檔

---

### [opfs-worker.js](file:///Users/bobkao/Desktop/Code/VS-Code/bobconnect/public/opfs-worker.js) — 修復 1 個問題

#### [MODIFY] `init` 改為支援續傳模式 (`init-resume`)
- 新增 `init-resume` 訊息類型：`getFileHandle(tempFilename, { create: false })` 打開已存在的暫存檔
- 若檔案不存在則 fallback 到 create 模式

---

## 驗證計畫

### 手動驗證
1. **正常傳輸測試**：選擇一個 50MB+ 的檔案，完整傳輸確認無卡住
2. **斷線重連測試**：傳輸中手動斷開網路/關閉瀏覽器頁面，確認自動重連與續傳
3. **背壓測試**：選擇一個 200MB+ 的大檔案，觀察 `bufferedAmount` 是否正常波動下降
4. **Console 觀察**：確認 Stats 中 `dcBytesSent`/`dcBytesReceived` 持續增長、無死鎖

# WebRTC 傳輸 Bug 修復 — 任務清單

- `[ ]` **Bug 1**: 修復背壓等待 50ms 空轉 → 改為 500ms + 停滯偵測
- `[ ]` **Bug 2**: 修復 ACK 機制 → 心跳定期強制刷新 ACK
- `[ ]` **Bug 3**: 修復看門狗失效 → 增加位元組進度停滯偵測、降低超時至 12 秒
- `[ ]` **Bug 4**: 修復重連後 Worker 未重建 → 提取共用 handler + 斷線保留暫存檔 + onopen 重建 Worker
- `[ ]` **Bug 5**: 修復斷線時分級清理 → 非致命僅終止 Worker 不刪檔
- `[ ]` **OPFS Worker**: 增加 createSyncAccessHandle 重試邏輯
- `[ ]` 驗證：確認程式碼無語法錯誤