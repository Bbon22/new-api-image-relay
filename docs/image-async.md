# 图片异步接口

平台提供两个异步扩展接口，原有同步接口不变：

- `POST /v1/images/generations/async`：文生图
- `POST /v1/images/edits/async`：图生图，支持原有 JSON 或 multipart 请求体

请求格式与对应的 OpenAI Images 接口相同。成功受理后返回 `202`：

```json
{
  "task_id": "task_xxx",
  "status": "queued",
  "message": "图片已受理，请使用任务号查询结果",
  "poll_interval_seconds": 3
}
```

使用同一个 API Key 查询 `GET /v1/tasks/{task_id}`。任务查询会校验 Key 的归属；不存在或不属于当前 Key 的任务统一返回 `404`。成功后只返回本站托管的签名 URL，图片保留 9 小时，过期返回 `expired`，不退额度。

任务查询建议按响应中的 `poll_interval_seconds` 轮询。每个 API Key 每分钟最多查询 120 次，单个任务每分钟最多查询 20 次；收到 `429` 时应遵循 `Retry-After`，不要立即重试。

建议提交时带 `Idempotency-Key`。10 分钟内相同 Key 和相同请求会复用原任务；相同 Key 但请求内容不同返回 `409`。
