# 图片中转扩展版快速开始

本仓库基于 QuantumNous New API，保留其项目名称、许可和归属信息，并增加图片生成中转能力。部署会从当前仓库源码本地构建，不会拉取未包含这些改动的上游预构建镜像。

## 启动

需要安装 Docker Engine 和 Docker Compose v2：

```sh
git clone https://github.com/Bbon22/new-api-image-relay.git
cd new-api-image-relay
docker compose up -d --build
```

首次启动后访问 <http://localhost:3000> 并完成管理员初始化。Compose 默认仅将应用绑定到本机回环地址；通过 Nginx、Caddy 等 HTTPS 反向代理对外提供服务。生产环境应先更换 Compose 示例中的数据库和 Redis 密码。

## 配置渠道与图片地址

在管理后台添加你有权使用的图像模型渠道、模型映射和计费设置，再创建供客户使用的 API Key。上游凭据只填写在管理后台，不要提交到 Git 或 `.env` 文件。

图片结果和参考图需要经本站 HTTPS 地址访问。将 `.env.example` 复制为 `.env`，设置 `TASK_PUBLIC_ADDRESS` 为本站公开 HTTPS 根地址，并确认反向代理允许访问 `/v1/images/relay/`。如果不设置该变量，系统会使用管理后台的服务地址。

## 接口

- 同步文生图：`POST /v1/images/generations`
- 同步图生图：`POST /v1/images/edits`
- 异步文生图：`POST /v1/images/generations/async`
- 异步图生图：`POST /v1/images/edits/async`
- 查询异步任务：`GET /v1/tasks/{task_id}`

异步提交成功后返回 `202` 和任务 ID；使用提交时的同一个 API Key 查询任务。接口字段、轮询提示、图片保留和失败处理规则见 [`docs/image-async.md`](./docs/image-async.md)。

## 构建说明

Docker 构建会安装 Web 依赖并编译前端与 Go 服务。也可以按项目主 README 的开发说明分别运行后端和前端。首次容器构建需要联网下载依赖，所需时间取决于机器和网络。
