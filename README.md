# fal.ai 视频渠道网关

## 1. 运行边界

此服务把 fal 组织级 API Key 与 Video MCP 任务分离。管理监听 `8000` 只供 Studio 受控 AppDashboard 代理调用；执行监听 `8001` 只供 Video MCP 调用。两个监听各有独立 Bearer Token，均不开放公网。应用发行、权限和部署事实以设计仓库的 `docs/design/app/fal/README.md` 与对应清单为准。

## 2. 必需配置

- `FAL_MANAGEMENT_TOKEN`：Studio 独占的内部管理凭据，至少 32 字符。
- `FAL_EXECUTION_TOKEN`：Video MCP 独占的执行凭据，至少 32 字符。
- `FAL_ENCRYPTION_KEY`：Fernet 主密钥，由 Kubernetes Secret 注入；与数据库分开保管。
- `FAL_STATE_DB`：持久数据库路径，默认 `/data/fal/state.sqlite3`。
- `FAL_NETWORK_URL`、`FAL_NETWORK_TOKEN`：成对配置的 Network 内部只读解析入口和专属服务凭据；未配置时只能创建明确直连账号，不能选择代理。
- `FAL_LEGACY_PROXY_REFERENCE`：首次迁移旧环境代理时显式提供的 Network 引用 JSON，只含 `mode=proxy`、`proxy_id`、`revision`。按组织解析并核对原代理地址、端口与认证；缺映射或不匹配则拒绝账号切换，不复制代理凭据。实际迁移与协议核验证据由设计仓库维护。

管理请求须携带 Studio 验证后的 `X-User-Id` 与 `X-Organization-Id`。Studio 注册规则对每个管理路径设置 `requireManage=true`。执行请求只信任 Video MCP 内部凭据及其组织声明。浏览器不直接接触上述内部 Token；提交的 fal Key 不返回、不写日志或客户端持久存储。

## 3. 授权与执行

新版管理接口提供账号管理、启停、代理选择及调用日志；旧授权和限额接口保留历史恢复兼容。Key 验证使用 fal 官方需要 `models:requests:read` 的只读请求查询；成功仅证明此权限，不证明余额或生成资格。执行白名单包含原 `minimax/h3/reference-to-video` 480P 适配和 `bytedance/seedance-2.0/reference-to-video` 参考生视频适配；客户端不能指定任意端点。新任务使用 `/v1/calls`，将模型、账号、Key 版本和代理修订固定到持久记录；账号改代理后重新验证授权，旧任务仍按原引用恢复。

执行提交先持久化幂等记录，再请求固定的 fal Queue 地址。结果不确定时保留未知受理，不自动重提。历史 Video fal 任务仍按其原适配器版本和旧 `/v1/submissions` 路径恢复。迁移前原任务使用环境出口，迁移后使用已核验并固定的 Network 引用，新账号直连不继承环境代理；解析失败不自动直连。媒体下载与项目 Artifact 仍由 Video MCP 完成，代理选择不改变该下载链路。

首次迁移记录源数据和目标投影的受密钥保护摘要。失败恢复旧版后再次升级，若只有旧表变化则重新导入并保留账号 ID；目标已有变化或检查点缺失则拒绝自动合并，保留原数据。此机制不代表新版产生业务数据后可直接回退旧版。

## 4. 轻量验证

在服务目录运行 `python -m unittest discover -s tests -v`；测试用 MockTransport，不持有真实 Key、不触发付费生成。Dashboard 构建可运行 `python -m fal_gateway.dashboard --source dashboard --output <空目录>`。镜像由 GitHub Actions 的 `fal.yml` 构建。

源码从 `verdantflarehub/verdantflare-app-video` 提交 `1a6c5422c77c29732fcbfd07a14c3aca907f7ead` 拆分，原始历史保留在来源仓库；fal 同时保留迁移时尚未提交的渠道账号实现。当前目录为唯一源码入口。产品与部署事实统一维护于 `verdantflare-design/docs/design/app/fal/README.md` 和设计仓库部署目录。独立工作流保留原镜像命名，下一发行递增版本；迁移不重打已发布标签。
