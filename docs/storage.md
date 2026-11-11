# 持久化权威路径设计

本文承接 [`design.md`](./design.md) 与 [`protocol.md`](./protocol.md)，给出消息基本协议在**权威持久化**上的设计边界、数据归属、提交事务与验收目标。语义以 `protocol.md` 为准；本文不改写协议字段与错误码，只规定如何可靠落盘与恢复。

当前阶段只做设计，不绑定最终库表 DDL，也不引入网关或产品权限框架。

## 目标与范围

### 目标

把内存内核已验证的权威语义，落到可崩溃恢复的持久存储上，使下列命题在进程重启后仍然成立：

- 发送成功表示约定范围内的**原子持久提交**已完成。
- 同幂等键重试返回原回执或原最终拒绝，不产生第二条消息。
- 接受窗口、查询期限与关闭水位跨重启单调可证。
- 会话变化流、快照与历史读取的固定上界来自已提交且可读的持久状态。
- 推送丢失后，仍可通过 SYNC / SNAPSHOT / HISTORY 从持久记录发现与补齐。

### 本阶段范围

- 单地域、单逻辑库（可多副本）上的权威写入与读取。
- 与现有 `iam-core` 命令路径对应的最小表集合与事务边界。
- 本地验证方式（例如 Docker 中的单一关系库）与故障注入验收项。
- 存储适配与内存实现的替换关系（端口/适配，保持现有 crate 划分，不提前微服务化）。

### 明确不做

- TCP+TLS 网关、写队列与慢连接治理（下一阶段）。
- TiDB/TiKV、跨地域多活、会话分片迁移（规模化阶段再评估）。
- 好友、群管理、已读、编辑撤回写接口。
- 媒体对象存储、冷热分层归档的完整产品方案（可预留接口，本阶段不展开）。
- 用缓存或异步索引充当第二权威。

## 设计原则

1. **一份权威**：消息正文、创建序、变化事件、幂等终态、关闭水位同一权威路径裁定；推送与读模型可滞后，不能独立改写提交事实。
2. **提交边界对齐协议**：`committed` 仅在「内容 + 顺序 + 幂等结果 + 可供后续同步的事件依据」同一事务成功后返回。
3. **关闭证明可持久**：水位与清理条件写入可靠存储；时钟越过截止本身不是关闭证明。
4. **先正确性，后扩展**：本阶段选型优先事务语义、运维简单与本地可复现，不以峰值单条写入论英雄。
5. **不放宽协议**：存储能力不足时调整实现或提出协议修订，不在适配层偷改语义。

## 权威状态清单

下列状态必须持久，并参与对应事务或明确的持久推进过程：

| 状态 | 用途 | 主要协议引用 |
|---|---|---|
| 幂等终态 | 同键回执 / 最终拒绝 / 查询期内重放 | SEND / SEND_STATUS |
| 会话消息与创建序 | 回执位置、历史读取 | SEND / HISTORY |
| 会话变化事件 | PUSH / SYNC 增量 | SYNC / PUSH |
| 会话 `history_id`、`access_version` | 上下文与 reset 原因 | SYNC / SNAPSHOT / HISTORY |
| 账户变化事件 | 会话发现 | 账户 SYNC / 账户 SNAPSHOT |
| 关闭水位 | 过期拒绝与清理前提 | 接受窗口与终态清理 |
| 认证代次撤销（若启用 resume） | RECONNECT 拒绝 | AUTH / RECONNECT |
| 令牌验证密钥标识 | 游标/快照/分页票据跨重启 | 不透明令牌 |

连接表、出站队列、`req_id` 水位属于连接生命周期，**默认不作为跨进程权威**；接管后客户端靠查询与重试恢复未决发送。

## 逻辑数据模型（概念）

以下为逻辑集合，不是最终 DDL：

```text
TenantUser
  tenant_id, user_id, auth 绑定信息（本阶段可继续测试桩）

Conversation
  conversation_id, kind, history_id, access_version,
  next_create_seq, next_event_seq, members[]

Message
  (conversation_id, message_id) -> position, sender, accepted_at_ms,
  state, content, content_digest

ConversationEvent
  (conversation_id, event_seq) -> Change payload（已提交）

AccountEvent
  (tenant_id, user_id, event_seq) -> Change payload（会话 upsert/remove）

IdempotencyRecord
  (tenant_id, user_id, device_id, conversation_id, client_msg_id)
  -> committed | rejected_final
  + content_digest + accept_until_ms + query_until_ms + 回执或错误

CloseWatermark
  覆盖范围（按 client_msg 时间轴或约定分片键）单调前进的水位

AuthEpochRevocation（可选）
  auth_epoch -> revoked

TokenKeyring
  key_id -> 验证材料；支持保留旧密钥直至票据过期
```

索引与分桶在实现稿中细化；概念上至少支持：

- 按幂等键点查终态
- 按会话 `event_seq` 区间扫描
- 按会话 `create_seq` 向前历史分页
- 按用户账户事件扫描

## SEND 权威提交事务

一次成功接受的发送，在同一事务中至少完成：

1. 在会话提交边界锁定或等价串行化（同会话写顺序）。
2. 读取或插入幂等记录；冲突与在途规则遵循协议。
3. 分配 `create_seq` / `event_seq`，写入 `Message` 与 `ConversationEvent`。
4. 写入 `IdempotencyRecord::committed`（含摘要与回执字段）。
5. 提交成功后，才向调用方返回 `committed`；其后推送失败不回滚。

最终拒绝（窗口内权限/内容规则等）若协议要求保存终态，同样在权威事务中写入 `rejected_final`，不得仅停留在网关内存。

查询路径只读权威表；接受窗口内查无返回 `NOT_FOUND`，不得因读副本延迟写成 `rejected_final`。

## 关闭水位与清理

- 水位持久保存，重启后不得回退。
- 推进条件：截止前可能成功的决定已恢复完毕，或旧写入方与未决决定已永久失去提交资格（与协议一致）。
- 清理幂等终态必须同时满足：已过 `query_until_ms`、水位已覆盖、未决已封闭、恢复记录仍能证明不可再接受。
- 本阶段允许用显式后台任务或管理接口推进水位以便验收；生产自动化策略可后置，但不能把“仅测试开关”写成最终关闭证明定义。

## 读取路径

- **SYNC**：上界取已提交且完整可读的最大事件序；分页令牌绑定的固定上界来自开启本轮时的持久快照点。
- **SNAPSHOT / HISTORY**：按固定上界与上下文重建视图；分页期间新提交不进入本轮页，留给下一轮。
- 读可使用主库或同步足够的副本；若副本不足以证明完整覆盖，不得假装 page 成功。

## 与现有代码的衔接

```text
iam-protocol          不变（编解码与校验）
iam-core              保留命令编排；MemoryStore 变为 Store 端口的一种实现
iam-store-postgres*   本阶段设计落地后的候选适配（名称实现时再定）
iam-loop              增加崩溃恢复 / 重启后重放类验收（仍可不引入真网关）
```

`*` 选型未最终冻结前，文档称「关系型权威库适配」。默认讨论候选为 PostgreSQL（本机 Docker），确认前不安装、不写代码。

## 选型讨论状态

| 选项 | 对本阶段 | 说明 |
|---|---|---|
| PostgreSQL | 推荐默认候选 | 事务清晰、本地 Docker 简单、足以验证权威路径 |
| MySQL/MariaDB | 可选 | 团队熟悉时可替换，语义覆盖需同等验收 |
| SQLite | 仅限单机原型 | 并发与运维模型不同，不作为亿级方向假设 |
| TiDB（TiKV） | 延后 | 水平扩展优势在规模化阶段评估；本阶段增加分布式故障面 |

双写「SQL + 独立 KV」不作为本阶段方案。

## 验收目标（设计冻结后实现）

至少覆盖：

1. 提交成功后杀进程，重启同键重试得到同一 `message_id` / 位置。
2. 接受截止后无水位推进 → 不得新提交；水位推进且记录完整 → `REQUEST_EXPIRED` 或协议规定的查询结果。
3. 查询截止且水位覆盖后，清理前后对旧键的结果一致且不可再提交。
4. 持久事件流支撑：丢推后 SYNC/SNAPSHOT 仍能发现消息。
5. 令牌密钥轮换：旧 key_id 在有效期内仍可验证，过期或未知 key 拒绝。
6. 恢复未完成标志打开时，新接受返回协议要求的不可用类结果，不得假装最终拒绝。

不在本阶段验收：网关断连风暴、跨城切换、P99 与五个九的生产统计。

## 文档关系与演进

- 总方向：[`design.md`](./design.md)
- 协议契约：[`protocol.md`](./protocol.md)
- 本文：权威持久化路径
- 实现开始前：冻结本文的范围与事务边界；库表 DDL、迁移与 Docker 编排另开实现分支（如 `feat/storage`）

后续若引入分片或 TiDB，另写演进设计，不在本文偷换本阶段验收标准。
