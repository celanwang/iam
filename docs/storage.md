# 存储设计

本文承接 [`design.md`](./design.md) 与 [`protocol.md`](./protocol.md)，先做数据库选型，再对照当前内存实现，说明要存什么、怎么存、怎么接到现有代码。协议语义以协议文档为准。当前代码范围见 [README](../README.md#当前实现与验收范围)。

本阶段目标：用真实数据库验证「发送成功 = 已落盘且可恢复」，不追求亿级吞吐，不做网关、分片、跨地域。

## 技术选型

### 当前实现卡在哪

现在的 `iam-core` 用 `MemoryStore` 跑通了协议路径。进程一退出，发送结果、消息和同步事件全部消失。协议要求的是：返回 `committed` 之后，重启仍能查到同一条结果，并能靠 SYNC 补齐。所以下一步必须换成可持久提交的存储，而不是继续堆内存细节。

### 存储要满足的能力

对照协议和现有引擎，数据库至少要能：

1. **多行同一事务提交**：一次成功发送要同时写下消息、会话序号、变化事件、发送终态；提交成功才能回 `committed`。
2. **按键点查**：同幂等键重试/查询要立刻找到原结果。
3. **按序号区间读**：SYNC/HISTORY 按 `event_seq` / `create_seq` 分页。
4. **同会话串行写**：避免两个发送抢到同一个序号。
5. **崩溃后状态明确**：事务要么全有，要么全无；不能留下半条消息。
6. **本地好跑**：开发机能用 Docker 起一份，方便 `iam-loop` 做杀进程验收。

### 候选比较

| 方案 | 结论 | 原因 |
|---|---|---|
| **PostgreSQL** | **本阶段选用** | 事务模型清楚，点查/区间扫都熟，Rust 生态成熟（如 sqlx），Docker 单实例即可验证正确性 |
| MySQL / MariaDB | 可替换后备 | 也能做事务；本仓库更熟 Postgres 工具链，无强理由换 |
| SQLite | 不选作主路径 | 适合单机原型，并发与运维模型和后续服务部署差太远 |
| Redis / 纯缓存 | 不选 | 难独立承担「提交即永久」和多行事务 |
| Cassandra 等 AP 库 | 不选 | 最终一致，难直接表达发送原子提交与幂等终态 |
| TiDB / TiKV | 以后再评 | 水平扩展强，但先引入分布式故障面；应在单库语义跑通后再谈 |
| Kafka + DB 双写 | 不选 | 两套真相源，本阶段负担过大 |

### 选型结论

**本阶段确定使用 PostgreSQL 单库（可带副本，写入与裁定以主库为准）。**

先验证正确性与恢复，不把峰值写入当选型标准。确认本文后，再开实现分支接 Postgres；在此之前不装库、不写适配代码也可以，但选型本身不再悬空。

不做：SQL 与另一套 KV 同时裁定提交结果；一上来上 TiDB。

## 对照当前实现：要存什么

以现有 `MemoryStore` / `Engine` 为清单，区分「必须进 Postgres」和「仍可留在进程内」。

### 必须持久化

这些今天在内存里，重启会丢，但协议依赖它们跨进程仍成立：

| 现有结构 | 用途 | 持久化原因 |
|---|---|---|
| `idempotency` | 同键回执 / 最终拒绝 | 重试与 `SEND_STATUS` 的依据 |
| `messages` | 消息正文与位置 | HISTORY / SNAPSHOT / 核对 |
| `Conversation` 的元数据与序号 | `history_id`、`access_version`、`next_*_seq`、成员 | 分配顺序、reset 原因、发权校验 |
| `Conversation.events` | 会话变化流 | SYNC / 丢推后补齐 |
| `account_streams` + 事件 | 账户侧会话发现 | 账户 SYNC / SNAPSHOT |
| `close_watermark_ms` | 关闭证明 | 过期拒绝与清理前提 |
| `max_observed_now_ms` | 见过的最大服务端时间 | 发现时钟回拨 |
| `recovery_incomplete` | 恢复是否完整 | 不完整时停止新接受 |
| `revoked_auth_epochs` | resume 撤销 | 重连拒绝 |
| token 密钥材料 | 游标/快照/分页验签 | 换进程后票据仍能验 |

本阶段会话与成员继续用种子数据写入（对应现在的 `seed_user` / `seed_direct`），暂无好友/建群写接口。

测试桩用户（`users_by_token`）可以先继续种子/配置，不强制做成完整账号系统。

### 不必持久化

| 现有结构 | 原因 |
|---|---|
| `connections` / `device_conn` | 连接生命周期；重连重建 |
| `req_id`、outbound、`push_id` | 连接内状态 |
| `inflight_send` | 串行屏障；崩溃后靠查询/重试恢复未决 |
| `entrance_overloaded`、进程内 `now_ms` | 运行时控制；时间以取库时采样并与 `max_observed_now_ms` 对照 |

## 在 PostgreSQL 里怎么存

先定逻辑表，不在本文锁死 DDL 细节。原则：一张（类）表对应上面一类必须持久化的状态；一次发送用**一个事务**写多表。

```text
idempotency_records   幂等终态（committed / rejected_final + 摘要 + 截止时间）
conversations         会话元数据与 next_create_seq / next_event_seq / members
messages              消息正文与 create_seq
conversation_events   会话事件（event_seq + Change）
account_streams       账户流 epoch / next_event_seq
account_events        账户事件
store_control         单行：close_watermark_ms, max_observed_now_ms, recovery_incomplete
token_keys            key_id + 验证材料
auth_epoch_revocations  可选
```

读写方式：

- **发送成功**：事务内锁会话行 → 查/写幂等 → 涨序号 → 写 messages + conversation_events + idempotency → `COMMIT` → 再回 `committed`。
- **发送查询**：读幂等表 + `store_control`；不要用落后只读副本的空结果下最终结论。
- **SYNC**：按会话或账户的 `event_seq` 区间读事件表；开轮时固定上界。
- **HISTORY / SNAPSHOT**：按 `create_seq` 读 messages，上界与上下文固定。
- **关窗 / 清理**：更新 `store_control.close_watermark_ms`；清理条件见协议（过查询期 + 水位已覆盖）。

Change、MessageContent 等可先存 Protobuf 字节或结构化列；实现时再定，但不允许发送事务拆成多次独立提交。

## 如何与现有代码整合

保持现有 crate 边界，把「存取」从引擎里拆出去：

```text
iam-protocol   不变
iam-core       Engine 继续编排放协议命令
               MemoryStore 改为 Store 的一种实现（内存）
iam-store-pg   新建：PostgreSQL 实现同一 Store（名称可再定）
iam-loop       内存套件保留；另加 Postgres 杀进程 / 重启用例
```

整合顺序：

1. 从 `Engine` 实际用到的 `MemoryStore` 能力抽出 `Store` 接口（发送提交、查询终态、读事件/历史、水位与控制行、种子写入）。
2. `MemoryStore` 先实现该接口，保证现有测试不回退。
3. `PgStore` 用同一接口接 Postgres；本地 Docker 起库。
4. `Engine` 只依赖接口，不关心背后是 HashMap 还是 SQL。
5. 推送仍在提交成功之后；推送失败不回滚库。

交互关系可以看成：

```text
客户端命令
    -> Engine（协议编排，现有逻辑）
        -> Store::commit_send / load_idempotency / read_events ...
            -> MemoryStore    或    PgStore (PostgreSQL)
```

本阶段不把 Engine 拆成微服务，也不引入独立消息队列做提交路径。

## 本阶段验收

接上 Postgres 后，至少要看到：

- 提交成功后杀进程，重启同键得到同一 `message_id` 和位置  
- 提交前杀掉，库中无半成品；同键可再发  
- 水位与过期行为符合协议；恢复不完整时不能新接受  
- 丢推后只靠 SYNC 仍能读到已提交消息  

不做：网关压测、跨城、生产 P99。TiDB 等留到单库语义稳定后再评估。
