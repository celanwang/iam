# 持久化权威路径设计

本文承接 [`design.md`](./design.md) 与 [`protocol.md`](./protocol.md)。语义以 `protocol.md` 为准；本文只规定权威状态如何落盘、何时算提交成功、关闭证明如何产生，以及重启后如何恢复。

当前阶段**只做设计**。本文冻结行为边界与事务规则；**不**冻结物理 DDL、索引名或最终 crate 名称。未最终确认选型前，不安装数据库、不写适配代码。

## 1. 本文回答什么、不回答什么

### 1.1 必须回答清楚（本阶段）

| 问题 | 本文立场 |
|---|---|
| 哪些状态跨进程重启后仍有效？ | §3 权威状态表；未列入者默认不持久 |
| `committed` 何时允许返回？ | §5：同一事务提交成功之后 |
| 进程崩溃时未决发送如何表现？ | §5.4：无终态记录则按窗口/恢复规则返回 `unresolved`，不得伪装终态 |
| 关闭水位是什么、何时可推进？ | §6：按 `client_msg_id` 时间 `T` 的单调覆盖上界 |
| `acceptance_closed = true` 何时成立？ | §6.3：关闭证明表，不可由“查空”单独推导 |
| SYNC/SNAPSHOT/HISTORY 的固定上界从哪来？ | §7：已提交且完整可读的持久事件上界 |
| 本阶段存什么产品数据？ | 单聊种子会话、文本消息与协议必需的流；无好友/建群/已读写接口 |

### 1.2 明确不回答（延后）

- 网关连接表、出站推送队列、慢连接治理
- TiDB/TiKV、跨地域、会话分片迁移
- 好友、群管理、已读、编辑撤回写接口
- 媒体对象存储与冷热归档产品方案
- 用缓存或异步索引充当第二权威
- 生产自动化水位推进策略的调度细节（本阶段允许显式管理接口推进，但关闭证明定义本身不能依赖“仅测试开关”）

## 2. 目标与不变量

把内存内核已验证的权威语义落到可崩溃恢复的存储上。重启后下列命题仍成立：

1. **提交原子性**：返回 `committed` 当且仅当「消息正文 + 创建序 + 会话变化事件 + 幂等终态（含回执字段）」已在同一权威事务中提交成功。
2. **幂等唯一性**：同一完整幂等键在查询期限内最多对应一个终态；重试返回原回执或原最终拒绝，不产生第二条消息。
3. **窗口单调性**：接受截止、查询截止由 `client_msg_id` 的 `T` 按协议固定公式计算；关闭水位跨重启只前进不回退。
4. **读取上界诚实**：SYNC/SNAPSHOT/HISTORY 的固定上界来自已提交且完整可读的持久状态；不允许用滞后副本的空结果假装 page 成功或证明“从未提交”。
5. **推送可补偿**：推送丢失不回滚提交；客户端仍能通过 SYNC / SNAPSHOT / HISTORY 从持久事件与消息发现并补齐。

## 3. 权威状态清单

### 3.1 必须持久（跨进程权威）

| 状态 | 主键 / 覆盖范围 | 必含字段（逻辑） | 谁写入 | 谁读取 |
|---|---|---|---|---|
| 幂等终态 | `(tenant_id, user_id, device_id, conversation_id, client_msg_id)` | 分支 `committed` 或 `rejected_final`；内容摘要；`accept_until_ms`；`query_until_ms`；回执或拒绝码 | SEND 权威事务 | SEND / SEND_STATUS |
| 会话消息 | `(tenant_id, conversation_id, message_id)` | `create_seq`、`history_id`、`sender`、`accepted_at_ms`、`state`、正文、内容摘要 | 同 SEND 事务（仅 `committed`） | HISTORY / SNAPSHOT / 核对 |
| 会话变化事件 | `(tenant_id, conversation_id, event_seq)` | 已提交 `Change` 载荷 | 同 SEND 事务（及本阶段允许的会话上下文变更） | 会话 SYNC / PUSH 补偿依据 |
| 会话元数据 | `(tenant_id, conversation_id)` | `kind`、`history_id`、`access_version`、`next_create_seq`、`next_event_seq`、成员集合（见 §3.3） | 种子写入；SEND 推进序号 | SYNC reset 原因、SNAPSHOT、HISTORY 上下文 |
| 账户变化事件 | `(tenant_id, user_id, event_seq)` | 会话 upsert/remove 等账户 `Change` | 本阶段：会话对用户可见时写入（种子/可见性变化） | 账户 SYNC / 账户 SNAPSHOT |
| 账户流元数据 | `(tenant_id, user_id)` | `epoch`、`next_event_seq` | 账户事件写入时 | 账户 SYNC |
| 关闭水位 | 单逻辑库一条覆盖上界（本阶段） | `close_watermark_ms`：已关闭证明覆盖到的最大 `T`（含） | 水位推进过程（§6） | SEND / SEND_STATUS 关闭判定 |
| 时钟观测上界 | 单逻辑库 | `max_observed_now_ms` | 每次权威路径采用的服务端时间 | 检测回拨；回拨时停止新接受 |
| 恢复不完整标志 | 单逻辑库 | `recovery_incomplete: bool` | 启动/恢复流程 | SEND / SEND_STATUS：为真则停止新接受并按 §6.3 返回 |
| 认证代次撤销 | `auth_epoch` | 已撤销标记 | 登出/撤销（若启用 resume） | RECONNECT / 已认证连接校验 |
| 令牌验证密钥 | `key_id` | 验证材料；旧密钥保留至绑定票据过期 | 密钥轮换管理 | 游标 / 快照 / 分页票据验签 |

### 3.2 默认不持久（连接生命周期）

| 状态 | 理由 | 崩溃后行为 |
|---|---|---|
| 连接表、`req_id` 水位、出站缓冲、`push_id` | 属于网关/连接；协议不以连接身份做去重 | 客户端重连；未决发送靠 SEND_STATUS / 重试恢复 |
| 设备当前连接映射、串行发送 inflight 表 | 仅保证同连接处理期的互斥与串行屏障 | 崩溃后 inflight 消失；若无持久终态，按未决规则返回 `unresolved`，**不得**写成 `rejected_final` |
| 入口过载开关等进程内限流状态 | 运行时控制面 | 重启后按新进程策略；不改变已提交终态 |

### 3.3 本阶段对“成员 / 会话可见性”的约定

本阶段**没有**好友与建群写接口。`members` 与账户 upsert 仅服务于：

- 本地/测试种子数据（例如预先写入的单聊双方）；
- 发送时校验“调用方是否在成员集合中”（权限拒绝走权威 `rejected_final`，与现有内存内核一致）。

不在本文设计生产级成员变更、退群重入或拉黑生效时序；那些进入产品权限模块后再扩展账户事件类型与事务。

## 4. 逻辑数据模型

以下是逻辑集合与字段义务，**不是** DDL。实现稿可拆表、加索引，但不得删减下列义务字段。

```text
IdempotencyRecord
  key = (tenant_id, user_id, device_id, conversation_id, client_msg_id)
  outcome =
    committed {
      receipt_message_id, create_seq, history_id, accepted_at_ms,
      content_digest, accept_until_ms, query_until_ms
    }
    | rejected_final {
      error_code, content_digest, accept_until_ms, query_until_ms
    }
  约束:
    - 同一 key 至多一行终态；禁止“pending/inflight”持久行（见 §5.2）
    - committed 与 rejected_final 互斥
    - 内容比较规则与 protocol.md 固定摘要一致；摘要规则标识若演进需显式版本字段

Message
  key = (tenant_id, conversation_id, message_id)
  create_seq, history_id, sender_user_id, accepted_at_ms,
  state, content, content_digest
  约束:
    - 仅在 IdempotencyRecord.committed 同一事务中插入
    - (conversation_id, create_seq) 在同一 history_id 下唯一

Conversation
  key = (tenant_id, conversation_id)
  kind, history_id, access_version,
  next_create_seq, next_event_seq, members[]
  约束:
    - next_* 表示“下一个将分配的序号”，从 1 起；水位 0 表示尚无事件（与协议一致）
    - 分配序号必须在会话提交边界内与事件/消息写入同事务完成

ConversationEvent
  key = (tenant_id, conversation_id, event_seq)
  change_payload（已提交 Change）
  约束:
    - event_seq 与 Conversation.next_event_seq 分配一致
    - PUSH 可丢失；SYNC 以本表为增量权威依据

AccountStream
  key = (tenant_id, user_id)
  epoch, next_event_seq

AccountEvent
  key = (tenant_id, user_id, event_seq)
  change_payload
  约束:
    - 本阶段至少覆盖会话对用户可见时的 conversation upsert

CloseWatermark
  scope = 本阶段单逻辑库全局一行
  close_watermark_ms   // 已取得关闭证明的最大 client_msg 时间 T（含）
  约束:
    - 只允许 UPDATE 增大，禁止减小
    - 清理幂等行后仍必须保留该行

StoreControl
  max_observed_now_ms
  recovery_incomplete
  约束:
    - max_observed_now_ms 只增不减（权威路径每次采用 now 时推进）
    - recovery_incomplete = true 时禁止形成新的接受决定

AuthEpochRevocation
  auth_epoch -> revoked_at_ms（或等价标记）

TokenKeyring
  key_id -> 验证材料, not_after_ms
  约束:
    - 验签只依赖 keyring，不依赖原网关内存
    - 轮换时旧 key_id 保留到其签发票据最晚过期之后
```

### 4.1 本阶段索引义务（逻辑）

实现必须能在权威库上高效完成，且读路径不得依赖“最终一致的第二索引”作为唯一依据：

1. 幂等键点查终态  
2. 会话 `event_seq` 区间扫描（含上界）  
3. 会话在固定 `history_id` 下按 `create_seq` 向前历史分页  
4. 用户账户 `event_seq` 区间扫描  

## 5. SEND / SEND_STATUS 权威路径

### 5.1 术语

| 术语 | 定义 |
|---|---|
| 完整幂等键 | `(tenant_id, user_id, device_id) + MessageKey`，与协议一致；不含 `connection_id` / `req_id` |
| 接受决定 | 在权威路径上形成 `committed` 或窗口内业务 `rejected_final` 并写入持久终态 |
| 在途（inflight） | 进程内：该连接正在处理、尚未提交终态的发送；**不落盘** |
| 关闭证明 | `close_watermark_ms >= accept_until_ms(T)`，且记录完整性可证（`recovery_incomplete = false`） |

### 5.2 为什么本阶段不持久化 “pending” 行

协议要求：尚未形成可恢复终态时返回 `IN_PROGRESS`，不能把仅内存冲突当成关闭证明。

本阶段选择与当前内存内核一致的模型：

- 权威表**只有**终态行（`committed` / `rejected_final`）。
- 处理中的互斥由「会话提交边界上的事务锁 + 连接内 inflight」保证。
- 进程崩溃导致事务回滚后，键上无终态行：查询按 §5.5 / §6.3 返回 `NOT_FOUND` / `IN_PROGRESS` / `RESULT_UNAVAILABLE` / `REQUEST_EXPIRED` 等，**绝不**因“空”直接 `rejected_final`（除非已有关闭证明且完整性可证，见过期拒绝规则）。

若将来引入跨进程共享的持久在途租约，必须另开修订：租约超时、fencing、与关闭水位的交集；**不在本阶段默认启用**。

### 5.3 成功接受（`committed`）事务步骤

对合法且仍在接受窗口内的发送，在**同一数据库事务**中按下列顺序执行（顺序可优化，但提交前必须全部成功）：

1. **进入会话提交边界**：锁定该 `(tenant_id, conversation_id)` 的会话行（或等价串行化手段），保证同会话写顺序与序号分配不交错。
2. **读取幂等终态**：
   - 若已有终态且摘要一致 → 返回原结果（只读提交或无写入提交）；不一致 → `IDEMPOTENCY_CONFLICT`，`acceptance_closed = true`，不覆盖。
   - 若无终态 → 继续。
3. **窗口与控制面检查**（任一条失败则不写入新接受决定）：
   - `recovery_incomplete == true` → `RESULT_UNAVAILABLE`（关闭标记见 §6.3）。
   - `now < max_observed_now_ms`（回拨）→ 停止新接受，`RESULT_UNAVAILABLE`。
   - `T > now + 120_000` → `CLOCK_SKEW`，`acceptance_closed = false`。
   - `now >= accept_until_ms` → 不得新接受；走 §6.3 过期/未关闭分支。
4. **业务规则**（本阶段：成员、文本长度等）：失败则在同一事务写入 `rejected_final` 并提交，然后返回；无法写入则 `unresolved`，不得伪装最终失败。
5. **分配序号并写入**：
   - `create_seq = next_create_seq`，`event_seq = next_event_seq`，随后两者各自 +1 写回会话行；
   - 插入 `Message`；
   - 插入对应 `ConversationEvent`（至少包含该消息创建变化）；
   - 插入 `IdempotencyRecord::committed`（含回执字段与两截止时间）。
6. **推进 `max_observed_now_ms`**（若本次 `now` 更大）。
7. **提交事务**。仅在提交成功后向调用方返回 `committed`。

提交成功后的推送失败**不回滚**。推送不是权威事务的一部分。

### 5.4 崩溃与并发语义（写死）

| 场景 | 存储结果 | 客户端可见 |
|---|---|---|
| 事务提交前进程崩溃 | 无终态行、无消息、无事件 | 同键重试可再次进入接受；查询在窗口内为 `NOT_FOUND`（可重试）或处理中语义，**非**最终拒绝 |
| 事务已提交、响应未写出 | 终态与消息已在 | 重试/查询返回同一 `message_id` 与位置 |
| 同键并发两个事务 | 至多一个提交成功；另一个看到终态后走幂等回放或冲突 | 不得产生两条消息 |
| 同设备同会话串行屏障 | 连接内行为；不落盘 | 崩溃后屏障消失；未决键仍靠查询/重试，不自动换键 |

### 5.5 SEND_STATUS 只读规则

1. 只读权威幂等表（及关闭水位 / 控制面标志）；**禁止**用延迟只读副本的空结果证明不存在。
2. 命中终态且在 `query_until_ms` 内 → 原回执或原拒绝；摘要冲突 → `IDEMPOTENCY_CONFLICT`。
3. 未命中终态 → 按 §6.3 决策表，不得发明终态行。
4. 查询回执校验幂等作用域身份，不返回消息正文。

## 6. 关闭水位、恢复与清理

### 6.1 水位定义（消除“覆盖范围”歧义）

协议：水位按**客户端消息时间**覆盖范围单调前进。

本阶段冻结为：

```text
T(client_msg_id) = client_msg_id 的 UUID 时间毫秒
accept_until_ms  = T + 86_400_000
query_until_ms   = T + 691_200_000

close_watermark_ms = W
含义: 对所有满足 T <= W 的逻辑键，权威路径已取得“不能再形成新接受决定”的关闭证明上界。
判定某键是否已关闭: W >= accept_until_ms(T)
```

本阶段使用**单逻辑库全局一个 `W`**。不按会话、不按用户分片水位。分片水位留待规模化阶段，且必须证明不会回退与裂缝。

### 6.2 何时允许推进 `W`

允许将 `W` 推进到某个值 `W'`（`W' > W`）当且仅当同时成立：

1. `recovery_incomplete == false`（当前权威记录完整性可证）；  
2. 所有**可能**在 `T <= W'` 范围内形成接受决定的写入方，要么已恢复到确定终态，要么已永久失去提交资格（本阶段：单进程权威路径重启后无跨进程租约，则“无未提交持久决定”即可对该范围封闭；若日后有多写方，必须增加 fencing，否则禁止推进）；  
3. 推进操作为权威库上的比较并交换式更新：`UPDATE ... SET close_watermark_ms = W' WHERE close_watermark_ms = W`（或等价只增更新），失败则重读重试；  
4. **禁止**仅因为墙上时钟 `now >= accept_until_ms` 就推进或把时钟当作关闭证明。

本阶段允许用**显式管理接口 / 后台任务**推进 `W` 以便验收。自动化策略可后置，但任何实现都必须满足上述四条；测试辅助开关不能重新定义关闭证明。

### 6.3 `acceptance_closed` 与结果决策表

对给定键，先算 `T`、`accept_until`、`query_until`，再查终态与控制面：

| 条件（自上而下先匹配先命中） | 结果 | `acceptance_closed` |
|---|---|---|
| 命中终态且 `now < query_until` | 原 `committed` / `rejected_final` | （终态分支，无此字段） |
| 命中终态且 `now >= query_until`，且 `W >= accept_until` | `unresolved / RESULT_EXPIRED` | `true` |
| 命中终态且 `now >= query_until`，且尚未 `W >= accept_until` | `IN_PROGRESS` 或 `RESULT_UNAVAILABLE` | `false`（不得清理） |
| 未命中终态，且 `recovery_incomplete` 或时钟不可靠 | `RESULT_UNAVAILABLE` | 仅当已独立满足 `W >= accept_until` 时可为 `true`，否则 `false` |
| 未命中终态，且 `T > now + 120s` | `CLOCK_SKEW` | `false` |
| 未命中终态，且 `now < accept_until` | `NOT_FOUND`（可 `RETRY_SEND`）或处理中 | `false` |
| 未命中终态，且 `now >= accept_until`，且尚未 `W >= accept_until` | `IN_PROGRESS` | `false` |
| 未命中终态，且 `now >= accept_until`，且 `W >= accept_until`，且完整性可证 | `rejected_final / REQUEST_EXPIRED` | （最终拒绝分支） |
| 摘要冲突且权威终态存在 | `IDEMPOTENCY_CONFLICT` | `true` |

说明：

- `REQUEST_EXPIRED` **不需要**为每个从未出现的键写拒绝行；依据是 `W` + 完整权威记录。  
- 查空**永远不是**单独的关闭证明。  
- `RESULT_UNAVAILABLE` 在已有关闭证明时可将关闭标记设为 `true`，表示“不会再提交”，但仍不能捏造历史终态内容。

### 6.4 恢复不完整标志

| 动作 | 规则 |
|---|---|
| 置 `recovery_incomplete = true` | 启动后无法证明幂等/水位/会话序号与事件完整可读；或从可能不完整的备份恢复时 |
| 为 true 时 | 禁止新的接受决定；SEND/SEND_STATUS 按上表返回，不得把缺失当最终拒绝（除非已有独立关闭证明且仅表达“不再提交”） |
| 清回 false | 仅当恢复校验通过：水位行存在、控制面可读、抽查/校验表明权威表完整到声明范围 |

本阶段恢复校验的最小可接受定义：进程启动后能读取 `CloseWatermark` 与 `StoreControl`；若这两行缺失则视为不完整并拒绝新接受，直到运维初始化或修复完成。更强的校验（校验和、备份位点）可在实现稿加强，但不能弱于本条。

### 6.5 终态清理

清理某时间范围的幂等终态行，必须**同时**满足：

1. 服务端时间已达到该范围所有键的 `query_until_ms`（即以该范围最大 `T` 计，`now >= T_max + 691_200_000`）；  
2. `close_watermark_ms` 已覆盖该范围（`W >= T_max` 对应的接受截止，即对范围上界 `T_max` 有 `W >= T_max + 86_400_000`）；  
3. 旧写入方与未决操作已封闭（与 §6.2 相同前提）；  
4. 清理后仍保留 `CloseWatermark` 与 `StoreControl`，使“该范围不能再接受”可继续证明。

TTL 只能触发检查，不能直接删除。消息正文保留期与幂等终态保留期独立；清理幂等行不得导致已提交消息在 HISTORY 承诺期内不可读（正文保留策略实现稿另定，本阶段验收以协议查询窗口与同步可见性为准）。

## 7. 读取路径

### 7.1 共同规则

- **上界**：本轮 SYNC/SNAPSHOT 的固定上界 = 开启本轮时，目标流上“已提交且完整可读”的最大 `event_seq`（或协议规定的等价观察点）。分页过程中新提交不进入本轮页。  
- **主读优先**：本阶段默认权威读走主库（或同步复制且能证明无空洞的副本）。若副本无法证明已覆盖到声明上界，必须失败为可重试错误或触发 reset，**不得**返回残缺 page 并签发“已覆盖”游标。  
- **令牌**：cursor / snapshot / page token 的完整性与用途绑定依赖 `TokenKeyring`；验签失败按协议错误码，不回落到明文信任。

### 7.2 SYNC

- 无游标：返回 `reset` + 快照入口，禁止直接 `page`。  
- 有游标：校验身份、目标、epoch、`access_version`、过滤规则；变化已被清理或无法完整恢复 → `reset` 及协议原因优先级。  
- `covered_through <= upper_bound`；未达上界必须给下一页令牌；`next_cursor` 只反映本页覆盖，不跳到上界。

### 7.3 SNAPSHOT / HISTORY

- 快照的 `snapshot_id`、上下文、上界、历史窗口在建立时固定，分页不得移动窗口。  
- 会话快照反映同一上界处的消息视图；不能把普通增量页拼接成假快照。  
- HISTORY 在固定视图与保留下界内按 `create_seq` 向前读；上下文变化按协议作废票据。

### 7.4 账户流 vs 会话流

- 账户流：发现会话可见性变化（本阶段主要是种子 upsert）。  
- 会话流：会话内消息/变化。  
- 二者进度独立；重连后两边都要能主动 SYNC，不能只靠推送。

## 8. 与现有代码的衔接

```text
iam-protocol     不变
iam-core         保留命令编排；MemoryStore 实现同一 Store 端口的易失后端
（关系库适配）   实现同一端口的持久后端；crate 名实现时再定
iam-loop         增加：杀进程 / 重启后同键回放、水位、清理、丢推后 SYNC 验收
```

端口义务（逻辑方法，名称可调整）：

- 在会话边界内执行 §5.3 的接受事务  
- 只读查询 §5.5  
- 读取/推进关闭水位与 `recovery_incomplete` / `max_observed_now_ms`  
- 按上界读取会话/账户事件页与历史页  
- 种子写入会话与账户 upsert（测试与本地验收）

MemoryStore 与持久适配必须能跑同一组语义测试；允许持久套件额外覆盖崩溃注入。

## 9. 选型状态

| 选项 | 本阶段 | 说明 |
|---|---|---|
| PostgreSQL | 默认候选 | 事务与本地 Docker 验收成本低；**确认前不安装、不写代码** |
| MySQL/MariaDB | 可选替换 | 需同等验收套件 |
| SQLite | 仅单机原型 | 不作为规模方向假设 |
| TiDB / TiKV | 延后 | 本阶段不引入分布式事务与多写方水位复杂度 |

禁止本阶段双写「SQL + 独立 KV」并让两者都能独立裁定提交事实。

## 10. 验收目标（设计冻结后实现）

1. 提交成功后杀进程，重启同键重试得到同一 `message_id` / `create_seq` / `history_id`。  
2. 事务提交前杀进程，重启后同键可再次接受或按窗口查询为未决，**不得**出现幽灵消息或无事件的回执。  
3. `now == accept_until`：无水位 → `unresolved / IN_PROGRESS`；水位推进且完整 → 无终态键为 `rejected_final / REQUEST_EXPIRED`。  
4. `now == query_until` 且已关闭：`RESULT_EXPIRED` 且 `acceptance_closed = true`；清理前后对旧键均不可再提交。  
5. 丢推后仅靠 SYNC/SNAPSHOT 仍能发现已提交消息。  
6. 令牌密钥轮换：旧 `key_id` 在有效期内可验，未知/过期拒绝。  
7. `recovery_incomplete = true` 时新接受返回不可用类结果，不假装最终拒绝。  
8. 时钟回拨（`now < max_observed_now_ms`）时停止新接受，已记录水位不回退。

不在本阶段验收：网关断连风暴、跨城切换、生产 P99/五个九统计。

## 11. 仍开放、但不得阻塞本阶段设计冻结的问题

下列问题**允许**在实现分支开头用一页补充决议拍板；在拍板前采用括号内默认：

| 开放项 | 本阶段默认 |
|---|---|
| 最终选用 PostgreSQL 还是其他关系库 | PostgreSQL 候选；确认前不动代码 |
| 消息正文保留多久 | 不少于同步/历史验收所需；可长于查询窗口 |
| 水位推进的生产调度周期 | 验收用显式推进；生产 cron/策略后置 |
| 是否引入持久在途租约 | **否** |
| 多会话并行写入的连接池与锁等待超时 | 实现稿给出可测超时 → `unresolved`，不改为最终拒绝 |

## 12. 文档关系

- 总方向：[`design.md`](./design.md)  
- 协议契约：[`protocol.md`](./protocol.md)  
- 本文：权威持久化路径  
- 下一步：评审冻结 §1–§7 与 §10 后，再开 `feat/storage` 做 DDL、适配与崩溃验收；库选型确认前不写代码  

若引入分片或 TiDB，另写演进设计；不得在实现中偷换本阶段关闭证明与事务边界。
