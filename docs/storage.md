# 持久化权威路径设计

本文承接 [`design.md`](./design.md) 与 [`protocol.md`](./protocol.md)，给出消息基本协议在权威持久化上的状态归属、提交边界、关闭证明、读取上界与恢复规则，作为存储适配、崩溃恢复测试和后续 DDL 实现的共同依据。

交互语义以 `protocol.md` 为准。本文不改写协议字段、错误码或窗口常数，只规定这些语义如何可靠落盘与跨重启成立。物理表名、索引 DDL 与最终 crate 名称在实现阶段确定；数据库产品在确认前不安装、不编写适配代码。当前实现仍以内存内核验证协议路径，持久化落地范围见 [实现说明](../README.md#当前实现与验收范围)。

## 范围与基本约定

本阶段覆盖单地域、单逻辑库上的权威写入与读取：幂等终态、会话消息与创建序、会话与账户变化事件、关闭水位、时钟观测上界、恢复完整性标志，以及游标与快照票据所需的密钥标识。好友操作、建群、已读上报、编辑撤回写接口，以及 TCP 网关的连接表、出站队列与慢连接治理，由对应模块设计。

发送成功表示权威路径完成约定范围内的原子持久提交，不表示设备收到或用户已读。推送可以丢失；丢失后必须仍能通过同步、快照与历史从持久记录发现并补齐。请求允许重复到达，由幂等键与会话提交边界保证至多一个终态。

同一逻辑库内，消息正文、创建顺序、变化事件、幂等终态与关闭水位由同一权威路径裁定。缓存、异步索引与推送缓冲可以滞后，不能独立改写或否定已提交事实。禁止以双写方式让另一套存储单独裁定提交结果。

本阶段不引入跨地域多活、会话分片迁移或 TiDB/TiKV。分片与分布式写入方的 fencing 在规模化阶段另文规定；在那之前，关闭水位采用单逻辑库全局一条单调上界。

## 持久性与进程内状态

跨进程重启后仍然有效的状态包括：

- 幂等终态：完整幂等键对应的 `committed` 或 `rejected_final`，含内容摘要、接受截止、查询截止，以及回执或拒绝码
- 会话消息：提交后的正文、创建位置、发送者与接受时间
- 会话变化事件与会话元数据：`history_id`、`access_version`、下一创建序与下一事件序、成员集合
- 账户变化事件与账户流元数据：会话发现所需的 upsert/remove 与流内序号
- 关闭水位 `close_watermark_ms`：按客户端消息时间覆盖范围单调前进的持久证明
- `max_observed_now_ms`：权威路径观测到的服务端时间上界，用于发现回拨
- `recovery_incomplete`：权威记录是否已证明完整可读
- 认证代次撤销（启用恢复令牌时）与令牌验证密钥标识

连接表、请求号水位、出站缓冲、推送编号、设备当前连接映射，以及同连接串行发送的在途标记，属于连接生命周期，默认不作为跨进程权威。进程崩溃后这些状态消失；未决发送通过查询与同键重试恢复。入口限流等进程内控制面重启后按新进程策略生效，不能改变已写入的终态。

本阶段没有好友与建群写接口。成员集合与账户会话 upsert 用于种子数据与发送时的成员校验；生产级成员变更、退群重入与拉黑时序由权限模块另定。权限或内容规则导致的窗口内拒绝，必须由权威路径写入 `rejected_final`，不能只停留在网关或进程内存。

## 逻辑记录

以下为逻辑集合与字段义务，不是物理 DDL。实现可以拆表或加索引，但不能删减义务字段，也不能把下列终态改成仅内存可见。

```text
IdempotencyRecord {
  tenant_id, user_id, device_id, conversation_id, client_msg_id
  outcome = oneof {
    committed {
      receipt_message_id: Id128
      create_seq: uint64
      history_id: Id128
      accepted_at_ms: TimestampMs
      content_digest: bytes  // 32
      accept_until_ms: TimestampMs
      query_until_ms: TimestampMs
    }
    rejected_final {
      error_code: ErrorCode
      content_digest: bytes  // 32
      accept_until_ms: TimestampMs
      query_until_ms: TimestampMs
    }
  }
}

Message {
  tenant_id, conversation_id, message_id
  create_seq: uint64
  history_id: Id128
  sender_user_id: UserId
  accepted_at_ms: TimestampMs
  state: MessageState
  content: MessageContent
  content_digest: bytes  // 32
}

Conversation {
  tenant_id, conversation_id
  kind: ConversationKind
  history_id: Id128
  access_version: uint64
  next_create_seq: uint64
  next_event_seq: uint64
  members: list<UserId>
}

ConversationEvent {
  tenant_id, conversation_id, event_seq
  change: Change
}

AccountStream {
  tenant_id, user_id
  epoch: Id128
  next_event_seq: uint64
}

AccountEvent {
  tenant_id, user_id, event_seq
  change: Change
}

CloseWatermark {
  close_watermark_ms: TimestampMs
}

StoreControl {
  max_observed_now_ms: TimestampMs
  recovery_incomplete: bool
}

AuthEpochRevocation {
  auth_epoch: Id128
  revoked: bool
}

TokenKeyring {
  key_id: bytes
  verify_material: bytes
  not_after_ms: TimestampMs
}
```

完整幂等键与 `protocol.md` 一致，不含连接身份与请求号。同一幂等键至多一条终态记录，`committed` 与 `rejected_final` 互斥。内容摘要规则与协议固定比较输入一致；未知内容类型不得静默写入。

`Message` 只在写入 `committed` 的同一事务中插入。同一 `history_id` 下 `(conversation_id, create_seq)` 唯一。`next_create_seq` 与 `next_event_seq` 表示下一个将分配的序号，从 1 开始；事件水位为 0 表示尚无变化。序号分配必须在会话提交边界内与对应事件、消息写入一同完成。

账户流只承载会话发现类变化；会话流承载消息变化。两者进度不能混用。本阶段账户事件至少覆盖会话对用户可见时的 `conversation_upsert`。

`CloseWatermark` 与 `StoreControl` 在本阶段各为单逻辑库一行。`close_watermark_ms` 只允许前进，清理幂等行后仍须保留。令牌验签只依赖 `TokenKeyring`，不依赖原网关内存；密钥轮换时旧 `key_id` 保留到其签发票据最晚过期之后。

权威读路径至少支持：幂等键点查；会话 `event_seq` 区间扫描；固定 `history_id` 下按 `create_seq` 向前历史分页；用户账户 `event_seq` 区间扫描。这些读取不能把最终一致的第二索引当作唯一依据。

## 发送提交

服务端在认证、字段与资源约束通过后，在会话提交边界处理幂等、业务规则与写入。消息内容、创建顺序、幂等结果及可供后续同步的事件依据必须原子提交，完成后才能返回 `committed`。推送失败不回滚已经成功的发送。

权威表只保存终态，不持久化“处理中”行。处理中的互斥由会话行上的事务串行化与连接内在途标记保证。尚未形成可恢复终态时返回 `IN_PROGRESS`，不能把仅在内存中检测到的冲突当作关闭证明。若将来引入跨进程持久在途租约，必须另行规定租约超时、fencing 与关闭水位的交集；本阶段默认不启用。

一次成功接受的发送，在同一权威事务中完成下列工作：

1. 锁定或等价串行化该会话提交边界，保证同会话写顺序。
2. 按幂等键读取终态。命中且摘要一致时返回原结果，不写入新消息；摘要不一致时返回 `IDEMPOTENCY_CONFLICT`，`acceptance_closed = true`，不覆盖原终态。
3. 检查恢复完整性、时钟回拨、接受窗口与业务规则。`recovery_incomplete` 为真，或服务端时间相对 `max_observed_now_ms` 回拨时，停止新接受并返回结果不可用类未决，不得假装最终拒绝。
4. 窗口内权限或内容规则不满足时，写入 `rejected_final` 并提交后返回；无法保存终态时返回 `unresolved`，不得伪装成最终失败。
5. 分配 `create_seq` 与 `event_seq`，写入消息、会话变化事件与 `committed` 终态，必要时推进 `max_observed_now_ms`。
6. 事务提交成功后，才向调用方返回 `committed`。

同键并发至多产生一个权威决定。事务提交前进程崩溃则回滚，键上无终态、无消息、无事件，同键可以再次进入接受路径。事务已提交但响应未写出时，重试与查询必须返回同一 `message_id` 与位置。串行发送屏障属于连接内行为，崩溃后消失；本地超时或入口错误不能产生关闭证明，也不能把未决写成最终拒绝。

## 发送查询

`SEND_STATUS` 只读取权威终态、关闭水位与控制面标志。不能用延迟副本的空结果证明消息不存在，也不能在接受窗口内把查空写成 `rejected_final`。

在结果查询期限内，命中终态且内容一致时返回原回执或原最终拒绝；内容不一致时返回 `IDEMPOTENCY_CONFLICT`，关闭标记为真。查询回执校验调用方对幂等作用域的身份，不返回消息正文。未命中终态时按下文窗口与关闭规则裁定，不得发明终态行。

## 接受窗口、关闭水位与清理

从 `client_msg_id` 的前 48 位取毫秒时间 `T`，截止时间与协议固定公式相同：

```text
允许提前量         = 120000 ms
accept_until_ms    = T + 86400000
query_until_ms     = T + 691200000
```

首次接受需满足 `T <= 服务端时间 + 允许提前量` 且 `服务端时间 < accept_until_ms`。该校验由权威提交路径执行。窗口内已经形成的有效提交决定，确认较晚到达时仍返回原回执；截止之后不能形成新的接受决定。

关闭水位 `W = close_watermark_ms` 表示：对所有满足 `T <= W` 的逻辑键，权威路径已取得“不能再形成新接受决定”的覆盖上界。判定某键是否已关闭接受，使用 `W >= accept_until_ms`。本阶段 `W` 为单逻辑库全局值，不按会话或用户分片。时钟越过截止本身不是关闭证明；水位不因重启、迁移或时钟回拨退回。

只有同时满足下列条件，才允许把 `W` 推进到更大的 `W'`：

1. `recovery_incomplete` 为假，当前权威记录完整性可证。
2. 所有可能在 `T <= W'` 范围内形成接受决定的写入方，要么已恢复到确定终态，要么已永久失去提交资格。本阶段为单进程权威路径且无跨进程在途租约时，该范围不存在未提交的持久决定即可封闭；出现多写方前必须先具备 fencing，否则禁止推进。
3. 更新必须只增：在权威库上以比较并交换或等价方式增大 `close_watermark_ms`，失败则重读后重试。

本阶段允许通过显式管理接口或后台任务推进水位以便验收。生产调度策略可以后置，但不能把仅测试开关重新定义为关闭证明。

未命中终态时的结果按下列规则裁定，先匹配先生效：

- 恢复不完整或时钟不可靠：返回 `RESULT_UNAVAILABLE`；关闭标记仅在已独立满足 `W >= accept_until_ms` 时可为真，否则为假。不能由查空推导从未提交。
- `T` 过度超前：返回 `CLOCK_SKEW`，关闭标记为假。
- 仍在接受窗口内：返回 `NOT_FOUND` 或处理中语义，关闭标记为假，可继续重试发送或查询。
- 已过接受截止但尚未 `W >= accept_until_ms`：返回 `IN_PROGRESS`，关闭标记为假。
- 已过接受截止、已取得关闭证明且完整性可证：返回 `rejected_final / REQUEST_EXPIRED`。该过期拒绝依据水位与完整权威记录，无需为每个从未出现的键再写拒绝行。
- 已过查询截止且已取得关闭证明：不再返回历史终态内容，返回 `RESULT_EXPIRED`，关闭标记为真。尚未取得关闭证明时继续返回处理中或结果不可用，且不得清理记录。

已提交终态至少保存完整幂等键、内容摘要、原回执与两个截止时间；最终拒绝记录保存键、内容摘要、原错误与截止时间。仅由关闭水位证明的过期拒绝不需要内容摘要。终态至少保留到 `query_until_ms`；消息正文保留期与之独立，但不能短到破坏本阶段同步与历史验收。

只有同时满足以下条件才能清理某时间范围的终态记录：服务端已达到该范围的查询截止；持久关闭水位已覆盖该范围的接受截止；旧写入方与未决操作已经封闭；清理后仍保留关闭水位与控制面记录，使该范围不能重新接受可继续证明。TTL 只能触发检查，不能代替关闭证明或直接删除。

进程启动后必须能读取 `CloseWatermark` 与 `StoreControl`。若缺失或无法证明完整可读，将 `recovery_incomplete` 置为真并拒绝新接受，直到修复完成。备份恢复与故障切换必须保留水位与清理边界；无法证明完整性时进入结果不可用，不能重新开放旧键，也不能把缺失结果裁定为最终拒绝。

## 同步、快照与历史读取

推送用于及时通知，同步用于发现和补齐遗漏。会话流与账户流都需要主动核对，不能只等待推送。

每一轮同步或快照的固定上界，取开启本轮时目标流上已提交且完整可读的最大事件序，或协议规定的等价观察点。分页过程中的新提交不进入本轮页，留给下一轮。`covered_through` 不超过上界；未达上界必须返回下一页令牌；`next_cursor` 只对应本页覆盖位置，不得直接跳到上界。

本阶段权威读默认走主库，或走能够证明无空洞、已覆盖到声明上界的同步副本。副本不足以证明完整覆盖时，不得返回残缺分页并签发“已覆盖”游标，应返回可重试错误或要求重置基线。

无游标的同步请求返回 `reset` 与快照入口，不得直接返回 `page`。游标验签失败、用途错误或身份目标不符按协议返回错误；验证通过但变化已清理或无法完整恢复时返回 `reset`，原因优先级与协议一致。快照的标识、上下文、上界与历史窗口在建立时固定，分页不得移动窗口。会话快照反映同一上界处的消息视图，不能把普通增量页拼接成假快照。历史分页在固定视图与保留下界内按创建序向前读取；上下文变化时作废旧票据。

游标、快照票据与分页令牌的完整性依赖 `TokenKeyring`。验签失败、到期或上下文变化按 `protocol.md` 的令牌规则处理，不回落到明文信任。

## 与现有模块的关系

```text
iam-protocol     编解码、校验与错误辅助，不因存储选型改契约
iam-core         命令编排与会话提交边界；MemoryStore 为易失后端
关系库适配       同一存储端口的持久后端，crate 名实现时确定
iam-loop         增加崩溃、重启回放、水位、清理与丢推后同步验收
```

存储端口至少暴露：会话边界内的发送提交；发送结果查询；关闭水位与控制面读写；按上界读取会话与账户事件页；按固定视图读取历史；测试所需的会话与账户种子写入。MemoryStore 与持久适配必须共享同一组语义测试；持久套件额外覆盖进程崩溃与重启。

## 选型边界

本阶段默认讨论候选为 PostgreSQL，以便用本地单一关系库验证事务与恢复。MySQL 或 MariaDB 在同等验收前提下可替换。SQLite 仅限单机原型，不作为规模方向假设。TiDB 与 TiKV 延后到需要水平扩展与多写方模型时再评估。

选型确认前不安装数据库、不编写适配代码。确认后另开实现分支给出 DDL、迁移与本地编排。存储能力不足时调整实现或提出协议修订，不在适配层偷改语义。

## 验收场景

存储适配至少验证下列场景；具体数据库部署与产品权限细则不在本文展开。

- 提交成功后结束进程，重启后同键重试得到同一 `message_id`、创建位置与历史代次
- 事务提交前结束进程，重启后同键可再次接受或按窗口查询为未决；不得出现有回执无事件，或无终态却被写成最终拒绝
- 正好到接受截止且窗口尚未闭合：`unresolved / IN_PROGRESS`；闭合且完整记录证明未提交：`rejected_final / REQUEST_EXPIRED`
- 正好到查询截止且已取得关闭证明：`unresolved / RESULT_EXPIRED`，关闭标记为真；清理前后旧键均不能重新提交
- 推送丢失后，仅依赖同步与快照仍能发现已提交消息
- 令牌密钥轮换后，旧 `key_id` 在有效期内仍可验证；未知或过期密钥拒绝
- `recovery_incomplete` 为真时，新接受返回结果不可用类未决，不假装最终拒绝
- 服务端时间相对已观测上界回拨时停止新接受，关闭水位不回退

不在本阶段验收：网关断连风暴、跨城切换，以及生产环境的 P99 与五个九统计。

## 文档关系与演进

- 总方向：[`design.md`](./design.md)
- 协议契约：[`protocol.md`](./protocol.md)
- 本文：权威持久化路径
- 实现说明：[README 当前实现与验收范围](../README.md#当前实现与验收范围)

实现开始前冻结本文的范围、事务边界与关闭证明定义，再开实现分支落实 DDL 与适配。若引入分片、多写方或 TiDB，另写演进设计，不在实现中偷换本阶段验收标准。
