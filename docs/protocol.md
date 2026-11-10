# 消息协议设计

本文承接 [`design.md`](./design.md)，给出基本协议的关键数据结构、请求与响应、状态变化和校验规则，作为编写 IDL、客户端 SDK、网关和协议测试的共同依据。

数据结构使用逻辑 IDL 表达，字段后的编号就是 Protobuf 字段号，枚举值与命令号分别固定。本文是字段、校验和交互行为的统一依据；调整先更新约定，再修改实现。数据库部署与产品权限细则由对应模块确定。

## 范围与基本约定

本协议覆盖认证、连接恢复、心跳、文本发送、发送结果查询、实时推送、变化同步、快照恢复和历史分页。好友操作、建群、已读上报和编辑撤回的写接口由对应模块设计；接收侧定义消息更新与移除，保证扩展不会破坏同步模型。

当前选择 TCP、TLS 和 Protobuf。发送成功表示服务端完成约定的持久提交，不表示设备收到或用户已读。请求与推送允许重复到达，由幂等、顺序和同步规则保证业务结果。

同一设备在同一会话内采用串行发送，推送不推进同步游标，压缩关闭。发送接受窗口、结果查询期限和终态清理规则在本文固定，普通重连不能改变这些边界。

## 数据类型约定

字段默认业务必填，`?` 表示可缺省，`list<T>` 表示列表，`oneof` 表示必须且只能选择一个分支。所有字段号在各自 message 内唯一；oneof 组本身不占字段号，其成员与同一 message 的其他字段共用编号空间。

IDL 使用 `syntax = "proto3"` 和 `package iam.protocol`。普通单值标量、字符串、bytes 和枚举统一使用 `optional` 保留存在性；message 字段使用原生存在性，列表使用 `repeated`，oneof 成员不加 `optional`。`Type = oneof` 映射为同名 message 内名为 `value` 的 oneof；显式命名的 `payload` 保持原名。固定头不使用 Protobuf。

解码后递归检查业务必填字段、非零标识、枚举合法值、字节长度、oneof 选择和跨字段约束。必填 bool 必须显式出现，`false` 是合法值；空列表与未编码列表等价，除明确要求非空的 `Push.changes` 外均允许为空。`?` 字段出现时仍须通过类型和取值校验。必填不等于非零，水位为 0、空列表和 `Empty` 都有明确用途。

生成的解码器可能合并重复字段或只保留最后一个 oneof 分支，因此解码流程还必须保留足以识别重复单值字段、多个分支和未知分支的线格式信息。重复单值字段或多个 oneof 分支，即使值相同，也按非法报文拒绝，不能依赖某种语言的覆盖行为。字段存在性与原生 oneof 行为参见 [Protobuf 规则](https://protobuf.dev/programming-guides/field_presence/)。

```text
TenantId       = uint64，非 0
UserId         = uint64，非 0
ConversationId = uint64，非 0
Id128          = bytes，恰好 16 字节，UUIDv7 格式
Token          = bytes，1 至 4096 字节，不透明，客户端只保存和回传
TimestampMs    = int64，Unix 毫秒，非负
Empty {}
```

类型别名映射到对应 Protobuf 底层类型，不增加消息包装；`Empty` 单独生成空 message。Rust 在业务边界用不同类型封装设备、连接、客户端消息、服务端消息和历史代次，避免互换。整数 ID 与序号不得经浮点数中转，日志使用十进制字符串。

### Id128 的生成与编码

所有 `Id128` 使用 [RFC 9562 定义的 UUIDv7](https://www.rfc-editor.org/rfc/rfc9562.html#section-5.7)：前 48 位为 Unix 毫秒时间，版本位为 7、variant 位为二进制 10，其余 74 位每次从系统密码学安全随机源独立生成，不使用进程号、普通伪随机数或自行拼接计数器。随机源不可用时生成失败，不降级。时间不用于会话排序。

设备 ID 在设备注册时生成并持久保存；连接 ID、服务端消息 ID、历史代次和快照 ID 由服务端生成。客户端在会话发送队列的队首准备首次提交时生成 `client_msg_id`，使用最近一次服务端时间校准本地时钟，并与内容一起持久保存后发送。离线草稿使用独立本地标识，重试不重新生成或修改 `client_msg_id`。

校验长度、UUID 版本位和 variant 位，拒绝 Nil、Max 和其他 UUID 格式。只有 `client_msg_id` 额外执行发送窗口校验；不能因设备 ID 或历史代次较旧而拒绝它。客户端检测到本地已使用的 ID 时，在首次发送前重新生成；已发送的键发生内容冲突时进入幂等冲突处理，不自动换键。

线上与存储使用 RFC 字节顺序的原始 16 字节，不使用平台特有的混合字节序或十六进制文本代替二进制字段。日志和诊断使用小写 `8-4-4-4-12` 十六进制格式，不带花括号或前缀。Token、正文和凭证不得随 ID 一起写入普通日志。

`message_id` 由服务端在首次成功提交时独立生成，不复制或计算自 `client_msg_id`。同一完整幂等键只映射到一个提交回执；重试返回原 `message_id`，最终拒绝不生成消息 ID。服务端在会话范围内校验消息 ID 唯一，提交前碰撞则重新生成；提交后的 ID 永不改写或复用。UUID 的时间相同不表示消息相同。

时间用于展示和接受窗口判断，创建序、变化序和访问版本从 1 开始，水位为 0 表示尚无变化；空历史窗口另有说明。序号使用 `uint64`，不回绕、不承诺数值连续，不能直接计算未读数。

## 帧与编解码

TCP 提供字节流，接收方需要处理半包和连续多帧。应用帧固定为以下三部分：

```text
FixedHeader + Head + Body

FixedHeader，共 12 字节，多字节整数使用大端
  version:  uint8     // 偏移 0，协议兼容标识，当前值为 1
  msg_type: uint8     // 偏移 1，命令编号
  flags:    uint16    // 偏移 2，保留标志位，当前必须为 0
  head_len: uint32    // 偏移 4，Head 的序列化字节数
  body_len: uint32    // 偏移 8，Body 的序列化字节数

Head {
  req_id: uint64? = 1
}
```

整帧长度为 `12 + head_len + body_len`。固定头读满后，先检查加法溢出和长度上限，再分配受限缓冲区读取剩余部分。数据暂未收齐时继续等待；长度非法、读取超时或连接结束时残帧未完成，则终止连接，不扫描猜测下一帧边界。

`Head` 只放请求关联信息。身份来自认证上下文，目标会话与逻辑消息标识只放在 `Body` 中，避免两份字段不一致。路由所需的会话信息从命令体读取，不把物理分片位置暴露给客户端。

需要应答的请求必须显式携带非 0 的 `req_id`，按实际写入连接的顺序严格递增，发送队列在确定写入顺序时分配请求号。响应可以乱序返回，但必须回显原请求号；重试使用新请求号，逻辑消息身份不变。`PUSH`、`PUSH_ACK` 和无法关联请求的连接错误使用 0，不更新请求号水位；只有这些场景允许 `head_len = 0`。

`Body` 根据 `msg_type` 解码为唯一指定结构。只有 `HEARTBEAT` 使用空消息体，其结构为 `HeartbeatRequest`，`body_len` 必须为 0。其他命令必须满足字段校验。线格式错误与业务错误分别按后文错误路由处理，不能仅凭解码成功就进入业务流程。

固定头的 `version` 用于校验帧与命令的兼容性，认证体不重复携带该信息。遇到不支持的版本或标志位时停止解码；可安全响应时返回协议错误，然后关闭连接。压缩扩展需要明确算法、压缩范围和解压后上限。

## 命令集合

命令编号、消息体及请求响应关系如下。未分配的编号保留，未知命令返回 `UNSUPPORTED_COMMAND`，不能作为业务成功处理。

```text
 1 AUTH          AuthRequest        ->  2 AUTH_ACK          AuthResponse
 3 HEARTBEAT     HeartbeatRequest            ->  4 HEARTBEAT_ACK     HeartbeatAck
 5 RECONNECT     ReconnectRequest   ->  6 RECONNECT_ACK     ReconnectResponse
 7 SEND          SendRequest        ->  8 SEND_ACK          SendResponse
 9 PUSH          Push              -> 10 PUSH_ACK          PushAck
11 SYNC          SyncRequest        -> 12 SYNC_ACK          SyncResponse
13 ERROR         ProtocolError     // 无应答
14 SEND_STATUS   SendStatusRequest  -> 15 SEND_STATUS_ACK   SendStatusResponse
16 SNAPSHOT      SnapshotRequest    -> 17 SNAPSHOT_ACK      SnapshotResponse
18 HISTORY       HistoryRequest     -> 19 HISTORY_ACK       HistoryResponse
```

`PUSH`、各类响应和 `ERROR` 由服务端发送，其余命令由客户端发送。服务端只在协商了 `push_ack` 能力后要求 `PUSH_ACK`，收悉本身不再应答，其余基本命令均需支持。

命令方向错误、请求号未递增或需要应答的请求号为 0，属于协议错误，返回错误后关闭连接。未认证的业务请求返回 `UNAUTHENTICATED`，不改变认证状态，也不构成该逻辑发送的最终拒绝。

业务响应使用 `oneof` 表达成功数据或错误，不能出现 `code = 0` 却缺少成功数据的情况。发送与发送查询使用专门的 `SendOutcome`，不能直接套用普通成功失败二分模型。

## 公共结构

### 错误与重试动作

```text
ErrorInfo {
  code: ErrorCode = 1
  action: RetryAction = 2
  retry_after_ms: uint32? = 3
  detail: string? = 4
}

RetryAction {
  RETRY_ACTION_UNSPECIFIED = 0
  NONE = 1
  RETRY_SAME_REQUEST = 2
  QUERY_SEND = 3
  REAUTHENTICATE = 4
  RESTART_SYNC = 5
  RETRY_SEND = 6
  RESUME_SYNC = 7
  RESTART_HISTORY = 8
}

ErrorCode {
  ERROR_CODE_UNSPECIFIED = 0
  INVALID_ARGUMENT = 1
  UNAUTHENTICATED = 2
  PERMISSION_DENIED = 3
  UNSUPPORTED_VERSION = 4
  UNSUPPORTED_COMMAND = 5
  UNSUPPORTED_CAPABILITY = 6
  MALFORMED_FRAME = 7
  RESOURCE_LIMIT = 8
  RATE_LIMITED = 9
  OVERLOADED = 10
  IDEMPOTENCY_CONFLICT = 11
  NOT_FOUND = 12
  IN_PROGRESS = 13
  RESULT_UNAVAILABLE = 14
  INVALID_CURSOR = 15
  PAGE_TOKEN_EXPIRED = 16
  PAGE_BUDGET_TOO_SMALL = 17
  INTERNAL_ERROR = 18
  CONTEXT_CHANGED = 19
  INVALID_TOKEN = 20
  REQUEST_EXPIRED = 21
  RESULT_EXPIRED = 22
  CLOCK_SKEW = 23
  UNSUPPORTED_CONTENT = 24
  SESSION_REPLACED = 25
  UNSUPPORTED_FLAGS = 26
}
```

每个枚举显式定义带类型前缀的零值，业务上无效；其他枚举值按本文固定，不使用数值别名。`detail` 仅用于诊断，不供客户端解析业务分支，不回传凭证或内部拓扑。`retry_after_ms` 是最小退避时间，客户端仍需增加抖动并遵守本地重试预算。

`RETRY_SAME_REQUEST` 表示重试当前操作，`RETRY_SEND` 专门表示重新提交本地保存的原 `SEND` 键与内容，`QUERY_SEND` 表示查询原发送。`RESTART_SYNC` 表示从无游标的同步请求重新建立基线，`RESUME_SYNC` 表示从最后落盘游标重新发起同步，`RESTART_HISTORY` 表示用原查询条件重新开始历史分页。`NONE` 表示不自动重试，不能据此判定发送失败；`REAUTHENTICATE` 表示重新执行 `AUTH`。

重试动作不能覆盖提交事实。限流、认证失败、过载等错误可能只拒绝当前尝试，不能据此认定同一逻辑发送此前没有提交。发送结果未知时，客户端保留原消息并通过重新认证、退避、查询或同标识重试恢复。

### 消息身份与内容

```text
MessageKey {
  conversation_id: ConversationId = 1
  client_msg_id: Id128 = 2
}

MessageRef {
  conversation_id: ConversationId = 1
  message_id: Id128 = 2
}

MessagePosition {
  history_id: Id128 = 1
  create_seq: uint64 = 2
}

MessageContent = oneof {
  text: TextContent = 1
}

TextContent {
  text: string = 1
}

MessageView {
  ref: MessageRef = 1
  position: MessagePosition = 2
  revision_seq: uint64 = 3
  sender_id: UserId = 4
  accepted_at_ms: TimestampMs = 5
  state: MessageState = 6
  content: MessageContent? = 7
}

MessageState {
  MESSAGE_STATE_UNSPECIFIED = 0
  ACTIVE = 1
  RECALLED = 2
  DELETED = 3
}
```

完整幂等键为认证上下文中的 `(tenant_id, user_id, device_id)` 加 `MessageKey`。连接身份和 `req_id` 不参与去重。客户端不能通过填入其他发送者身份代发消息。

`message_id` 是提交后稳定的消息身份。`history_id` 标识一段可连续解释的会话历史，计划迁移应保持它；无法保持历史连续性时更换代次并显式要求恢复。不同历史代次的序号不能直接比较。

`create_seq` 取消息创建事件在该会话历史中的序号，`revision_seq` 取最近一次消息变化的事件序号。创建时两者相等，编辑或撤回只更新后者。消息按创建位置展示，不能因编辑被移动到末尾。

`revision_seq` 不得小于 `create_seq`。`accepted_at_ms` 是首次持久提交决定对应的服务端时间，不是网关收包或重试时间。回执证明原提交事实，其位置不用于覆盖设备当前的历史代次，也不证明灾难恢复后的历史仍然连续。

`ACTIVE` 必须有内容，撤回和删除状态不得带正文。完全无权获知的消息不返回其身份或占位内容，使用同步过滤与访问范围变更处理。相同历史代次内，客户端不能用较低版本覆盖较高版本。

文本必须是非空 UTF-8 字符串，长度按 UTF-8 字节计。幂等比较使用已定义的内容字段和值，不做隐式裁剪或 Unicode 归一化；增加内容类型时同时扩展比较规则。不能直接将 Protobuf 序列化字节当作稳定指纹，参见 [Protobuf 非规范化序列化说明](https://protobuf.dev/programming-guides/serialization-not-canonical/)。

### 会话发现与变化流

```text
ConversationState {
  conversation_id: ConversationId = 1
  kind: ConversationKind = 2
  history_id: Id128 = 3
  access_version: uint64 = 4
}

ConversationKind {
  CONVERSATION_KIND_UNSPECIFIED = 0
  DIRECT = 1
  GROUP = 2
}

SyncTarget = oneof {
  account: Empty = 1
  conversation: ConversationId = 2
}

StreamContext {
  target: SyncTarget = 1
  epoch: Id128 = 2
  access_version: uint64? = 3
}

Change {
  event_seq: uint64 = 1
  payload: oneof {
    conversation_upsert: ConversationState = 2
    conversation_remove: ConversationId = 3
    message_upsert: MessageView = 4
    message_remove: MessageRef = 5
  }
}

PageLimit {
  max_items: uint32 = 1
  max_bytes: uint32 = 2
}
```

`account` 的用户由认证上下文确定，不允许指定其他用户。账户变化流负责发现会话集合和访问范围变化，使用自己的历史代次与序号；会话变化流负责消息内容，两者的进度不能混用。

账户流只使用会话变化分支，会话流只使用消息变化分支。同一流、同一代次的 `event_seq` 唯一定位一次变化。会话流的 `epoch` 等于 `history_id`，`access_version` 必填；账户流不填写该字段。访问范围发生会影响缓存或历史解释的变化时，服务端更新对应用户的 `access_version`。

会话流中的消息必须属于上下文指定的会话与历史代次，`message_upsert.revision_seq` 等于该变化的 `event_seq`。`message_remove` 表示移除设备可见视图，客户端保留相应版本依据，防止迟到的旧推送重新显示该消息。

`conversation_remove` 要求停止该会话同步并使缓存不可见。`conversation_upsert` 中历史代次或访问版本改变时，先使旧缓存失效，再建立新基线。它不表示允许用新权限直接解释旧缓存。

账户变化流是交互契约，不要求每条群消息都写一份逐成员事件。实现可结合会话集合变更、共享状态与读取时核对；已知会话仍需主动同步，不能只等待账户流通知每次聊天变化。

## 认证与连接恢复接口

```text
AuthRequest {
  access_token: Token = 1
  device_id: Id128 = 2
  requested_capabilities: list<string> = 3
}

ReconnectRequest {
  resume_token: Token = 1
  device_id: Id128 = 2
  previous_connection_id: Id128? = 3
  requested_capabilities: list<string> = 4
}

Session {
  tenant_id: TenantId = 1
  user_id: UserId = 2
  device_id: Id128 = 3
  connection_id: Id128 = 4
  resume_token: Token? = 5
  enabled_capabilities: list<string> = 6
  server_time_ms: TimestampMs = 7
  limits: ProtocolLimits = 8
}

AuthResponse = oneof {
  session: Session = 1
  error: ErrorInfo = 2
}

ReconnectResponse = oneof {
  session: Session = 1
  error: ErrorInfo = 2
}

HeartbeatRequest {}

HeartbeatAck {
  server_time_ms: TimestampMs = 1
}
```

TLS 建立后，连接处于未认证状态，只接受 `AUTH` 或 `RECONNECT`。认证中或已认证的连接再次收到这两个命令时，返回 `ERROR / INVALID_ARGUMENT / NONE`，`close_connection = false`，原认证过程或身份不变。认证成功后才能发送心跳和业务命令，认证失败返回未认证状态。需要重新认证时建立新连接，不在活跃连接上切换身份。

`access_token` 绑定用户和租户，设备身份需通过服务端校验。`resume_token` 必须校验有效期、身份绑定和撤销状态，`previous_connection_id` 仅是关联信息，不是凭证。恢复凭证无效时重新走 `AUTH`，不能因旧连接不存在而无法恢复业务。

同一设备只有一个活跃连接，不同设备独立登录。新连接接管后，旧连接不再接受新业务请求；已经进入核心处理的发送仍可能完成，客户端必须查询或重试原逻辑消息。重新认证不清空待发送队列，也不更换 `client_msg_id`。

成功返回的 `Session.device_id` 必须等于认证请求中的设备 ID，租户和用户来自已验证的凭证，`connection_id` 为新连接独立分配，不能沿用 `previous_connection_id`。

心跳请求无消息体，服务端必须回复 `HEARTBEAT_ACK`，并回显 `req_id`。客户端按协商间隔发送心跳，双方以最近收到的有效帧计算空闲超时，计时使用本地单调时钟。心跳只判断连接活性，不证明消息已送达或同步已完成。

## 能力协商

能力名称区分大小写，必须匹配 `[a-z][a-z0-9_]{0,31}`。列表最多 32 项，不允许重复；空列表表示只使用必备协议功能。`requested_capabilities` 是客户端支持且愿意启用的可选能力集合，`enabled_capabilities` 是服务端允许启用的子集，按名称升序返回。当前能力名只有：

- `push_ack`：客户端可以对有效推送返回收悉，服务端可维护有界的推送收悉状态。

语法合法但未知的请求能力忽略，不出现在启用集合中，也不导致认证失败；格式非法、重复或超过上限通过认证响应的 `error / INVALID_ARGUMENT / NONE` 返回。客户端若收到自己未请求、不认识、重复或未按约定排序的启用能力，视为服务端协议错误并关闭连接。

每次 `AUTH` 或 `RECONNECT` 都重新协商，连接建立后集合不变；恢复令牌不继承旧连接的启用集合。必备命令、文本内容及本文定义的变化类型不需要能力名。

启用 `push_ack` 时，客户端对完整验证成功的每批 Push 返回 `PushAck`；重复的同一推送可重复收悉，但不能重复应用变化。未启用时服务端仍正常发送 Push，客户端不得发送 PushAck，服务端不等待收悉，也不按未收悉发起重传。未协商却收到 PushAck 时返回 `ERROR / UNSUPPORTED_CAPABILITY`，`close_connection = false`，不改变任何业务进度。

## 不透明令牌

### 服务端共同义务

Token 由服务端签发和验证，客户端只能原样保存与回传。除外部认证凭证 `access_token` 外，票据内容必须通过成熟的认证加密机制保护，覆盖类型、用途、签发方、密钥标识、签发与到期时间、身份、位置和访问范围；不得使用可修改的裸编码或只校验部分字段。加密 nonce 在同一密钥下不可复用，解密成功后仍需校验用途和当前权限。

普通游标与分页票据绑定 `(tenant_id, user_id, device_id)`，不绑定连接 ID、网关或物理分片。跨连接与正常迁移继续可用；不同身份、不同目标或不同用途的票据不可混用。恢复令牌也绑定相同身份，并绑定可撤销的认证代次。凭证校验不依赖原网关内存。

密钥轮换需保留仍在有效期内票据的验证能力；因撤销或灾难无法验证时，按下列错误规则处理，不能重新解释为无令牌请求。分页依赖的快照、扫描位置或读取边界可由共享可靠状态或可重建状态承载，丢失时明确失效，不伪造分页成功。任何令牌都不授权绕过当前权限。

有效票据允许重复使用，位置是不可变的请求输入，不能把服务端迭代器消费一次就前移作为其语义。重复读取同一范围不推进设备已完成进度；只有客户端完整应用并持久保存返回游标后，该设备的本地进度才前进。

### 令牌内容与期限

- `access_token`：验证签发方、IM 接入用途、租户、用户、允许使用的设备、到期时间和撤销状态。有效期由认证签发方提供，不能以连接存活为由延长。
- `resume_token`：绑定认证身份、认证代次、签发时间和到期时间，有效期不超过签发后 7 天与认证授权允许恢复的截止时间两者中的较早值。它只恢复认证资格，新连接仍重新分配连接 ID、接管连接并协商能力。签发新恢复令牌不自动撤销尚未过期的同代次令牌，登出或凭证撤销则使对应认证代次失效。
- 同步 `cursor`：绑定身份、`SyncTarget`、`epoch`、会话的 `access_version`、完整覆盖位置和过滤规则，有效期为签发后 30 天。该期限不延长变化记录保留期；记录已不可完整恢复时必须要求快照重建。每个成功同步页签发对应覆盖位置的新游标。
- 同步 `page_token`：除游标绑定项外，绑定本轮扫描标识、固定 `upper_bound`、已扫描位置与过滤规则，有效期为本轮开始后 15 分钟，各页不能滚动延期。
- `snapshot_token` 与快照 `page_token`：使用同一快照票据类型，绑定身份、目标、快照 ID、上下文、固定上界、历史窗口及已输出位置。有效期为快照建立后 15 分钟，后续页沿用同一截止时间。
- 历史 `page_token`：绑定身份、会话上下文、首次查询的排他创建序、固定上界、保留下界和已扫描位置，有效期为本轮历史读取开始后 15 分钟。下一页可以调整页大小，不能改变查询条件或上界。

所有截止时间均为排他边界，服务端时间达到截止即失效；续页不延长原轮次生命周期。`max_items` 和 `max_bytes` 可以在服务端上限内调整，不影响固定读取范围。票据的内部编码不对客户端开放，也不作为日志字段。

### 失效结果

认证或恢复凭证无效、过期、身份不符或已撤销，通过对应认证响应返回 `UNAUTHENTICATED / REAUTHENTICATE`，连接保持未认证。已认证连接的认证资格被撤销，则使用 `ERROR / UNAUTHENTICATED` 并关闭连接。

游标验签失败、用途错误或身份目标不符，返回 `SyncResponse.error = INVALID_CURSOR / RESTART_SYNC`。验证通过但变化已被清理或游标到期，返回 `SyncResponse.reset / CURSOR_EXPIRED`；历史代次变化为 `HISTORY_RESET`，仍有访问权但访问范围变化为 `ACCESS_CHANGED`。多个条件同时存在时，按历史代次、访问范围、恢复期限的顺序选择原因。

同步分页令牌无效返回 `SyncResponse.error = INVALID_TOKEN / RESUME_SYNC`；到期或扫描状态不可恢复返回 `PAGE_TOKEN_EXPIRED / RESUME_SYNC`。可验证的历史或访问上下文变化仍返回 `reset`，不继续旧轮次。

快照票据无效返回 `SnapshotResponse.error = INVALID_TOKEN / RESTART_SYNC`；到期或快照资源不可恢复为 `PAGE_TOKEN_EXPIRED / RESTART_SYNC`；可验证的上下文变化为 `CONTEXT_CHANGED / RESTART_SYNC`。

历史票据无效返回 `HistoryResponse.error = INVALID_TOKEN / RESTART_HISTORY`；到期、固定读取视图或保留下界不可维持时为 `PAGE_TOKEN_EXPIRED / RESTART_HISTORY`；上下文变化为 `CONTEXT_CHANGED / RESTART_SYNC`。

任何读取操作已无访问权时返回对应业务响应的 `PERMISSION_DENIED / NONE`，不返回快照票据或受限内容，客户端使该范围缓存不可见。验证服务暂不可用属于 `INTERNAL_ERROR / RETRY_SAME_REQUEST`，不能伪装成令牌过期。以上业务失效均不关闭正常连接。

## 发送与结果查询接口

```text
SendRequest {
  key: MessageKey = 1
  content: MessageContent = 2
}

SendReceipt {
  ref: MessageRef = 1
  position: MessagePosition = 2
  accepted_at_ms: TimestampMs = 3
}

SendOutcome = oneof {
  committed: SendReceipt = 1
  rejected_final: ErrorInfo = 2
  unresolved: SendUnresolved = 3
}

SendUnresolved {
  error: ErrorInfo = 1
  acceptance_closed: bool = 2
}

SendResponse {
  key: MessageKey = 1
  outcome: SendOutcome = 2
}

SendStatusRequest {
  key: MessageKey = 1
}

SendStatusResponse {
  key: MessageKey = 1
  outcome: SendOutcome = 2
}
```

### 服务端处理

服务端先校验认证、字段和资源约束，再在会话的提交边界中处理幂等、业务权限和消息写入。消息内容、创建顺序、幂等结果及可靠分发依据必须原子提交，完成后才能返回 `committed`。发送与查询响应的 `key` 必须逐字段等于该请求号对应的原请求键，回执会话也必须一致；推送失败不回滚已经成功的发送。

同键并发只能产生一个权威决定，处理中请求的内容不能被后到请求替换。尚未形成可恢复终态时返回 `IN_PROGRESS`，不能把仅在内存中检测到的冲突当作已关闭接受的证明。

在结果查询期限内，同一键命中终态且内容一致时返回原回执或最终拒绝。内容不一致时返回 `unresolved.error = IDEMPOTENCY_CONFLICT / QUERY_SEND`，不覆盖原内容或终态。该冲突由权威记录确认，`acceptance_closed = true`，该键不可能再产生另一条消息；客户端仍需查询原发送，不自动换键重发。

文本比较依据固定为内容分支号的大端 uint32、文本 UTF-8 字节长度的大端 uint32、原始 UTF-8 字节拼接后的 SHA-256 摘要。内容分支号使用 `MessageContent` 的字段号；不加入连接、请求号或重试次数，不裁剪或归一化文本。终态保留该摘要及比较规则标识，正文删除不影响重复判定。新增内容类型必须先定义其规范输入，未知写入内容不得静默忽略。

`rejected_final` 表示该逻辑发送确定没有提交，且以后也不能再用该键提交，重试动作为 `NONE`。接受窗口内的权限或内容规则拒绝，需要可靠保存键、内容比较依据和拒绝终态，并阻止并发或迟到请求提交；最终拒绝不能仅由网关判断。窗口关闭后还可按下文的关闭证明裁定过期拒绝。

`unresolved.error` 说明未获得确定结果的原因，`acceptance_closed` 单独说明该键是否已被权威路径证明不能再提交。`false` 表示未取得关闭证明，不能解释为当前仍允许提交；`true` 也不表示从未提交。入口错误不能产生关闭证明。

无法确定提交结果时返回 `unresolved`。单凭查无记录、仍在处理、内部超时或恢复期间结果不可用，都不能裁定最终拒绝；过期拒绝必须同时具备关闭证明与完整权威记录。未通过认证、无法解析键或入口过载时可返回 `ERROR`，其含义仅限当前尝试，不改变已知的逻辑发送状态。

查询必须读取权威提交或拒绝结果，不能用延迟索引的空结果证明消息不存在。接受窗口内、可正常提交但暂未找到记录时，返回 `unresolved.error = NOT_FOUND / RETRY_SEND`，`acceptance_closed = false`；仍在处理时使用 `IN_PROGRESS / QUERY_SEND`。过期与恢复故障按下文窗口规则处理。查询回执校验调用方对幂等作用域的身份，不返回消息正文。

### 客户端处理

客户端在发送前持久保存键与内容。同一设备、同一会话只允许一条仍可能提交的逻辑发送，同一消息可以查询或重试，多个业务线程共用该串行队列。只有保存 `committed`、`rejected_final` 或权威返回的 `acceptance_closed = true` 后，才能释放该消息的提交屏障并处理下一条；本地超时、取消等待和入口错误均不能释放屏障。

收到 `committed` 保存回执，收到 `rejected_final` 保存最终失败。收到 `unresolved` 且未关闭接受时继续保留待确认状态；接受已关闭但历史结果未知时，保留“结果未知”的记录并停止自动发送该键，可以处理下一条，但不能将未知结果显示成发送失败或自动换键重发。任何迟到的未知结果或入口错误都不能降级本地已保存的终态，发送回执也不推进同步游标。

发生内容冲突时保留冲突标记。查询回执只能定位原键对应的消息，不证明本次冲突正文已提交；客户端按消息引用读取权威视图核对，不能将冲突正文直接标记为发送成功。

业务上自行取消等待不等于服务端取消提交。本协议未定义取消在途发送的接口，同一设备在同一会话内遵循串行发送规则。取消操作和并行提交需要明确的接口与顺序约束，不能在 SDK 中隐式实现。

### 接受窗口与结果查询期限

从 `client_msg_id` 的前 48 位取毫秒时间 `T`，固定以下边界，客户端不另行提交可修改的过期字段：

```text
允许提前量         = 120000 ms       // 2 分钟
accept_until_ms    = T + 86400000    // T 后 24 小时
query_until_ms     = T + 691200000   // 接受截止后再保留 7 天
```

首次接受需满足 `T <= 服务端时间 + 允许提前量` 且 `服务端时间 < accept_until_ms`。该校验由权威提交路径执行，网关收包或入队不代表已经接受。窗口内形成有效提交决定、但确认较晚到达的消息仍返回原回执；截止之后不能形成新的提交决定。接受和窗口关闭在同一权威顺序中裁定，不依赖客户端时间或不同网关各自判断。

结果查询期限内先查已有终态，已提交或已最终拒绝的键仍返回原结果，不受接受窗口已关闭影响。未命中终态且 ID 时间过度超前时，返回 `unresolved.error = CLOCK_SKEW / NONE`，`acceptance_closed = false`；SDK 校准时间并报告问题，不修改已发送的 ID，也不假定该键永远不会被接受。

接受截止后，权威提交路径必须等待此前可能成功的决定完成恢复，或使旧写入方和未决决定永久失去提交资格，再推进持久化的窗口关闭水位。水位按客户端消息时间覆盖范围单调前进，清理后仍须保留，不因重启、迁移或时钟回拨退回。时钟超过截止本身不是关闭证明。

在查询期限内，窗口已关闭，且完整权威记录证明该键既未提交、也不存在仍可能提交的在途操作时，返回 `rejected_final = REQUEST_EXPIRED / NONE`。这里的最终拒绝依据是持久关闭水位与完整权威记录，无需为每个从未出现的过期键再写一条拒绝记录。若有已保存终态，仍优先返回它。

窗口尚不能证明关闭时返回 `unresolved.error = IN_PROGRESS / QUERY_SEND`，关闭标记为 `false`。故障恢复期间无法确认记录是否完整时使用 `RESULT_UNAVAILABLE / QUERY_SEND`，仅在已独立取得关闭证明时将关闭标记设为 `true`，不能由查无记录推导从未提交。

到达 `query_until_ms` 后，协议统一不再返回过期键的历史终态，即使物理记录尚未清理。取得关闭证明后返回 `unresolved.error = RESULT_EXPIRED / NONE`，`acceptance_closed = true`，表示历史结果已不再可查询，但该键绝不会再次提交；尚未取得关闭证明则继续返回处理中或结果不可用，且不得清理记录。

这些窗口是固定协议约束，不能作为运行参数随意放宽并重新打开旧键。服务端时间不可靠、回拨或无法确认窗口状态时停止新接受，返回结果不可用；已记录的关闭边界继续有效。改写 UUID 时间意味着另一个逻辑键，SDK 不得用这种方式绕过原消息的结果恢复。

### 终态保留与清理

已提交记录至少保存完整幂等键、内容摘要、原回执和两个固定截止时间；最终拒绝记录保存键、内容摘要、原错误和截止时间。仅由关闭水位证明的过期拒绝不需要内容摘要。终态至少保留到 `query_until_ms`，正文保留期与之独立。

只有同时满足以下条件才能清理某时间范围的终态记录：服务端已达到该范围的查询截止；持久关闭水位已覆盖该范围；旧写入方与未决操作已经封闭；恢复记录能够继续证明该范围不能重新接受。TTL 只能触发检查，不能代替关闭证明或直接删除记录。

备份恢复、迁移和故障切换必须保留关闭水位及清理边界。无法证明恢复完整性时进入结果不可用状态，不能重新开放旧键或把缺失结果裁定为最终拒绝。消费者的重放边界与消息正文归档各自保存，不以去重记录清理为由改变发送结果或重复生成消息。

## 推送与收悉接口

```text
Push {
  push_id: uint64 = 1
  context: StreamContext = 2
  changes: list<Change> = 3
}

PushAck {
  push_id: uint64 = 1
}
```

`push_id` 在单条连接内非 0 且不分配给不同推送，同一实例重传保持原 ID 和内容；连接改变后不能确认旧推送。推送必须包含至少一项已提交变化，可重复、乱序或丢失。未知或已淘汰的推送 ID 收悉直接忽略，不影响业务进度。

客户端检查目标与上下文，按消息身份和变化版本幂等应用，不能按到达先后覆盖状态。同一批推送中的变化按 `event_seq` 升序排列，但不同批次之间不保证连续。

`PUSH_ACK` 只表示本设备收悉推送。推送与收悉都不推进同步游标，不代替用户已读；客户端通过批量同步确认完整覆盖。实时内容可以按创建位置更新视图，不能把较晚推送的到达认定为此前变化已全部收到。

网关可以在缓冲区超限时丢弃推送或关闭慢连接，后续依赖同步恢复。未发送或未确认的推送不要求在网关内无限保存。

## 变化同步接口

```text
SyncRequest {
  target: SyncTarget = 1
  cursor: Token? = 2
  page_token: Token? = 3
  limit: PageLimit = 4
}

SyncPage {
  context: StreamContext = 1
  changes: list<Change> = 2
  covered_through: uint64 = 3
  upper_bound: uint64 = 4
  next_cursor: Token = 5
  next_page_token: Token? = 6
}

ResetRequired {
  reason: ResetReason = 1
  snapshot_token: Token = 2
  data_loss_possible: bool = 3
}

ResetReason {
  RESET_REASON_UNSPECIFIED = 0
  INITIAL_SYNC = 1
  CURSOR_EXPIRED = 2
  ACCESS_CHANGED = 3
  HISTORY_RESET = 4
}

SyncResponse = oneof {
  page: SyncPage = 1
  reset: ResetRequired = 2
  error: ErrorInfo = 3
}
```

`cursor` 表示上次已完整应用的范围，`page_token` 用于继续本轮固定上界的分页，两者不能同时填写。首次请求都不填写，服务端返回 `reset` 和快照入口。每轮结束后用最后保存的 `next_cursor` 发起下一轮。

游标和分页令牌是不透明凭据，需要由服务端绑定认证身份、同步目标、历史代次、访问范围及位置，并校验完整性，但不绑定连接身份，普通重连仍可继续使用。客户端不得自行拼接、比较字节大小或从最大消息序号构造游标。令牌不能替代当前权限校验，也不包含可由客户端指定的内部路由。

服务端在一轮同步开始时固定 `upper_bound`，后续页沿用该边界。上界必须指向已经提交且能完整读取的变化范围，不能使用已分配但尚未发布的最大序号。并发产生的新变化留到下一轮。

每页返回调用方可见的变化，按 `event_seq` 严格升序，位于请求游标位置到 `covered_through` 的左开右闭区间。该值声明这段范围已经完整扫描；过滤掉不可见内容后，变化列表可以为空或不连续。客户端不能将空页直接理解为到达最新，也不能推断隐藏内容。

`covered_through` 不超过 `upper_bound`。小于上界时必须返回 `next_page_token`，达到上界时不返回，表示赶上本轮观察点。`next_cursor` 只对应本页覆盖位置，不得直接跳到上界。只要还有待覆盖范围，成功分页必须推进覆盖位置，不能不断返回不前进的空页。

客户端将变化应用、移除记录和 `next_cursor` 原子保存，再请求下一页。崩溃后从最后落盘游标继续，重复页不能重复创建消息或回退状态。未知的必要变化不能跳过后保存游标，需停止该范围同步并进行能力处理。

每个设备对同一同步目标串行执行同步或快照任务，重启任务后忽略旧任务的迟到响应，避免覆盖较新的游标。与当前历史或访问上下文不匹配的数据不能重新激活旧缓存，需要先核对并恢复对应上下文。

有效但过旧的游标、访问范围变化或历史连续性失效返回 `reset`。伪造、错目标或错身份的游标返回错误，不沿用其中的进度。分页令牌过期后，客户端使用最后落盘游标重新开始一轮；若游标也失效，再进入快照恢复。

账户流与已知会话都需要主动核对，重连时也要恢复。检查可以批量调度和合并，不能仅在收到新推送时触发，否则新会话首次推送或最后一次推送丢失会永久隐藏变化。

## 快照恢复接口

```text
SnapshotRequest {
  page_token: Token = 1
  limit: PageLimit = 2
}

HistoryWindow {
  from_create_seq: uint64 = 1
  through_create_seq: uint64 = 2
}

SnapshotItem = oneof {
  conversation: ConversationState = 1
  message: MessageView = 2
}

SnapshotPage {
  snapshot_id: Id128 = 1
  context: StreamContext = 2
  upper_bound: uint64 = 3
  history_window: HistoryWindow? = 4
  items: list<SnapshotItem> = 5
  next_page_token: Token? = 6
  resume_cursor: Token? = 7
}

SnapshotResponse = oneof {
  page: SnapshotPage = 1
  error: ErrorInfo = 2
}
```

第一次使用 `ResetRequired.snapshot_token`，后续使用 `next_page_token`。一次快照的 `snapshot_id`、上下文和上界固定；有下一页时只返回分页令牌，最后一页只返回 `resume_cursor`。令牌过期或访问范围改变时，本次快照作废，客户端重新从无游标的 `SYNC` 开始。

账户快照完整枚举当前可见会话集合，只使用 `conversation` 分支，`history_window` 必须缺省。会话快照只使用 `message` 分支，返回固定近期窗口内完整的当前消息视图，`history_window` 必填；非空窗口满足 `0 < from_create_seq <= through_create_seq <= upper_bound`，空窗口两端均为 0。窗口大小由服务端预算决定，不能在分页过程中移动，较早历史由 `HISTORY` 按需读取。

会话快照的内容反映同一上界处的消息状态，`revision_seq` 不超过上界，`resume_cursor` 从该上界衔接后续变化。实现需要提供一致快照或等价的重建能力，不能将普通分页结果拼接成假快照。

客户端将快照写入暂存区，完整接收后再原子切换基线与 `resume_cursor`。账户快照替换旧会话集合；会话快照使原服务端消息缓存失效，再装载新窗口，窗口外历史重新按需读取，不能保留可能已经失权或撤回的旧缓存。尚未确认的本地发送记录独立保存，不随快照清空。

恢复期间，无法按新上下文验证的旧视图保持不可见；推送可以暂存，快照完成后由增量同步补齐。快照完成只表示当前状态基线建立，不表示全部历史已经下载。

历史回退时，`data_loss_possible` 必须如实表达无法证明连续恢复的风险。重新建立基线不等于恢复已丢失的消息或发送结果；相关状态需要保留给上层处理，不能转成普通同步成功后消失。

## 历史读取接口

```text
HistoryRequest {
  context: StreamContext = 1
  before_create_seq: uint64? = 2
  page_token: Token? = 3
  limit: PageLimit = 4
}

HistoryPage {
  context: StreamContext = 1
  upper_bound: uint64 = 2
  retention_floor_seq: uint64 = 3
  messages: list<MessageView> = 4
  next_page_token: Token? = 5
}

HistoryResponse = oneof {
  page: HistoryPage = 1
  error: ErrorInfo = 2
}
```

历史读取仅接受会话上下文。首次可以指定非 0、排他的 `before_create_seq`，不填表示从当前最新位置向前读取；继续分页保留相同 `context`，只使用 `page_token`，不能同时填写 `before_create_seq`。返回消息的创建序严格降序且低于该排他边界，`revision_seq` 不超过固定上界。客户端展示时再按创建顺序排列。

一轮历史分页固定读取上界和权限上下文，每页重新检查当前权限。`retention_floor_seq` 是从 1 起的历史保留下界，返回消息的创建序不得低于它，也不代表调用方可以看到范围内全部内容。过滤后可返回空页，但有下一页时内部扫描位置必须前进。没有下一页表示本轮可读范围结束，不表示这些消息从未存在或该会话没有新变化。

历史读取不推进同步游标。返回的消息按历史代次和版本合并，不能覆盖已知较新的编辑、移除或撤回状态。历史代次或访问版本失效时停止分页，重新建立会话基线。

## 错误路由与连接处理

```text
ProtocolError {
  ref_msg_type: uint32? = 1
  error: ErrorInfo = 2
  close_connection: bool = 3
}
```

`ERROR` 使用 `Head.req_id` 关联当前尝试，消息体不重复请求号。无法信任或无法关联请求号时使用 0；`ref_msg_type` 若存在，保存实际收到的命令字节值，包括未知命令号。一个请求最多发送一个结果包，不能先回 `ERROR` 又回业务响应；查询另一请求取得终态不受此限制。校验顺序固定为帧与关联、命令识别、连接状态、已协商能力、字段结构、当前授权与令牌、业务处理，出现多个问题时返回最先遇到的错误。

### 直接关闭或返回 ERROR

- TLS 失败、固定头未读完即断开、帧长度非法或超限、读取超时：直接关闭，不按不可信长度继续读取，不承诺错误包。
- 完整固定头中的版本不支持或标志位非 0：尽力返回 `ERROR / UNSUPPORTED_VERSION` 或 `UNSUPPORTED_FLAGS`，请求号为 0，`close_connection = true`，不解析剩余消息体。
- 完整帧存在非法 Protobuf 线格式、重复单值字段、多个 oneof 分支、非法 UTF-8、整数溢出、错误 wire type 或超出解码资源预算：尽力返回 `ERROR / MALFORMED_FRAME`，`close_connection = true`。缺少可信请求号则使用 0。
- 命令方向错误、请求号为 0 或未递增：返回 `ERROR / INVALID_ARGUMENT`，`close_connection = true`。`PUSH`、`PUSH_ACK` 和连接级错误的零请求号按其专门规则处理。
- 完整且边界可信的未知命令：返回 `ERROR / UNSUPPORTED_COMMAND / NONE`，`close_connection = false`。未协商的 PushAck 返回 `UNSUPPORTED_CAPABILITY / NONE`，同样不关闭。
- 未认证或认证中的业务请求：返回 `ERROR / UNAUTHENTICATED / REAUTHENTICATE`，`close_connection = false`。已认证连接被撤销或被同设备新连接接管时，返回连接级 `ERROR / UNAUTHENTICATED` 或 `SESSION_REPLACED`，关闭连接。
- 尚未进入命令处理器的全连接限流、并发上限或入口过载：返回 `ERROR / RATE_LIMITED`、`RESOURCE_LIMIT` 或 `OVERLOADED`，`close_connection = false`。动作是 `RETRY_SAME_REQUEST`，但涉及未决发送时仍必须保留原键。

关闭标记为 `true` 时，发送方尽力写出该错误后关闭，不再接收新请求；不能为了等待错误发送而无限阻塞。标记为 `false` 时该错误本身不关闭连接，客户端仍须容忍独立发生的网络断开。任何关闭都不撤销已进入提交路径的操作。

关闭连接的协议错误与 `SESSION_REPLACED` 动作为 `NONE`，认证资格撤销为 `REAUTHENTICATE`。客户端收到 `SESSION_REPLACED` 后不自动争抢同设备连接；未收到错误包的网络断开按普通重连处理。无响应的 PushAck 除错误外不产生另一个应答。

### 命令已识别后的业务结果

通过帧、方向和连接状态校验并进入处理器后，认证、同步、快照和历史请求的字段缺失、取值错误、权限不足、页预算、令牌失效和内部错误，都通过各自响应的 `oneof error` 返回，连接保持可用。必要的缺省标量、非法零值与 oneof 未选择属于 `INVALID_ARGUMENT / NONE`；未知枚举值属于 `UNSUPPORTED_CAPABILITY / NONE`。

`SEND` 与 `SEND_STATUS` 没有普通 `oneof error`。键缺失、键无法关联或无法解析内容结构时，返回 `ERROR / INVALID_ARGUMENT` 或 `UNSUPPORTED_CONTENT`，`close_connection = false`，只说明当前尝试无效。键与内容结构均有效且处于允许接受的窗口内，但文本长度、业务权限或内容规则不满足时，由权威路径保存最终拒绝后返回 `rejected_final`；无法保存或查明终态时返回 `unresolved`，不得伪装成最终失败。

在查询期限内，发送处理器先匹配已有终态再裁定新的业务拒绝，避免权限变更或新的限额推翻原提交。`SEND_STATUS` 的有效键只返回 `SendOutcome`；鉴权与入口错误仍走 `ERROR`。进入发送处理器后的临时限流、过载和内部故障使用 `unresolved.error` 中的对应错误码，动作 `QUERY_SEND`，没有权威证明时关闭标记为 `false`，不因处理器位置不同改变原逻辑消息的提交事实。

`rejected_final` 仅使用 `INVALID_ARGUMENT`、`PERMISSION_DENIED`、`RESOURCE_LIMIT` 或 `REQUEST_EXPIRED`，动作为 `NONE`，不带退避时间。`unresolved` 中，`NOT_FOUND`、`IN_PROGRESS`、`CLOCK_SKEW` 的关闭标记必须为 `false`，`IDEMPOTENCY_CONFLICT` 与 `RESULT_EXPIRED` 必须为 `true`；`RESULT_UNAVAILABLE` 依关闭证明决定。客户端同时检查三态分支、错误码、动作和关闭标记，不能只读取其中一个字段。

入口错误、一次尝试的校验错误和 `unresolved` 都不能覆盖客户端已知终态。没有权威关闭证明时，它们也不能使客户端放行后序发送。出现本地已知提交与服务端最终拒绝冲突时，客户端停止该会话提交并报告一致性故障，不自动选择较新的响应。

### 未知字段、分支与内容

普通未知字段允许跳过，但不得改变必填、枚举和互斥校验。未知枚举数值不能被映射为 0 或其他已知值。新增会影响解释或提交效果的字段必须先定义能力协商规则，不得以普通可选字段名义启用。

为识别解码器可能丢弃的未知 oneof 分支，纯 oneof message 的字段号 1 至 63 专用于分支；`Change` 的字段号 2 至 63 专用于 `payload`，字段 1 固定为事件序。这些 message 的普通扩展字段使用 64 及以上编号，不得占用分支空间。未知分支号与已知分支同时出现仍属于多个分支，按非法线格式处理；只有未知分支时按不支持的语义处理。单纯缺少全部分支属于缺少必填项。

发送请求的 `SendRequest`、`MessageKey`、`MessageContent` 和内容子结构出现未知字段或未知内容分支时，返回 `ERROR / UNSUPPORTED_CONTENT / NONE`，不提交，不自动换键。未知内容必须完成能力与内容比较规则设计后才能写入，不按空文本接受。

客户端收到未知必要变化、未知 `Change.payload`、未知消息内容、未知状态枚举，或响应中只有未知 oneof 分支时，不应用该批结果、不保存其游标、不确认该 Push。客户端关闭连接并向上层报告不支持的协议能力，保持最后完整落盘位置，停止自动重复恢复该范围；更新能力后重新认证并从已保存游标恢复。不能反复用快照掩盖同一未知语义，也不能显示为普通空消息。

客户端发现服务端响应违反必填、互斥、身份关联或上下文约束时，同样关闭并报告协议错误，保留发送未决状态与最后有效游标。客户端不发送服务端专用的 `ERROR` 命令。

字段编号、类型和含义一经发布不得重用或重新解释；删除字段同时保留名称与编号。oneof 新分支仍使用已声明分支空间，新增必需语义先协商能力；范围不足时修改协议约定，不能让实现自行分配。

## 资源限制与默认配置

```text
ProtocolLimits {
  max_frame_bytes: uint32 = 1
  max_text_bytes: uint32 = 2
  max_page_items: uint32 = 3
  max_page_bytes: uint32 = 4
  max_inflight_requests: uint32 = 5
  heartbeat_interval_ms: uint32 = 6
  idle_timeout_ms: uint32 = 7
}
```

整帧硬上限为 1 MiB，`Head` 上限为 256 字节，服务端可以协商更小的上限。认证前同样执行硬上限和独立接入限流。解码还需限制嵌套深度、字段与列表数量，不能仅控制网络字节数。配置组合必须保证一条合法消息及其响应能装入对应预算，不能发布自身无法满足的限制。

默认文本上限为 64 KiB，分页上限为 100 项且响应消息体不超过 256 KiB，并发请求上限为 32，心跳间隔为 30 秒，连接空闲超时为 90 秒。这些参数可配置，并依据接入环境和压测结果调整。

`PageLimit.max_bytes` 约束成功分页响应的完整消息体大小，不只统计正文，且仍受整帧上限约束；错误响应受整帧上限约束，不受客户端过小的分页预算限制。页条数和字节预算为 0 时返回 `INVALID_ARGUMENT / NONE`，超过服务端上限时返回 `RESOURCE_LIMIT / RETRY_SAME_REQUEST`，客户端收紧到协商上限再重试。单项放不进本页预算时返回 `PAGE_BUDGET_TOO_SMALL / RETRY_SAME_REQUEST`，客户端将字节预算提升到协商上限后重试同一位置，不拆开事件或跳过它推进进度。

所有 Token 最多 4096 字节，错误 `detail` 最多 256 个 UTF-8 字节，解码嵌套深度最多 16 层、单帧字段出现总数最多 16384。bool 的线值只接受 0 或 1，整数必须符合声明类型范围，不截断。字段与列表计数包括未知字段的出现，资源检查在大规模分配前完成。

控制、发送结果、推送和批量读取分别限流排队，但同一 TCP 连接中已写出的数据无法被后续确认越过。实现需控制写入批次、队列字节数与等待时间；重连、推送补偿和历史读取共享恢复预算，避免恢复流量挤占在线提交。

## 实现顺序与验收

按依赖关系实现固定头解析、结构校验、认证与错误处理，再完成持久发送、结果查询、同步和快照恢复，接入推送与历史分页。客户端采用同设备同会话串行队列和独立设备游标，并行发送、压缩与去重清理遵循对应设计约束。

IDL 严格使用本文字段号、枚举值和命令映射，协议测试使用统一的有效与无效报文样例。实现模型按本文生成结构和处理分支，数据库适配与业务规则单独实现；遇到尚未确定的产品语义时先提出对应设计变更，不自行增加隐式规则。

验收至少覆盖以下情况：

- 帧分成多次读取、一次读到多帧、非法长度与超限输入，解析结果正确且内存受限
- 确认丢失后同键重试得到原回执，同键不同内容不产生新消息
- 前序发送未知且未获得关闭证明时后序等待；接受已关闭但结果未知时可处理下一条，历史结果仍保持未知；最终拒绝的旧键不能再次提交
- 结果查询暂时查无记录不转成确定失败，网关限流也不推翻先前提交
- 旧连接状态丢失后重新认证，待发送消息与设备游标保持有效
- 新会话首次推送丢失、已知会话最后一次推送丢失，主动同步均可发现
- 编辑、撤回与旧推送乱序到达，状态不回退；空页与过滤间隙不造成错误跳跃
- 客户端落盘中途崩溃或重复收到同步页，不丢状态、不重复生成消息
- 快照分页期间发生消息变化，最终基线与增量衔接；访问范围变化使旧缓存失效
- 游标过期、分页令牌过期、未知必要事件与历史回退均进入明确的恢复或拒绝分支
- 未请求、未启用与已启用 `push_ack` 分别遵守能力约束；未知请求能力被忽略，非法启用集合使客户端关闭连接
- 令牌被修改、跨用户或设备使用、游标与分页票据混用时拒绝；合法令牌跨网关重连仍可继续，失去快照资源时返回规定错误
- 接受窗口边界、查询截止、窗口关闭与终态清理并发时不重新接受旧键；时钟回拨、旧写入方复活和不完整恢复不能绕过关闭水位
- 缺省与显式零值、重复单值字段、多个 oneof 分支、未知分支和未知内容在不同语言端得到一致结果

存储适配必须验证原子提交、窗口关闭证明、固定读取边界和恢复完整性；具体数据库部署与产品权限细则不在本文展开。客户端、网关和核心服务分别按本文约束验收，不以单侧解码成功替代端到端语义验证。

## 编解码与边界样例

以下十六进制仅用于可重复测试，正向编码样例按字段号升序输出；合法字段重排也应能解码。心跳帧的业务前提是连接已经认证。拒绝样例由协议校验层判定，不能只断言 Protobuf 库是否报错。

```text
Head(req_id = 1)
  08 01

包含该 Head 的 HEARTBEAT 完整帧
  01 03 00 00 00 00 00 02 00 00 00 00 08 01

SyncTarget(account = Empty)
  0a 00

同一 SyncTarget 同时含 account 与 conversation = 7，必须拒绝
  0a 00 10 07

Head 重复携带 req_id，必须拒绝
  08 01 08 02

SyncTarget 只有未知分支 3，不得按空目标继续
  1a 00

MessageContent 只有未知内容分支 2，不得当作空文本发送
  12 00
```

`SendUnresolved.acceptance_closed = false` 必须编码为字段 2 的 `10 00`，省略该字段是缺少必填项；这段字节只是字段片段，完整消息还必须有 `error`。`ErrorInfo.code = 0` 即使显式编码也不合法。

ID 测试值 `018bcfe5-6800-7000-8000-000000000001` 的字节为 `01 8b cf e5 68 00 70 00 80 00 00 00 00 00 00 01`，其 `T = 1700000000000`。这是固定测试输入，真实生成必须使用随机源。接受截止为 `1700086400000`，查询截止为 `1700691200000`。

- 服务端时间为 `T - 120000`，结构与权限合法时允许首次接受；为 `T - 120001` 时返回 `CLOCK_SKEW`，不产生提交。
- 截止前已经提交且确认丢失，接受截止后、查询截止前重试仍取得原回执。
- 正好到接受截止时不得新提交。窗口尚未闭合为 `unresolved / IN_PROGRESS`；闭合且完整记录证明未提交为 `rejected_final / REQUEST_EXPIRED`。
- 正好到查询截止且已取得关闭证明时，为 `unresolved / RESULT_EXPIRED`，关闭标记为 `true`。清理前后结果一致，旧键不能重新提交。
- 接受截止前进入队列、截止后才到达权威决定点的请求不能获得新提交资格；已在截止前形成的决定需要恢复到确定结果后才能证明窗口关闭。
