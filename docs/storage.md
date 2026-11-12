# 存储设计

本阶段明确支持多租户，仅使用 `conversations` 与 `messages` 两表，保存各租户的会话位置、成功文本消息及其去重依据。[`iam-store`](../crates/iam-store/src/lib.rs) 提供 SQL 组件和本机原生文本入口；默认协议路径仍使用 `MemoryStore`，不等于完整协议已持久化。[`protocol.md`](./protocol.md) 的约定保持不变。

存储入口保持 `应用 → TiDB SQL → TiKV`，PD 提供集群协调。应用统一走 SQL，不直连 TiKV 或双写，组件关系见 [TiDB 官方架构](https://docs.pingcap.com/tidb/stable/tidb-architecture/)。

## conversations：会话位置

| 字段 | 用途 |
|---|---|
| `tenant_id` | 标识会话所属租户，用于隔离不同租户的数据。 |
| `conversation_id` | 标识会话，供消息关联和按会话读取。 |
| `history_id` | 标识会话的历史代次，用于解释消息位置和还原回执。 |
| `next_sequence` | 保存下一条消息的创建序号，从 1 起，避免重用已提交位置。 |

主键：`(tenant_id, conversation_id)`；不加额外索引。

## messages：成功文本消息

| 字段 | 用途 |
|---|---|
| `tenant_id` | 限定消息所属的租户范围，与所属会话一致。 |
| `conversation_id` | 将消息关联到同租户的会话。 |
| `message_id` | 保存服务端生成的稳定消息身份，重试返回同一值。 |
| `user_id` | 保存本租户认证用户的发送者身份，对应协议消息中的 `sender_id`。 |
| `device_id` | 标识发送设备，参与区分不同设备的逻辑发送。 |
| `client_msg_id` | 保存协议定义的客户端逻辑发送身份，重试保持不变。 |
| `sequence` | 保存会话内创建顺序，对应 `create_seq`，供排序与分页。 |
| `text` | 保存文本原文，也用于按协议摘要规则检查重试内容是否一致。 |
| `accepted_at` | 保存首次接受的服务端毫秒时间，用于还原成功回执，不参与排序。 |

主键：`(tenant_id, conversation_id, message_id)`。仅增加两个唯一索引：

- `(tenant_id, conversation_id, sequence)`：保证创建位置唯一并支持顺序分页。
- `(tenant_id, user_id, device_id, conversation_id, client_msg_id)`：按完整发送身份对成功消息去重，不含连接或请求号。

## 最小读写关系

同一租户内，一个会话对应多条真实消息，仅通过 `(tenant_id, conversation_id)` 关联。当前只追加文本、保持 `history_id` 不变，消息沿用所属会话的历史代次，不重复存一份；排序沿用创建序语义，但组件不持久化变化事件，不作为协议完整提交依据。

1. **发送与重试**：先遵守协议结果查询期限，再按认证身份的完整发送键查消息；命中且内容一致时还原原回执，不同则返回冲突，不改写原消息。未命中且通过现有窗口和业务校验时，在同一事务中推进该租户的会话序号并插入消息；同键并发只能留下同一条消息，重试不改变原位置或接受时间，不为拒绝生成消息。
2. **查询与列表**：结果查询在协议期限内按当前用户、设备的完整发送键还原回执，不返回正文，也不因后来成员变化推翻原成功结果；消息列表另检查当前访问权，再在认证租户内按会话和 `sequence` 边界分页。查空不能证明最终失败。

当前只有文本，重试比较在应用中分别按协议固定规则从原文计算摘要，不裁剪、归一化，也不依赖数据库字符串比较；因此不另存内容哈希、成功状态或回执副本。

成功回执由消息的 `conversation_id`、`message_id`、`sequence`、`accepted_at` 和所属会话的 `history_id` 还原。前提是消息原文、身份、位置和时间不变，关联会话行及其历史代次持续保留；种子初始化不能覆盖已有历史或序号。本阶段不编辑、删除消息，不删除会话或重置历史。

## 租户隔离约定

多租户是确定需求。`tenant_id`、`user_id` 及经服务端校验的 `device_id` 均从认证上下文取得，不允许客户端任意覆盖；`user_id` / `sender_id` 标识用户，不能替代 `tenant_id`。

两表的主键、关联、成功去重唯一约束及所有查询和写入都包含当前 `tenant_id`，不能只凭会话或消息 ID 跨租户访问；不同租户的同号会话、用户和发送键互不影响。

本阶段仅提供租户数据边界，不新增租户管理表、后台或组织、计费、成员管理功能，也不改变协议身份定义。

## 能力边界

最终拒绝的跨重启重放、正文删除后的结果查询，以及编辑、撤回、历史重置后的结果保持，都尚未持久化，待后续需求细化。原内存协议的拒绝行为不变；新入口的校验错误只说明当前操作，不是可靠的 `rejected_final`，不生成空消息或隐藏记录。

成员权限、变化流、令牌和恢复控制状态仍在内存或暂缓持久化。两表只覆盖成功消息落库、排序与成功去重，不承诺完整 SYNC、SNAPSHOT、HISTORY 固定视图或重启恢复；恢复不完整时不能仅凭查空重新接受发送。这些缺口须在相应协议路径接入前补齐，不能降低现有协议要求或宣称持久 `committed` 已实现。

当前只保存文本；未来媒体对象与消息引用元数据分开考虑，本阶段不新增媒体表或系统。

## 实现入口与验证

[`TidbTextService`](../crates/iam-store/src/service.rs) 持有原 Engine，复用 AUTH / RECONNECT 及种子会话成员校验；业务调用只传连接句柄、会话和消息输入，不能传入租户或发送用户。会话初始化、发送、结果查询和分页实际调用 SQL，不走 MemoryStore 的消息集合。会话认证入口拒绝 SEND / SYNC / SNAPSHOT / HISTORY，避免偷偷回退内存路径。

原生 `SavedSend` / 控制台 `STORED` 只说明 SQL 写入成功，不是协议 `SendOutcome.committed`；`Unknown` 或查询到期错误都不提供关闭证明。原生分页是仅追加消息的排他序号分页，不签发协议令牌或建立同步基线。

启动默认 `OpenMode::ReadOnly`：已有成功消息可查询、按当前权限分页，初始化只能读取已有会话。`FreshDatabase` 是调用方对“全新、无历史的本机试验库”的显式声明，同时要求两表为空；仅本进程新建的会话获得写入资格。空库检查只用于服务端启动校验，不向客户端返回租户数据；业务查询始终限定认证租户。不能用此模式处理重启、备份恢复或旧库查空，也没有强制恢复写入的开关。

写事务使用 TiDB 悲观事务和提交后可见的读取语义，会话锁后重新取数据库时间，保证等待期间越过接受截止的请求不会被新接受。提交或回滚结果未知、写入等待被取消时，当前服务实例停止所有新接受；已知成功结果仍可按原键读取。这里只关闭当前实例的新接受能力，不宣称完成持久拒绝或故障恢复。TiDB 的当前读与快照读差异见 [官方事务说明](https://docs.pingcap.com/tidb/stable/pessimistic-transaction/)。

SQL 位于 [`schema.sql`](../crates/iam-store/schema.sql)，仅全新库模式执行两条 `CREATE TABLE IF NOT EXISTS`；只读启动不建表，不修补缺失状态。两种模式都检查必要字段及四个主键/唯一索引，结构不符即失败，不自动改写。不建迁移记录表，不 DROP / TRUNCATE / 清理数据。ID 用无符号整数与原始 UUID 字节，正文按 UTF-8 字节保存，支持协议的 64 KiB 文本边界。

组件只接受 loopback TCP URL，逐连接要求 `tidb_version()` 成功，不把 MySQL 兼容性当作 TiDB 验证。以下仅供全新本机试验库使用：

```sh
# 已有 TiUP 时启动本机测试集群；安装与要求见下方官方快速开始。
tiup playground --db 1 --pd 1 --kv 1 --without-monitor

# 另一终端；只创建新的本机专用数据库，不复用生产库。
mysql -h 127.0.0.1 -P 4000 -u root -e 'CREATE DATABASE iam_local; CREATE DATABASE iam_test;'
export IAM_TIDB_URL='mysql://root@127.0.0.1:4000/iam_local'
export IAM_ACCESS_TOKEN='local-demo'
export IAM_TENANT_ID=1 IAM_USER_ID=100 IAM_CONVERSATION_ID=1
IAM_TIDB_MODE=fresh cargo run -p iam-store --bin iam-tidb-text
```

TiUP 要求见 [官方本机快速开始](https://docs.pingcap.com/tidb/stable/quick-start-with-tidb/)。控制台输入 `auth local-demo 018bcfe5680070008000000000000001`、`init`、`id`，再用生成的 ID 输入 `send ID 文本`、`status ID`、`page`。文本不使用引号包装，空格原样保留；多行内容可用 Rust 服务 API。`stop` 永久关闭本实例新接受，`quit` 退出。重启使用 `IAM_TIDB_MODE=read-only`，同一设备和原消息 ID 可查成功结果；不能继续新发。种子 token 仅用于本机验证，不是真实鉴权系统。

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings

# 显式运行真实 TiDB 套件；缺少配置或连到 MySQL 会失败，不会悄悄跳过。
IAM_TIDB_TEST_URL='mysql://root@127.0.0.1:4000/iam_test' \
IAM_TIDB_ALLOW_TEST_WRITES=1 \
cargo test -p iam-store tidb_ -- --ignored --nocapture
```

真实测试默认 ignored，普通 `cargo test` 不代表它们通过；每个用例在 `iam_test` 前缀的本机数据库下创建新的隔离数据库，只写测试数据并保留，不自动删除。覆盖初始化保留、插入失败回滚、并发序号与同键重试、内容冲突、无符号 ID、租户/用户/设备隔离、分页、结构检查和重启只读。取消用例先通过 TiDB `PROCESSLIST` 确认真实写入已进入锁等待，再取消服务调用并核对后续新接受关闭；未知提交分类另有单测，真实提交断连与进程崩溃仍需另外演练。未执行真实 TiDB 套件时不得宣称 TiDB 已验证。

### 当前验证限制（2026-11-12）

当前环境没有依赖缓存，crates.io 下载失败，沙箱禁止网络套接字；因此 Cargo.lock 生成、完整编译、单测及原有回归均未完成。rustfmt、clippy、TiUP 和 Docker 不可用，真实 TiDB 套件未执行。清单、结构、链接和差异检查不能替代上述验证，不以语法解析或模拟数据库结果计为通过。

核心阶段的正常 `git add` 因 `.git/index.lock: Read-only file system` 失败，尚未创建本地提交；未绕过权限或 hooks，未 push / merge。解除环境阻塞后应先生成锁文件并执行上述检查，再将核心组件与控制台接入按正常阶段提交。
