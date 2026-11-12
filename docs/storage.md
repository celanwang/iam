# 存储设计

[`iam-store`](../crates/iam-store/src/lib.rs) 提供统一的 TiDB 存储，覆盖多租户单聊文本的持久发送、结果查询、同步和快照。[`protocol.md`](./protocol.md) 定义对外契约。

存储入口保持 `应用 → TiDB SQL → TiKV`，PD 提供集群协调。应用统一走 SQL，不直连 TiKV 或双写，组件关系见 [TiDB 官方架构](https://docs.pingcap.com/tidb/stable/tidb-architecture/)。

## 正式协议的事务与恢复

### 持久状态

| 表 | 职责 |
|---|---|
| `store_control` | 保存唯一存储 ID、同步票据密钥、已观察的最大数据库时间和接受窗口关闭水位 |
| `conversations` | 按租户保存会话历史代次和下一创建序 |
| `messages` | 保存成功发送的完整幂等键、正文、回执数据及创建序。在只追加文本的阶段，消息行同时提供对应的 `message_upsert` 事件，`event_seq` 等于 `create_seq` |
| `rejections` | 按完整幂等键保存最终拒绝及协议规定的内容摘要。接受和查询截止可由 UUIDv7 时间按固定协议公式重建 |
| `members` | 保存初始化时的会话成员及账户变化序号，供授权、账户同步和推送选址 |

正式协议和本机文本控制台共用 [schema.sql](../crates/iam-store/schema.sql) 中的五张表。初始化显式提供存储 ID、会话及成员，种子会话与控制行在同一事务提交。会话成员、历史代次和消息在这一阶段保持可重读。支持追加文本、稳定会话基线和账户会话发现。编辑、删除、成员变更、历史读取、密钥轮换和真正的认证签发方由后续阶段接入。

使用 [`TidbService::connect_local`](../crates/iam-store/src/service.rs) 打开库，向 `handle_frame` 传入由 `FrameDecoder` 收到的完整帧。首次使用 `OpenMode::Initialize`，后续使用 `Resume`。首次的存储 ID 由调用方生成并保存，认证种子由调用方在每次启动时提供。

初始化要求存储表为空。服务发现旧的 `protocol_` 表名时会拒绝打开该库。

### 发送事务

1. `SEND` 和 `SEND_STATUS` 在 TiDB 悲观事务内先锁住同一个控制行，再读取数据库时间和权威记录。所有正式路径写入遵循这一顺序。同一库的发送决定暂时串行化。
2. 首先检查结果查询截止。在期限内读取原成功或拒绝记录，内容冲突返回 `IDEMPOTENCY_CONFLICT`。结果仍未知时才检查关闭水位、时间和当前权限。
3. 首次成功发送在同一事务分配会话序号并插入消息。该消息也是去重、回执和同步的持久依据。业务拒绝只写最终拒绝记录。确认事务提交后才返回 `committed` 或已保存的 `rejected_final`，并按需向本实例的在线会话成员推送。推送丢失后由同步补齐。
4. 关闭水位记录已封闭的客户端消息时间 `T` 的包含上界。可靠数据库时间下，同一控制行将水位推进到 `now - 24h`。取得行锁表示先前持锁事务已经结束，后续写入也会检查新水位。签发及校验同步票据时也在控制行上采样时钟。时钟回拨时使用持久时间上界保护查询截止，保留已有水位并停止新接受。现阶段保留终态和消息，无后台清理。

### 恢复与同步

正常进程重启时，调用方显式打开同一份权威 TiDB 数据库，并提供初始化时的存储 ID。入口校验控制记录和所需表，并通过控制行锁等待旧事务确定结果。提交响应丢失时返回 `unresolved`，下次同键查询重新获取锁并读取真实结果。已知数据库发生回退、恢复来源无法确认或控制记录缺失时，禁止启用该入口。存储 ID 只用于校验路由，备份还原出来的同 ID 数据库需要单独的恢复方案。

`SYNC` 和 `SNAPSHOT` 使用 TiDB 的已提交记录并重新检查当前权限。会话读取按创建序，账户读取按持久成员序号，每轮固定上界。游标、分页和快照票据以数据库中保存的密钥验证，正常应用重启后可继续使用。认证会话仍由进程内协议引擎管理，重启后可重新认证并继续使用同设备游标。页发现序号缺口时返回错误并保留客户端已保存的进度。

## 租户隔离

租户、用户和设备来自认证上下文。会话与消息按租户关联，发送幂等键包含租户、用户、设备、会话和客户端消息 ID。读取与写入均以认证租户为条件。已保存的发送结果按原发送身份查询，消息读取另行检查当前成员权限。

## 本机控制台与验证

[`iam-tidb-text`](../crates/iam-store/src/bin/iam-tidb-text.rs) 通过 `TidbService` 处理认证、发送和查询。会话在首次初始化时写入，`init` 查看现有会话位置。`page` 在检查成员权限后按创建序读取同一张消息表。

组件只接受 loopback TCP URL，逐连接要求 `tidb_version()` 成功，不把 MySQL 兼容性当作 TiDB 验证。以下仅供全新本机试验库使用：

```sh
# 已有 TiUP 时启动本机测试集群；安装与要求见下方官方快速开始。
tiup playground --db 1 --pd 1 --kv 1 --without-monitor

# 另一终端；只创建新的本机专用数据库，不复用生产库。
mysql -h 127.0.0.1 -P 4000 -u root -e 'CREATE DATABASE iam_local; CREATE DATABASE iam_test;'
export IAM_TIDB_URL='mysql://root@127.0.0.1:4000/iam_local'
export IAM_ACCESS_TOKEN='local-demo'
export IAM_STORE_ID=018bcfe5680070008000000000000002
export IAM_TENANT_ID=1 IAM_USER_ID=100 IAM_CONVERSATION_ID=1
IAM_TIDB_MODE=fresh cargo run -p iam-store --bin iam-tidb-text
```

TiUP 要求见 [官方本机快速开始](https://docs.pingcap.com/tidb/stable/quick-start-with-tidb/)。控制台输入 `auth local-demo 018bcfe5680070008000000000000001`、`init`、`id`，再用生成的 ID 输入 `send ID 文本`、`status ID`、`page`。文本空格原样保留。`stop` 停止本实例接受新发送，已保存的结果仍可查询或重放。`quit` 退出。重启同一权威库时保留 `IAM_STORE_ID` 并设置 `IAM_TIDB_MODE=resume`，用同一设备和消息 ID 查询原回执。种子 token 用于本机验证。

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings

# 显式运行真实 TiDB 套件；缺少配置或连到 MySQL 会失败，不会悄悄跳过。
IAM_TIDB_TEST_URL='mysql://root@127.0.0.1:4000/iam_test' \
IAM_TIDB_ALLOW_TEST_WRITES=1 \
cargo test -p iam-store tidb_ -- --ignored --nocapture
```

真实 TiDB 用例默认标记为 `ignored`，使用上面的命令显式执行。用例覆盖单套表结构、初始化保护、事务回滚、并发去重、租户隔离、正文限制、分页、时钟回拨和关窗。每个用例使用独立的 `iam_test_*` 测试库。

提交故障用例启动实际的 `iam-tidb-text` 子进程，通过本机 TCP 代理在转发 `COMMIT` 前或收到 TiDB 成功响应后暂停。两个位置分别验证断连和进程被强制终止。提交前故障后同键查询为未知，恢复后重试可提交一次。提交后响应丢失时，同键查询与重试返回原回执。协议验收还验证丢推后的同步补齐和重启后的票据续读。

### 验证记录

在本机 TiDB、TiKV 和 PD v8.5.7 上完成工作区编译、格式检查、Clippy 和回归测试。验证使用仓库内的 `Cargo.lock`，真实数据库用例在专用 `iam_test` 库下运行。
