# swarm-discovery 维护基线与生命周期补丁

基线：官方发布版本 0.6.4，源码提交 `360811856f3f6fe9883e3b2988a5d8b961b4dd6a`，来自发布包 `.cargo_vcs_info.json`；上游无同名 release tag。自有维护分支 `syzygy/gc-lifecycle-v0.6.4`，保留完整上游历史。

沿用上游默认 multicast join 失败容忍（PR #30）与 GC 停止重试（PR #27）的行为。保留本地多接口 IPv4 socket 绑定顺序、路由和 SendStats。

原 DropGuard 只 abort guardian，Acto 0.8.2 abort 会丢掉实际 Tokio JoinHandle；父 actor Drop 也只 abort 子任务。因此消费方无法从公开 API 证明 socket owner 已退出。RemoveInterface 只删 ActoRef 而 receiver 仍在监督树中持有 UDP socket。

修复保留 Acto 监督树：CancellationToken 请求退出，guardian 收齐所有已登记 child 的 Supervision 后返回；接口按独立 token 取消，以 ActoId 识别退休回执。sender/updater 定时器收回各 actor 自己的 sleep/select，删除 detached timer 任务和旧 Timeout/GC 消息机制。原仅测 detached GC helper 的测试随该 helper 删除。

DropGuard::shutdown 借用并保留同一个 guardian handle，取消或并发等待不会丢失 join；完成后释放 guard 自己持有的 sockets，重复等待返回相同结果。Drop 仅请求同一合作取消；需要完成凭据的消费者调用 shutdown。set_interfaces_v4 在原 mailbox 上可靠等待背压并提交完整 inventory，guardian 对自己持有的接口对账，失败新增会在下次 inventory 重试。没有第二队列或后台清理任务。

删除条件：上游稳定 release 提供可取消、可重复的 joined shutdown、interface receiver 真正退出以及无游离定时器的同等合同后，移除 fork 路径并复跑消费者与真实端口旅程。

来源：
- https://github.com/rkuhn/swarm-discovery/commit/360811856f3f6fe9883e3b2988a5d8b961b4dd6a
- https://github.com/rkuhn/swarm-discovery/pull/30
- https://github.com/rkuhn/swarm-discovery/pull/27
- https://docs.rs/acto/0.8.2/src/acto/tokio.rs.html

本轮验证由父仓 tmp/evaluation/iroh-upgrade-20261021/ 和 docs/ongoing/2026-10/iroh-upgrade-20261021.md 记录。

2026-10-21，macOS aarch64、Rust 1.98.1 的实际验证：

- 独立 `mbx +1.98.1 test --lib --all-features`：4 passed，包含真实 multicast 地址变更。
- 独立 `mbx +1.98.1 clippy --locked --all-targets --all-features --no-deps -- -D warnings`：通过。
- 消费方 syzygy-net-discovery：79 passed；syzygy-net：86 passed；syzygy-net-node-runtime：57 passed；syzygy-net-transport：53 passed。
- 本机业务驱动观察本进程 UDP 5353：运行时 8 个、移除接口后 1 个、取消首次 shutdown 等待并继续关闭后 0 个。关闭后仍保留 DropGuard 变量，重复 shutdown 返回同一完成结果。
- Windows/Linux/iOS/Android 的实际接口切换和套接字运行验证未执行。
