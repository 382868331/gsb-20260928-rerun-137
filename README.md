# Replayable App Framework

这是一个 Go 事件溯源与可重放投影的最小框架。底层只保存通用事件记录：流 ID、版本号、事件类型和 JSON 负载；不同业务定义自己的事件与状态投影，不需要改存储层。

## 核心能力

- 事件追加按流内版本做乐观并发检查，版本不匹配返回 `ConcurrencyError`。
- `FileEventStore` 把事件持久化到追加日志（CRC 帧 + fsync），确认的事件在进程重启后仍可找回；重开时截断不完整的尾部（未确认的半写入不会被当成历史），但完整记录的校验失败属于已确认内容损坏：不再静默截断，而是保留原文件、记录 `CorruptionReport`（同时落盘 `events.corruption.json`），以降级视图打开并拒绝新追加，等待操作者处理。
- 恢复走候选流程：`BuildRecoveryCandidate` 根据另存的可信材料核对可恢复范围，无法连续恢复的区间保留为缺口（`RecoveryGap`），与已确认前缀或另一份材料冲突的内容报告为 `RecoveryConflict`，绝不补造连续历史；操作者确认后 `CommitRecovery` 写入独立分支日志和清单（`events.recovery-N.log/.json`），原文件、原确认范围和恢复依据全部保留。
- 每次读取都能说明视图归属：`View()` / `ReadStreamView()` 返回 `ViewInfo`，区分原始视图、降级视图和恢复分支视图，重开之后仍然可分辨。
- `FileSnapshotStore` 先写临时文件、fsync 再原子 rename 发布快照；写入、发布或切换任一阶段中断都保留上一份可用快照。
- 投影严格要求事件版本连续，坏事件处理失败后立即停止，不越过事件继续提交。
- 新投影可以从完整历史重建；切换前旧投影仍作为当前查询视图。
- 快照保存流 ID、投影版本和状态；从快照恢复后只读取后续事件，避免漏算或重算。
- `ProjectionSwitch.Cutover` 在追加锁内追赶尾部事件再切换当前视图，切换期间的新追加不会丢失或重复应用。
- `TinyStateModel` 用确定性的版本与深相等比较，核对全量重放、快照重放和在线状态。

## 基本流程

1. 用 `OpenFileEventStore` 创建持久化事件存储（或 `NewMemoryEventStore` 做纯内存测试）。
2. 业务定义事件、状态、初始状态、深拷贝函数和 `EventHandler`。
3. 用 `Append` 追加业务事件，并传入期望版本。
4. 用 `NewProjector` 从历史重放状态，或先 `Restore` 快照再 `Replay`。
5. 用 `ProjectionSwitch` 注册多个投影；新投影可先从 `FileSnapshotStore` 恢复快照再 `Rebuild` 追赶，最后用 `Cutover` 在追加锁内切换查询视图。

虚构业务示例在 `example/shop`，演示库存、订单和收入状态的恢复。

## 验证

```bash
go test ./...
go test -race ./...
go vet ./...
```
