# Replayable App Framework

这是一个 Go 事件溯源与可重放投影的最小框架。底层只保存通用事件记录：流 ID、版本号、事件类型和 JSON 负载；不同业务定义自己的事件与状态投影，不需要改存储层。

## 核心能力

- 事件追加按流内版本做乐观并发检查，版本不匹配返回 `ConcurrencyError`。
- 投影严格要求事件版本连续，坏事件处理失败后立即停止，不越过事件继续提交。
- 新投影可以从完整历史重建；切换前旧投影仍作为当前查询视图。
- 快照保存流 ID、投影版本和状态；从快照恢复后只读取后续事件，避免漏算或重算。
- `FileEventStore` 将每次成功 `Append` 作为不可变事件段持久化；临时段在崩溃后仍与完整历史隔离。
- `FileSnapshotStore` 使用不可变快照文件加当前指针，写入、发布、指针切换中断后仍可打开旧视图并继续恢复。
- `RestoreRebuildAndSwitch` 从旧快照追赶新事件，并在追加锁内读取最终尾部后原子切换，避免漏追加和重复应用。
- `TinyStateModel` 用确定性的版本与深相等比较，核对全量重放、快照重放和在线状态。

## 基本流程

1. 用 `NewMemoryEventStore` 创建进程内存储，或用 `NewFileEventStore(dir)` 创建持久事件日志。
2. 业务定义事件、状态、初始状态、深拷贝函数和 `EventHandler`。
3. 用 `Append` 追加业务事件，并传入期望版本；只有临时段 fsync、原子发布并同步目录后才返回成功。
4. 用 `NewProjector` 从历史重放状态，或从 `FileSnapshotStore.Load` 恢复快照后再 `Replay` 后续事件。
5. 用 `ProjectionSwitch.RestoreRebuildAndSwitch` 从旧快照追平并原子切换；切换后到达的追加可再次追平，不会重复应用。

虚构业务示例在 `example/shop`，演示库存、订单和收入状态的恢复；`example/device` 演示多个设备流交错追加、进程重开、快照追赶与完整重放一致性。

## 验证

```bash
go test ./...
go test -race ./...
go vet ./...
```
