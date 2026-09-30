# Replayable App Framework

这是一个 Go 事件溯源与可重放投影的最小框架。存储层只保存通用事件记录：流 ID、流内版本、全局位置、事件类型和 JSON 负载；不同业务定义自己的事件与投影，不需要改存储层。

## 核心能力

- `MemoryEventStore` 提供测试用存储；`FileEventStore` 使用仅追加 JSONL，每条事件写入后 `fsync`，重开时保留完整事件并截断未完成的尾部写入。
- 事件追加按流内版本做乐观并发检查，版本不匹配返回 `ConcurrencyError`。
- 投影严格要求事件版本连续；处理坏事件时保留最后成功版本、全局位置和失败原因，不跳过后声称追平。
- `FileSnapshotStore` 用临时文件、`fsync` 和原子改名发布快照，不能用较旧快照覆盖较新快照。
- 新投影可以从全量历史、已有快照或增量事件重建；重建期间旧投影仍作为当前查询视图。
- `Switch` 在存储级临界区内读取终点版本、补齐事件、验证追平并发布当前投影标记；重建期间追加的事件不会漏算。
- `FileCurrentStore` 原子持久化当前投影名称、版本和位置；快照或标记发布失败时不会替换内存或磁盘上的旧有效版本。
- `TinyStateModel` 用确定性的版本与深相等比较，核对全量重放、快照重放和增量重放。

## 基本流程

1. 用 `NewMemoryEventStore` 或 `NewFileEventStore(eventsPath)` 创建事件存储。
2. 业务定义事件、状态、初始状态、深拷贝函数和 `EventHandler`。
3. 用 `Append` 追加事件，并传入期望版本。
4. 用 `NewProjector` 做全量重放，或 `Restore` 快照后再 `Replay` 增量事件。
5. 用 `ProjectionSwitch` 注册新旧投影，`Rebuild` 新投影，`PublishSnapshot` 发布快照，最后 `Switch(ctx, name)` 追平并切换。
6. 进程重开后重新注册处理器，调用 `Restore` 恢复投影快照、`RestoreCurrent` 恢复当前指针，再 `CatchUp`。

虚构业务示例在 `example/shop`。设备注册、遥测状态和固件更新示例在 `example/device`，覆盖全量、快照、增量投影，以及事件追加、快照发布和切换标记中断后的重开接续。

## 验证

```bash
go test ./...
go test -race ./...
go vet ./...
```
