# Replay Events

Go 实现的可重放事件应用框架第一轮骨架。

- `Event` 是底层统一信封，包含全局序号、流名、流版本、事件类型和 JSON payload。
- `Codec` 由各业务注册自己的 payload 类型，底层存储不感知业务结构。
- `MemoryEventStore.Append` 使用流版本做乐观并发检查，`AnyVersion` 表示新建流。
- `Projection` 严格按事件序号推进；handler 返回的状态版本必须等于事件序号。
- `Runner.Restore` 可从快照后继续重放，坏事件直接返回错误，不会越过继续成功。
- `Manager` 可同时维护新旧投影；新投影重建和追赶期间，旧投影仍保持可查询，验证后再 `Promote`。

运行验证：

```bash
go test ./...
```
