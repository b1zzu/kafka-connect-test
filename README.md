# Kafka Connect Test Connector

A Kafka Connect source connector that produces no records. Its only purpose is to simulate
connectors that do **not** react instantly to lifecycle actions (start, stop, pause, restart) or
that accept a config update but fail to actually bring their tasks up - so the
[kafka-connect-operator](../kafka-connect-operator) can be tested against that behaviour instead of
assuming every action completes the moment it's requested.

## Why

A real connector may:
- take a few seconds to start (connecting, warming up, negotiating).
- take a few seconds to stop or pause.
- accept a new/updated config but still fail to start its tasks.
- start fine and fail some time later on its own.

If the operator treats "I asked Kafka Connect to do X" as "X is done", it can report a stale or
wrong status. For example: a restart is implemented as stop + start; if stop takes a few seconds,
the operator must not report the connector as restarted (or running) before the stop has actually
completed and the new start has begun.

This connector lets you reproduce each of those cases on demand via config, without depending on a
real datastore or broker behaving flaky.

## Config

| Property | Type | Default | Description |
|---|---|---|---|
| `start.delay.ms` | long | `0` | How long the **connector** blocks inside `start()` before returning. |
| `start.fail` | boolean | `false` | If `true`, the **connector's** `start()` throws (after `start.delay.ms`), so the connector itself is reported `FAILED`. |
| `start.tasks.delay.ms` | long | `0` | How long each **task** blocks inside `start()` before returning. |
| `start.tasks.fail` | boolean | `false` | If `true`, each **task's** `start()` throws (after `start.tasks.delay.ms`), so the connector accepts the config but its tasks fail to come up. Ignored when `start.tasks.fail.count` is set. |
| `start.tasks.fail.count` | int | `-1` | If `>= 0`, exactly this many of the `tasks.max` tasks (the first N) fail at start, while the rest run fine - overrides `start.tasks.fail`. Disabled when negative. |
| `fail.after.ms` | long | `-1` | If `>= 0`, a task starts successfully but fails itself this many ms later. Disabled when negative. |
| `stop.delay.ms` | long | `0` | How long each task blocks inside `stop()` before returning. |
| `poll.delay.ms` | long | `1000` | How long each `poll()` call blocks before returning no records. Doesn't affect correctness, just loop friendliness. |

The connector runs exactly `tasks.max` tasks, each with the same config (so each task delays/fails
on the same schedule).

### Note on pause

Kafka Connect's `PAUSED` state is handled entirely by the framework (it just stops calling
`poll()`); there is no task-level hook to delay it. `stop.delay.ms` therefore only applies to the
`STOPPED` state (and to restarts, which are `stop()` followed by `start()`) - which is also the
scenario most likely to trip up an operator that assumes stop is instantaneous.

## Build

```bash
mvn clean package
```

## Docker Build

```bash
docker build -t kafka-connect-test:1.0.0 .
```

The JAR will be at `/kafka-connect-test.jar` in the container root - mount it into your Kafka
Connect image's plugin path.

## Example: simulate a slow, then flaky, restart

```json
{
  "name": "test-connector",
  "config": {
    "connector.class": "com.example.kafka.connect.test.TestConnector",
    "tasks.max": "1",
    "start.tasks.delay.ms": "5000",
    "stop.delay.ms": "8000",
    "fail.after.ms": "30000"
  }
}
```

- Restarting this connector takes >= 8s to actually stop the old task, then >= 5s to start the new
  one - an operator polling status right after issuing the restart should still see the old task
  winding down, not `RUNNING`.
- 30s after each successful start, the task fails on its own, simulating a connector that looked
  healthy right after start but degraded afterwards.

## Example: simulate a connector that fails to start

```json
{
  "name": "test-connector",
  "config": {
    "connector.class": "com.example.kafka.connect.test.TestConnector",
    "tasks.max": "1",
    "start.fail": "true"
  }
}
```

The config is accepted (`PUT /connectors/.../config` succeeds), but the connector itself goes to
`FAILED` - its tasks never even get created.

## Example: simulate a config update whose tasks can't start

```json
{
  "name": "test-connector",
  "config": {
    "connector.class": "com.example.kafka.connect.test.TestConnector",
    "tasks.max": "1",
    "start.tasks.fail": "true"
  }
}
```

The connector is created/updated successfully and reports `RUNNING`, but each task immediately goes
to `FAILED`.

## Example: simulate some tasks failing while others run fine

```json
{
  "name": "test-connector",
  "config": {
    "connector.class": "com.example.kafka.connect.test.TestConnector",
    "tasks.max": "3",
    "start.tasks.fail.count": "2"
  }
}
```

With `tasks.max=3` and `start.tasks.fail.count=2`, tasks 0 and 1 go to `FAILED` on start while task 2
starts normally and stays `RUNNING` - simulating a connector where only some of its tasks fail to
come up.
