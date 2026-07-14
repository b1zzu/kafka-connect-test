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
| `start.delay.ms` | long | `0` | How long the task blocks inside `start()` before returning. |
| `start.fail` | boolean | `false` | If `true`, `start()` throws (after `start.delay.ms`), so the connector accepts the config but the task fails to come up. |
| `fail.after.ms` | long | `-1` | If `>= 0`, the task starts successfully but fails itself this many ms later. Disabled when negative. |
| `stop.delay.ms` | long | `0` | How long the task blocks inside `stop()` before returning. |
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
    "start.delay.ms": "5000",
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

## Example: simulate a config update that can't start

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

The connector is created/updated successfully (the config is valid), but the task immediately goes
to `FAILED`.
