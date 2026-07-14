package com.example.kafka.connect.test;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * The task backing {@link TestConnector}. Never emits records; every method that Connect calls
 * synchronously (start/stop/poll) can be made to block for a configured duration, and the task can
 * be made to fail either immediately in start() or asynchronously after it is already running.
 */
public class TestTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(TestTask.class);

    private TestConnectorConfig config;
    // Deadline (System.currentTimeMillis()) at which poll() should throw to simulate an
    // asynchronous task failure, or -1 if disabled. There is no SourceTaskContext hook to fail a
    // task out-of-band, so poll() - the only method Connect calls in a loop while running - is
    // where we check for it instead.
    private long failAtMs = -1;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new TestConnectorConfig(props);

        long startDelayMs = config.startTasksDelayMs();
        if (startDelayMs > 0) {
            log.info("Simulating start delay of {}ms", startDelayMs);
            sleep(startDelayMs);
        }

        if (config.startTasksFail()) {
            long failAfterMs = config.failAfterMs();
            if (failAfterMs < 0) {
                log.info("Simulating start failure");
                throw new ConnectException("simulated failure: task configured with " + TestConnectorConfig.START_TASKS_FAIL_CONFIG + "=true");
            }
            log.info("Task will simulate an asynchronous failure in {}ms", failAfterMs);
            failAtMs = System.currentTimeMillis() + failAfterMs;
        }

        log.info("Task started");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (failAtMs >= 0 && System.currentTimeMillis() >= failAtMs) {
            log.info("Simulating asynchronous task failure now");
            throw new ConnectException(
                    "simulated failure: task configured with " + TestConnectorConfig.START_TASKS_FAIL_CONFIG
                            + "=true, " + TestConnectorConfig.FAIL_AFTER_MS_CONFIG + "=" + config.failAfterMs());
        }

        long pollDelayMs = config.pollDelayMs();
        if (pollDelayMs > 0) {
            Thread.sleep(pollDelayMs);
        }
        // Never produce any records - this connector only simulates lifecycle timing.
        return null;
    }

    @Override
    public void stop() {
        long stopDelayMs = config != null ? config.stopTasksDelayMs() : TestConnectorConfig.STOP_TASKS_DELAY_MS_DEFAULT;
        if (stopDelayMs > 0) {
            log.info("Simulating stop delay of {}ms", stopDelayMs);
            sleep(stopDelayMs);
        }

        log.info("Task stopped");
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("interrupted while simulating delay", e);
        }
    }
}
