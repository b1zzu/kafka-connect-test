package com.example.kafka.connect.test;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Kafka Connect source connector that never produces any records and only exists to simulate
 * lifecycle behaviours real connectors exhibit that the naive "the operator asked, therefore it is
 * done" assumption gets wrong:
 *
 * <ul>
 *   <li>Starting up takes time, either at the connector level ({@code start.delay.ms}) or the task
 *       level ({@code start.tasks.delay.ms}).
 *   <li>Stopping / pausing takes time ({@code stop.delay.ms}) - and since Connect always calls
 *       stop() before start() on a restart, this is also what makes a restart look "stuck".
 *   <li>The connector itself fails to come up ({@code start.fail}), or accepts the config but its
 *       tasks fail to start ({@code start.tasks.fail}).
 *   <li>A task starts fine but fails on its own some time later ({@code fail.after.ms}).
 * </ul>
 *
 * Runs exactly {@code tasks.max} tasks, each simulating the same configured behaviour.
 */
public class TestConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(TestConnector.class);

    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        TestConnectorConfig config = new TestConnectorConfig(props);

        long startDelayMs = config.startDelayMs();
        if (startDelayMs > 0) {
            log.info("Simulating connector start delay of {}ms", startDelayMs);
            sleep(startDelayMs);
        }

        if (config.startFail()) {
            log.info("Simulating connector start failure");
            throw new ConnectException("simulated failure: connector configured with " + TestConnectorConfig.START_FAIL_CONFIG + "=true");
        }

        this.configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TestTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        int failCount = new TestConnectorConfig(configProps).startTasksFailCount();

        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            if (failCount < 0) {
                configs.add(configProps);
            } else {
                Map<String, String> taskProps = new HashMap<>(configProps);
                taskProps.put(TestConnectorConfig.START_TASKS_FAIL_CONFIG, String.valueOf(i < failCount));
                configs.add(taskProps);
            }
        }
        return configs;
    }

    @Override
    public void stop() {
        // No resources held at the connector level, only the task simulates delays.
    }

    @Override
    public ConfigDef config() {
        return TestConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
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
