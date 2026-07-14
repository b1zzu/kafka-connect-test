package com.example.kafka.connect.test;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A Kafka Connect source connector that never produces any records and only exists to simulate
 * lifecycle behaviours real connectors exhibit that the naive "the operator asked, therefore it is
 * done" assumption gets wrong:
 *
 * <ul>
 *   <li>Starting up takes time ({@code start.delay.ms}).
 *   <li>Stopping / pausing takes time ({@code stop.delay.ms}) - and since Connect always calls
 *       stop() before start() on a restart, this is also what makes a restart look "stuck".
 *   <li>The task accepts the config but fails to come up ({@code start.fail}).
 *   <li>The task starts fine but fails on its own some time later ({@code fail.after.ms}).
 * </ul>
 *
 * Runs exactly {@code tasks.max} tasks, each simulating the same configured behaviour.
 */
public class TestConnector extends SourceConnector {

    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        // Validate eagerly so a bad config is rejected at PUT /connectors time.
        new TestConnectorConfig(props);
        this.configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TestTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(configProps);
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
}
