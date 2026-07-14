package com.example.kafka.connect.test;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * Shared configuration for {@link TestConnector} and {@link TestTask}.
 *
 * <p>None of these knobs affect what the connector does (it never produces records) - they only
 * control how long, and how, its lifecycle methods take to run. This is used to simulate a real
 * connector plugin that does not react instantly to start/stop/restart requests, or that accepts
 * a config update but later fails to bring its tasks up.
 */
public class TestConnectorConfig extends AbstractConfig {

    public static final String START_DELAY_MS_CONFIG = "start.delay.ms";
    private static final String START_DELAY_MS_DOC =
            "Milliseconds the task blocks inside start() before returning, simulating a connector "
                    + "that takes time to come up (e.g. warming up a client, negotiating a connection).";
    public static final long START_DELAY_MS_DEFAULT = 0L;

    public static final String START_FAIL_CONFIG = "start.fail";
    private static final String START_FAIL_DOC =
            "If true, start() throws after start.delay.ms, simulating a connector that accepts the "
                    + "config but its task fails to start.";
    public static final boolean START_FAIL_DEFAULT = false;

    public static final String FAIL_AFTER_MS_CONFIG = "fail.after.ms";
    private static final String FAIL_AFTER_MS_DOC =
            "If >= 0, the task starts successfully but fails itself this many milliseconds later "
                    + "(via SourceTaskContext#raiseError), simulating a connector whose task starts running "
                    + "and only fails asynchronously afterwards. Disabled when negative.";
    public static final long FAIL_AFTER_MS_DEFAULT = -1L;

    public static final String STOP_DELAY_MS_CONFIG = "stop.delay.ms";
    private static final String STOP_DELAY_MS_DOC =
            "Milliseconds the task blocks inside stop() before returning, simulating a connector "
                    + "that takes time to release its resources when paused, stopped, or restarted. "
                    + "Because Connect always calls stop() before start() on a restart, this delay is "
                    + "also what makes a restart look 'stuck' between the two.";
    public static final long STOP_DELAY_MS_DEFAULT = 0L;

    public static final String POLL_DELAY_MS_CONFIG = "poll.delay.ms";
    private static final String POLL_DELAY_MS_DOC =
            "Milliseconds each poll() call blocks before returning no records. Only affects how "
                    + "busy the task loop is; it never emits records.";
    public static final long POLL_DELAY_MS_DEFAULT = 1000L;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(START_DELAY_MS_CONFIG, Type.LONG, START_DELAY_MS_DEFAULT, Importance.MEDIUM, START_DELAY_MS_DOC)
            .define(START_FAIL_CONFIG, Type.BOOLEAN, START_FAIL_DEFAULT, Importance.MEDIUM, START_FAIL_DOC)
            .define(FAIL_AFTER_MS_CONFIG, Type.LONG, FAIL_AFTER_MS_DEFAULT, Importance.MEDIUM, FAIL_AFTER_MS_DOC)
            .define(STOP_DELAY_MS_CONFIG, Type.LONG, STOP_DELAY_MS_DEFAULT, Importance.MEDIUM, STOP_DELAY_MS_DOC)
            .define(POLL_DELAY_MS_CONFIG, Type.LONG, POLL_DELAY_MS_DEFAULT, Importance.LOW, POLL_DELAY_MS_DOC);

    public TestConnectorConfig(Map<String, String> originals) {
        super(CONFIG_DEF, originals);
    }

    public long startDelayMs() {
        return getLong(START_DELAY_MS_CONFIG);
    }

    public boolean startFail() {
        return getBoolean(START_FAIL_CONFIG);
    }

    public long failAfterMs() {
        return getLong(FAIL_AFTER_MS_CONFIG);
    }

    public long stopDelayMs() {
        return getLong(STOP_DELAY_MS_CONFIG);
    }

    public long pollDelayMs() {
        return getLong(POLL_DELAY_MS_CONFIG);
    }
}
