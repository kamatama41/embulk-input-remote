package org.embulk.test;

import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.exec.ResumeState;
import org.embulk.spi.OutputPlugin;

import java.io.IOException;
import java.lang.reflect.Field;

public class MyTestingEmbulk extends TestingEmbulk {

    public static class Builder extends TestingEmbulk.Builder {
        public TestingEmbulk build() {
            this.registerPlugin(OutputPlugin.class, "memory", MemoryOutputPlugin.class);
            return new MyTestingEmbulk(this);
        }
    }

    public static TestingEmbulk.Builder builder()
    {
        return new MyTestingEmbulk.Builder();
    }

    private final EmbulkEmbed superEmbed;

    MyTestingEmbulk(Builder builder) {
        super(builder);
        this.superEmbed = extractSuperField("embed");
    }

    public RunResult runInput(ConfigSource inConfig) throws IOException {
        return runInput(inConfig, (ConfigDiff) null);
    }

    public RunResult runInput(ConfigSource inConfig, ConfigDiff confDiff) throws IOException {
        ConfigSource execConfig = newConfig()
                .set("min_output_tasks", 1);

        ConfigSource outConfig = newConfig()
                .set("type", "memory");

        ConfigSource config = newConfig()
                .set("exec", execConfig)
                .set("in", inConfig)
                .set("out", outConfig);

        // embed.run returns TestingBulkLoader.TestingExecutionResult because
        MemoryOutputPlugin.clearRecords();
        if (confDiff == null) {
            return (RunResult) superEmbed.run(config);
        } else {
            return (RunResult) superEmbed.run(config.merge(confDiff));
        }
    }

    public EmbulkEmbed.ResumableResult resume(ConfigSource inConfig) throws IOException {
        return resume(inConfig, null);
    }

    public EmbulkEmbed.ResumableResult resume(ConfigSource inConfig, ResumeState resumeState) throws IOException {
        ConfigSource execConfig = newConfig()
                .set("min_output_tasks", 1);

        ConfigSource outConfig = newConfig()
                .set("type", "memory");

        ConfigSource config = newConfig()
                .set("exec", execConfig)
                .set("in", inConfig)
                .set("out", outConfig);

        MemoryOutputPlugin.clearRecords();
        if (resumeState == null) {
            return superEmbed.runResumable(config);
        } else {
            return superEmbed.new ResumeStateAction(config, resumeState).resume();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T extractSuperField(String fieldName) {
        try {
            Field field = TestingEmbulk.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(this);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
