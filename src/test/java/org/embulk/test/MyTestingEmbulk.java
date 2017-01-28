package org.embulk.test;

import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.exec.ResumeState;
import org.embulk.spi.OutputPlugin;

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

    public RunResult runInput(ConfigSource inConfig) {
        return runInput(inConfig, (ConfigDiff) null);
    }

    public RunResult runInput(ConfigSource inConfig, ConfigDiff confDiff) {
        MemoryOutputPlugin.clearRecords();
        return new RunConfig()
                .inConfig(inConfig)
                .configDiff(confDiff)
                .execConfig(newConfig().set("min_output_tasks", 1))
                .outConfig(newConfig().set("type", "memory"))
                .run();
    }

    public EmbulkEmbed.ResumableResult resume(ConfigSource inConfig) {
        return resume(inConfig, null);
    }

    public EmbulkEmbed.ResumableResult resume(ConfigSource inConfig, ResumeState resumeState) {
        MemoryOutputPlugin.clearRecords();
        return new RunConfig()
                .inConfig(inConfig)
                .resumeState(resumeState)
                .execConfig(newConfig().set("min_output_tasks", 1))
                .outConfig(newConfig().set("type", "memory"))
                .resume();
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

    private class RunConfig {
        private ConfigSource inConfig;
        private ConfigSource execConfig;
        private ConfigSource outConfig;
        private ConfigDiff configDiff;
        private ResumeState resumeState;

        private RunConfig() {}

        RunConfig inConfig(ConfigSource inConfig) {
            this.inConfig = inConfig;
            return this;
        }

        RunConfig execConfig(ConfigSource execConfig) {
            this.execConfig = execConfig;
            return this;
        }

        RunConfig outConfig(ConfigSource outConfig) {
            this.outConfig = outConfig;
            return this;
        }

        RunConfig configDiff(ConfigDiff configDiff) {
            this.configDiff = configDiff;
            return this;
        }

        RunConfig resumeState(ResumeState resumeState) {
            this.resumeState = resumeState;
            return this;
        }

        RunResult run() {
            ConfigSource config = newConfig()
                    .set("exec", execConfig)
                    .set("in", inConfig)
                    .set("out", outConfig);
            // embed.run returns TestingBulkLoader.TestingExecutionResult because
            if (configDiff == null) {
                return (RunResult) superEmbed.run(config);
            } else {
                return (RunResult) superEmbed.run(config.merge(configDiff));
            }
        }

        EmbulkEmbed.ResumableResult resume() {
            ConfigSource config = newConfig()
                    .set("exec", execConfig)
                    .set("in", inConfig)
                    .set("out", outConfig);
            if (resumeState == null) {
                return superEmbed.runResumable(config);
            } else {
                return superEmbed.new ResumeStateAction(config, resumeState).resume();
            }
        }
    }
}
