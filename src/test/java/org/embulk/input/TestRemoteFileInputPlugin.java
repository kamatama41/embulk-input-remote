package org.embulk.input;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;
import org.embulk.test.MemoryOutputPlugin;
import org.embulk.test.MyEmbulkTests;
import org.embulk.test.MyTestingEmbulk;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Enclosed.class)
public class TestRemoteFileInputPlugin
{
    public static class TestForOneHost extends TestBase {
        @Test
        public void loadFromRemote() throws Exception
        {
            embulk.runInput(baseConfig());

            List<MemoryOutputPlugin.Record> records = MemoryOutputPlugin.getRecords();
            assertThat(records.get(0).getValues(), is(Arrays.<Object>asList(1L, "kamatama41")));
            assertThat(records.get(1).getValues(), is(Arrays.<Object>asList(2L, "kamatama42")));
        }

        @Ignore("Cannot pass on TravisCI, although pass on Local Mac OS...")
        @Test
        public void loadFromRemoteViaPublicKey() throws Exception
        {
            String keyPath = System.getenv("KEY_PATH");
            if (keyPath == null) {
                keyPath = "./id_rsa_test";
            }

            final ConfigSource publicKeyAuth = newConfig().set("auth", newConfig()
                    .set("type", "public_key")
                    .set("key_path", keyPath)
            );
            embulk.runInput(baseConfig().merge(publicKeyAuth));

            List<MemoryOutputPlugin.Record> records = MemoryOutputPlugin.getRecords();
            assertThat(records.get(0).getValues(), is(Arrays.<Object>asList(1L, "kamatama41")));
            assertThat(records.get(1).getValues(), is(Arrays.<Object>asList(2L, "kamatama42")));
        }

        @Test
        public void testDefaultPort() throws Exception
        {
            final ConfigSource defaultPort = newConfig()
                    .set("hosts", Collections.singletonList("localhost"))
                    .set("default_port", 10022);

            embulk.runInput(baseConfig().merge(defaultPort));

            List<MemoryOutputPlugin.Record> records = MemoryOutputPlugin.getRecords();
            assertThat(records.get(0).getValues(), is(Arrays.<Object>asList(1L, "kamatama41")));
            assertThat(records.get(1).getValues(), is(Arrays.<Object>asList(2L, "kamatama42")));
        }
    }

    public abstract static class TestBase {
        @Rule
        public MyTestingEmbulk embulk = (MyTestingEmbulk) MyTestingEmbulk
                .builder()
                .registerPlugin(InputPlugin.class, "remote", RemoteFileInputPlugin.class)
                .build();

        @Before
        public void prepare() {
            // Show degub logs
            Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            rootLogger.setLevel(Level.toLevel("debug"));
        }

        ConfigSource baseConfig() {
            return MyEmbulkTests.configFromResource("yaml/base.yml");
        }

        ConfigSource newConfig() {
            return embulk.newConfig();
        }
    }
}
