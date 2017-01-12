package org.embulk.input;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.embulk.test.EmbulkTests.readResource;
import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TestRemoteFileInputPlugin
{
    @Rule
    public TestingEmbulk embulk = TestingEmbulk
            .builder()
            .registerPlugin(InputPlugin.class, "remote", RemoteFileInputPlugin.class)
            .build();

    @Before
    public void prepare() {
        // Show degub logs
        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.toLevel("debug"));
    }

    @Test
    public void loadFromRemote() throws Exception
    {
        ConfigSource baseConfig = EmbulkTests.config("BASE_YAML");
        Path out = embulk.createTempFile("csv");

        embulk.runInput(baseConfig, out);

        assertThat(
                readSortedFile(out),
                is(readResource("expect/test01.csv")));
    }

    @Ignore("Cannot pass on TravisCI, although pass on Local Mac OS...")
    @Test
    public void loadFromRemoteViaPublicKey() throws Exception
    {
        ConfigSource baseConfig = EmbulkTests.config("BASE_YAML");
        final String yaml = ""
                + "auth:\n"
                + "  type: public_key\n"
                + "  key_path: " + System.getenv("KEY_PATH") + "\n";
        final ConfigSource publicKeyAuth = embulk.configLoader().fromYamlString(yaml);
        Path out = embulk.createTempFile("csv");

        embulk.runInput(baseConfig.merge(publicKeyAuth), out);

        assertThat(
                readSortedFile(out),
                is(readResource("expect/test01.csv")));
    }

    @Test
    public void testDefaultPort() throws Exception
    {
        ConfigSource baseConfig = EmbulkTests.config("BASE_YAML");
        final String yaml = ""
                + "hosts:\n"
                + "  - localhost\n"
                + "default_port: 10022\n";
        final ConfigSource defaultPort = embulk.configLoader().fromYamlString(yaml);

        Path out = embulk.createTempFile("csv");

        embulk.runInput(baseConfig.merge(defaultPort), out);

        assertThat(
                readSortedFile(out),
                is(readResource("expect/test01.csv")));
    }
}
