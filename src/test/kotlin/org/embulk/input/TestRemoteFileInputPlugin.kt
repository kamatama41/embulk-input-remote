package org.embulk.input;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;
import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;
import org.embulk.test.EmbulkPluginTest;
import org.embulk.test.ExtendedEmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.embulk.test.TestOutputPlugin.assertRecords;
import static org.embulk.test.Utils.record;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TestRemoteFileInputPlugin extends EmbulkPluginTest {
    private static final String CONTAINER_ID_HOST1 = "embulkinputremote_host1_1";
    private static final String CONTAINER_ID_HOST2 = "embulkinputremote_host2_1";
    private static final DockerClient dockerClient = DockerClientBuilder.getInstance().build();

    @Override
    protected void setup(TestingEmbulk.Builder builder) {
        builder.registerPlugin(InputPlugin.class, "remote", RemoteFileInputPlugin.class);

        // Setup docker container
        startContainer(CONTAINER_ID_HOST1);
        startContainer(CONTAINER_ID_HOST2);

        String logLevel = System.getenv("LOG_LEVEL");
        if (logLevel != null) {
            // Set log level
            Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            rootLogger.setLevel(Level.toLevel(logLevel));
        }
    }

    @Test
    public void loadFromRemote() throws Exception
    {
        runInput(baseConfig());
        assertRecords(record(1, "user1"));
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
        runInput(baseConfig().merge(publicKeyAuth));

        assertRecords(record(1, "user1"));
    }

    @Test
    public void testMultiHosts() throws Exception
    {
        final ConfigSource multiHosts = newConfig()
                .set("hosts", Arrays.asList("localhost:10022", "localhost:10023"));
        final ConfigSource config = baseConfig().merge(multiHosts);

        // Run
        runInput(config);
        assertRecords(
                record(1, "user1"),
                record(2, "user2")
        );
    }

    @Test
    public void loadAllFilesInDirectory() throws Exception
    {
        final ConfigSource directoryPath = newConfig()
                .set("path", "/mount");
        final ConfigSource config = baseConfig().merge(directoryPath);

        runInput(config);
        assertRecords(
                record(1L, "user1"),
                record(1L, "command_user1")
        );
    }

    @Test
    public void testDefaultPort() throws Exception
    {
        final ConfigSource defaultPort = newConfig()
                .set("hosts", Collections.singletonList("localhost"))
                .set("default_port", 10022);

        runInput(baseConfig().merge(defaultPort));

        assertRecords(record(1L, "user1"));
    }

    @Test
    public void testConfDiff() throws Exception
    {
        final ConfigSource host2Config = newConfig()
                .set("hosts", Collections.singletonList("localhost:10023"));
        ConfigSource config = baseConfig().merge(host2Config);

        // Run
        TestingEmbulk.RunResult runResult = runInput(config);
        assertRecords(record(2, "user2"));

        // Re-run with additional host1
        final ConfigSource multiHost = newConfig()
                .set("hosts", Arrays.asList("localhost:10022", "localhost:10023"));
        config = baseConfig().merge(multiHost);

        runInput(config, runResult.getConfigDiff());

        assertRecords(record(1, "user1"));
    }

    @Test
    public void testResume() throws Exception
    {
        final ConfigSource multiHost = newConfig()
                .set("hosts", Arrays.asList("localhost:10022", "localhost:10023"));
        final ConfigSource config = baseConfig().merge(multiHost);

        // Stop host2 temporarily
        stopContainer(CONTAINER_ID_HOST2);

        // Run (but will fail)
        EmbulkEmbed.ResumableResult resumableResult = resume(config);

        assertThat(resumableResult.isSuccessful(), is(false));
        assertRecords(record(1, "user1"));

        // Start host2 again
        startContainer(CONTAINER_ID_HOST2);

        // Resume
        resumableResult = resume(config, resumableResult.getResumeState());

        assertThat(resumableResult.isSuccessful(), is(true));
        assertRecords(record(2, "user2"));
    }

    @Test
    public void testIgnoreNotFoundHosts() throws Exception
    {
        final ConfigSource ignoreNotFoundHosts = newConfig()
                .set("hosts", Arrays.asList("localhost:10022", "localhost:10023"))
                .set("ignore_not_found_hosts", true);
        final ConfigSource config = baseConfig().merge(ignoreNotFoundHosts);

        // Stop host2
        stopContainer(CONTAINER_ID_HOST2);

        // Run (host2 will be ignored)
        EmbulkEmbed.ResumableResult resumableResult = resume(config);

        assertThat(resumableResult.isSuccessful(), is(true));
        assertRecords(record(1, "user1"));
    }

    @Test
    public void testCommandOptions() throws Exception
    {
        final ConfigSource ignoreNotFoundHosts = newConfig()
                .set("hosts_command", "./src/test/resources/script/hosts.sh")
                .set("hosts_separator", "\n")
                .set("path_command", "echo '/mount/test_command.csv'");
        final ConfigSource config = baseConfig().merge(ignoreNotFoundHosts);

        runInput(config);

        assertRecords(
                record(1, "command_user1"),
                record(2, "command_user2")
        );
    }

    //////////////////////////////
    // Helpers
    //////////////////////////////

    private ConfigSource baseConfig() {
        return ExtendedEmbulkTests.configFromResource("yaml/base.yml");
    }

    //////////////////////////////
    // Methods for Docker
    //////////////////////////////

    private static void stopContainer(String containerId) {
        if (isRunning(containerId)) {
            dockerClient.stopContainerCmd(containerId).exec();
        }
    }

    private static void startContainer(String containerId) {
        if (!isRunning(containerId)) {
            dockerClient.startContainerCmd(containerId).exec();
        }
    }

    private static boolean isRunning(String containerId) {
        List<Container> containers = dockerClient.listContainersCmd().exec();
        for (Container container : containers) {
            for (String name : container.getNames()) {
                if (name.contains(containerId)) {
                    System.out.println("Found " + containerId);
                    return true;
                }
            }
        }
        System.out.println("Not Found " + containerId);
        return false;
    }
}
