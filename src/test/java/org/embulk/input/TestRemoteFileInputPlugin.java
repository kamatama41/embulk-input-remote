package org.embulk.input;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;
import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;
import org.embulk.test.MemoryOutputPlugin;
import org.embulk.test.MyEmbulkTests;
import org.embulk.test.MyTestingEmbulk;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TestRemoteFileInputPlugin {
    private static final String CONTAINER_ID_HOST1 = "embulkinputremote_host1_1";
    private static final String CONTAINER_ID_HOST2 = "embulkinputremote_host2_1";
    private static final DockerClient dockerClient = DockerClientBuilder.getInstance().build();

    @Rule
    public MyTestingEmbulk embulk = (MyTestingEmbulk) MyTestingEmbulk
            .builder()
            .registerPlugin(InputPlugin.class, "remote", RemoteFileInputPlugin.class)
            .build();

    @Before
    public void prepare() {
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
        embulk.runInput(baseConfig());
        assertValues(values(1L, "user1"));
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

        assertValues(values(1L, "user1"));
    }

    @Test
    public void testMultiHosts() throws Exception
    {
        final ConfigSource multiHosts = newConfig()
                .set("hosts", Arrays.asList("localhost:10022", "localhost:10023"));
        final ConfigSource config = baseConfig().merge(multiHosts);

        // Run
        embulk.runInput(config);
        assertValues(
                values(1L, "user1"),
                values(2L, "user2")
        );
    }

    @Test
    public void loadAllFilesInDirectory() throws Exception
    {
        final ConfigSource multiHosts = newConfig()
                .set("path", "/mount");
        final ConfigSource config = baseConfig().merge(multiHosts);

        embulk.runInput(config);
        assertValues(
                values(1L, "user1"),
                values(1L, "command_user1")
        );
    }

    @Test
    public void testDefaultPort() throws Exception
    {
        final ConfigSource defaultPort = newConfig()
                .set("hosts", Collections.singletonList("localhost"))
                .set("default_port", 10022);

        embulk.runInput(baseConfig().merge(defaultPort));

        assertValues(values(1L, "user1"));
    }

    @Test
    public void testConfDiff() throws Exception
    {
        final ConfigSource host2Config = newConfig()
                .set("hosts", Collections.singletonList("localhost:10023"));
        ConfigSource config = baseConfig().merge(host2Config);

        // Run
        TestingEmbulk.RunResult runResult = embulk.runInput(config);
        assertValues(values(2L, "user2"));

        // Re-run with additional host1
        final ConfigSource multiHost = newConfig()
                .set("hosts", Arrays.asList("localhost:10022", "localhost:10023"));
        config = baseConfig().merge(multiHost);

        embulk.runInput(config, runResult.getConfigDiff());

        assertValues(values(1L, "user1"));
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
        EmbulkEmbed.ResumableResult resumableResult = embulk.resume(config);

        assertThat(resumableResult.isSuccessful(), is(false));
        assertValues(values(1L, "user1"));

        // Start host2 again
        startContainer(CONTAINER_ID_HOST2);

        // Resume
        resumableResult = embulk.resume(config, resumableResult.getResumeState());

        assertThat(resumableResult.isSuccessful(), is(true));
        assertValues(values(2L, "user2"));
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
        EmbulkEmbed.ResumableResult resumableResult = embulk.resume(config);

        assertThat(resumableResult.isSuccessful(), is(true));
        assertValues(values(1L, "user1"));
    }

    @Test
    public void testCommandOptions() throws Exception
    {
        final ConfigSource ignoreNotFoundHosts = newConfig()
                .set("hosts_command", "echo localhost:10022,localhost:10023")
                .set("hosts_separator", ",")
                .set("path_command", "echo /mount/test_command.csv");
        final ConfigSource config = baseConfig().merge(ignoreNotFoundHosts);

        embulk.runInput(config);

        assertValues(
                values(1L, "command_user1"),
                values(2L, "command_user2")
        );
    }

    //////////////////////////////
    // Helpers
    //////////////////////////////

    private ConfigSource baseConfig() {
        return MyEmbulkTests.configFromResource("yaml/base.yml");
    }

    private ConfigSource newConfig() {
        return embulk.newConfig();
    }

    private void assertValues(List... valuesList) {
        Set<List> actual = new HashSet<>();
        for (MemoryOutputPlugin.Record record : MemoryOutputPlugin.getRecords()) {
            actual.add(record.getValues());
        }

        Set<List> expected = new HashSet<>();
        Collections.addAll(expected, valuesList);

        assertThat(actual, is(expected));
    }

    private List values(Object... values) {
        return Arrays.asList(values);
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
