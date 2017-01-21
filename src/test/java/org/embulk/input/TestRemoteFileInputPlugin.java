package org.embulk.input;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;
import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;
import org.embulk.test.MemoryOutputPlugin;
import org.embulk.test.MyEmbulkTests;
import org.embulk.test.MyTestingEmbulk;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Enclosed.class)
public class TestRemoteFileInputPlugin {
    public static class OneHost extends TestBase {
        @BeforeClass
        public static void startContainers() {
            startContainer(CONTAINER_ID_HOST1);
        }

        @Test
        public void loadFromRemote() throws Exception
        {
            embulk.runInput(baseConfig());

            assertValues(
                    values(1L, "kamatama41"),
                    values(2L, "kamatama42")
            );
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

            assertValues(
                    values(1L, "kamatama41"),
                    values(2L, "kamatama42")
            );
        }

        @Test
        public void testDefaultPort() throws Exception
        {
            final ConfigSource defaultPort = newConfig()
                    .set("hosts", Collections.singletonList("localhost"))
                    .set("default_port", 10022);

            embulk.runInput(baseConfig().merge(defaultPort));

            assertValues(
                    values(1L, "kamatama41"),
                    values(2L, "kamatama42")
            );
        }
    }

    public static class MultiHost extends TestBase {
        @BeforeClass
        public static void startContainers() {
            startContainer(CONTAINER_ID_HOST1);
            startContainer(CONTAINER_ID_HOST2);
        }

        @Override
        ConfigSource baseConfig() {
            ConfigSource baseConfig = super.baseConfig();
            final ConfigSource multiHost = newConfig()
                    .set("hosts", Arrays.asList("localhost:10022", "localhost:10023"));
            return baseConfig.merge(multiHost);
        }

        @Test
        public void testDefault() throws Exception
        {
            embulk.runInput(baseConfig());

            assertValues(
                    values(1L, "kamatama41"),
                    values(2L, "kamatama42"),
                    values(3L, "kamatama43"),
                    values(4L, "kamatama44")
            );
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
            String logLevel = System.getenv("LOG_LEVEL");
            if (logLevel != null) {
                // Set log level
                Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
                rootLogger.setLevel(Level.toLevel(logLevel));
            }
        }

        ConfigSource baseConfig() {
            return MyEmbulkTests.configFromResource("yaml/base.yml");
        }

        ConfigSource newConfig() {
            return embulk.newConfig();
        }

        void assertValues(List... valuesList) {
            List<MemoryOutputPlugin.Record> records = MemoryOutputPlugin.getRecords();

            Set<List> actual = new HashSet<>();
            for (MemoryOutputPlugin.Record record : records) {
                actual.add(record.getValues());
            }

            Set<List> expected = new HashSet<>();
            Collections.addAll(expected, valuesList);

            assertThat(actual, is(expected));
        }

        List values(Object... values) {
            return Arrays.asList(values);
        }

        //////////////////////////////
        // Methods for Docker
        //////////////////////////////

        static final String CONTAINER_ID_HOST1 = "test_sshd1";
        static final String CONTAINER_ID_HOST2 = "test_sshd2";
        private static final DockerClient dockerClient = DockerClientBuilder.getInstance().build();

        static void stopContainer(String containerId) {
            if (isRunning(containerId)) {
                dockerClient.stopContainerCmd(containerId).exec();
            }
        }

        static void startContainer(String containerId) {
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
}
