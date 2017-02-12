package org.embulk.input

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.github.dockerjava.core.DockerClientBuilder
import org.embulk.config.ConfigSource
import org.embulk.test.EmbulkPluginTest
import org.embulk.test.TestOutputPlugin.Matcher.assertRecords
import org.embulk.test.configFromResource
import org.embulk.test.record
import org.embulk.test.registerPlugin
import org.embulk.test.set
import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.slf4j.LoggerFactory

class TestRemoteFileInputPlugin : EmbulkPluginTest() {

    @Before fun setup() {
        builder.registerPlugin(RemoteFileInputPlugin::class, "remote")

        // Setup docker container
        startContainer(CONTAINER_ID_HOST1)
        startContainer(CONTAINER_ID_HOST2)

        System.getenv("LOG_LEVEL")?.let {
            // Set log level
            val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
            rootLogger.level = Level.toLevel(it)
        }
    }

    @Test fun loadFromRemote() {
        runInput(baseConfig())
        assertRecords(record(1, "user1"))
    }

    @Ignore("Cannot pass on TravisCI, although pass on Local Mac OS...")
    @Test fun loadFromRemoteViaPublicKey() {
        val keyPath = System.getenv("KEY_PATH") ?: "./id_rsa_test"

        val publicKeyAuth = config().set("auth" to config().set(
                "type" to "public_key",
                "key_path" to keyPath
        ))
        runInput(baseConfig().merge(publicKeyAuth))

        assertRecords(record(1, "user1"))
    }

    @Test fun testMultiHosts() {
        val multiHosts = config()
                .set("hosts", listOf("localhost:10022", "localhost:10023"))

        // Run
        runInput(baseConfig().merge(multiHosts))
        assertRecords(
                record(1, "user1"),
                record(2, "user2")
        )
    }

    @Test fun loadAllFilesInDirectory() {
        val directoryPath = config().set("path", "/mount")

        runInput(baseConfig().merge(directoryPath))
        assertRecords(
                record(1L, "user1"),
                record(1L, "command_user1")
        )
    }

    @Test fun testDefaultPort() {
        val defaultPort = config().set(
                "hosts" to listOf("localhost"),
                "default_port" to 10022
        )

        runInput(baseConfig().merge(defaultPort))

        assertRecords(record(1L, "user1"))
    }

    @Test fun testConfDiff() {
        val host2Config = config().set("hosts", listOf("localhost:10023"))

        // Run
        val runResult = runInput(baseConfig().merge(host2Config))
        assertRecords(record(2, "user2"))

        // Re-run with additional host1
        val multiHost = config().set("hosts", listOf("localhost:10022", "localhost:10023"))
        runInput(baseConfig().merge(multiHost), runResult.configDiff)

        assertRecords(record(1, "user1"))
    }

    @Test fun testResume() {
        // Stop host2 temporarily
        stopContainer(CONTAINER_ID_HOST2)

        // Run (but will fail)
        val multiHost = config().set("hosts", listOf("localhost:10022", "localhost:10023"))
        val config = baseConfig().merge(multiHost)
        var resumableResult = resume(config)

        assertThat(resumableResult.isSuccessful, `is`(false))
        assertRecords(record(1, "user1"))

        // Start host2 again
        startContainer(CONTAINER_ID_HOST2)

        // Resume
        resumableResult = resume(config, resumableResult.resumeState)

        assertThat(resumableResult.isSuccessful, `is`(true))
        assertRecords(record(2, "user2"))
    }

    @Test fun testIgnoreNotFoundHosts() {
        val ignoreNotFoundHosts = config().set(
                "hosts" to listOf("localhost:10022", "localhost:10023"),
                "ignore_not_found_hosts" to true
        )
        val config = baseConfig().merge(ignoreNotFoundHosts)

        // Stop host2
        stopContainer(CONTAINER_ID_HOST2)

        // Run (host2 will be ignored)
        val resumableResult = resume(config)

        assertThat<Boolean>(resumableResult.isSuccessful, `is`(true))
        assertRecords(record(1, "user1"))
    }

    @Test fun testCommandOptions() {
        val ignoreNotFoundHosts = config().set(
                "hosts_command" to "./src/test/resources/script/hosts.sh",
                "hosts_separator" to "\n",
                "path_command" to "echo '/mount/test_command.csv'"
        )
        runInput(baseConfig().merge(ignoreNotFoundHosts))

        assertRecords(
                record(1, "command_user1"),
                record(2, "command_user2")
        )
    }

    //////////////////////////////
    // Helpers
    //////////////////////////////

    private fun baseConfig(): ConfigSource {
        return configFromResource("yaml/base.yml")
    }

    companion object DockerUtils {
        private val CONTAINER_ID_HOST1 = "embulkinputremote_host1_1"
        private val CONTAINER_ID_HOST2 = "embulkinputremote_host2_1"
        private val dockerClient = DockerClientBuilder.getInstance().build()

        private fun stopContainer(containerId: String) {
            if (isRunning(containerId)) {
                dockerClient.stopContainerCmd(containerId).exec()
            }
        }

        private fun startContainer(containerId: String) {
            if (!isRunning(containerId)) {
                dockerClient.startContainerCmd(containerId).exec()
            }
        }

        private fun isRunning(containerId: String): Boolean {
            return dockerClient.listContainersCmd().exec().any { container ->
                container.names.any { name ->
                    name.contains(containerId)
                }
            }
        }
    }
}
