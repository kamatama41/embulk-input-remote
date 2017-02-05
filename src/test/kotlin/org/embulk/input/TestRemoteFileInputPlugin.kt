package org.embulk.input

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.github.dockerjava.core.DockerClientBuilder
import org.embulk.config.ConfigSource
import org.embulk.spi.InputPlugin
import org.embulk.test.EmbulkPluginTest
import org.embulk.test.ExtendedEmbulkTests
import org.embulk.test.TestOutputPlugin.assertRecords
import org.embulk.test.TestingEmbulk
import org.embulk.test.Utils.record
import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Ignore
import org.junit.Test
import org.slf4j.LoggerFactory
import java.util.Arrays

class TestRemoteFileInputPlugin : EmbulkPluginTest() {
    private val CONTAINER_ID_HOST1 = "embulkinputremote_host1_1"
    private val CONTAINER_ID_HOST2 = "embulkinputremote_host2_1"
    private val dockerClient = DockerClientBuilder.getInstance().build()

    override fun setup(builder: TestingEmbulk.Builder) {
        builder.registerPlugin(InputPlugin::class.java, "remote", RemoteFileInputPlugin::class.java)

        // Setup docker container
        startContainer(CONTAINER_ID_HOST1)
        startContainer(CONTAINER_ID_HOST2)

        val logLevel = System.getenv("LOG_LEVEL")
        if (logLevel != null) {
            // Set log level
            val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
            rootLogger.level = Level.toLevel(logLevel)
        }
    }

    @Test fun loadFromRemote() {
        runInput(baseConfig())
        assertRecords(record(1, "user1"))
    }

    @Ignore("Cannot pass on TravisCI, although pass on Local Mac OS...")
    @Test fun loadFromRemoteViaPublicKey() {
        var keyPath: String? = System.getenv("KEY_PATH")
        if (keyPath == null) {
            keyPath = "./id_rsa_test"
        }

        val publicKeyAuth = newConfig().set("auth", newConfig()
                .set("type", "public_key")
                .set("key_path", keyPath)
        )
        runInput(baseConfig().merge(publicKeyAuth))

        assertRecords(record(1, "user1"))
    }

    @Test fun testMultiHosts() {
        val multiHosts = newConfig()
                .set("hosts", Arrays.asList("localhost:10022", "localhost:10023"))
        val config = baseConfig().merge(multiHosts)

        // Run
        runInput(config)
        assertRecords(
                record(1, "user1"),
                record(2, "user2")
        )
    }

    @Test fun loadAllFilesInDirectory() {
        val directoryPath = newConfig()
                .set("path", "/mount")
        val config = baseConfig().merge(directoryPath)

        runInput(config)
        assertRecords(
                record(1L, "user1"),
                record(1L, "command_user1")
        )
    }

    @Test fun testDefaultPort() {
        val defaultPort = newConfig()
                .set("hosts", listOf("localhost"))
                .set("default_port", 10022)

        runInput(baseConfig().merge(defaultPort))

        assertRecords(record(1L, "user1"))
    }

    @Test fun testConfDiff() {
        val host2Config = newConfig()
                .set("hosts", listOf("localhost:10023"))
        var config = baseConfig().merge(host2Config)

        // Run
        val runResult = runInput(config)
        assertRecords(record(2, "user2"))

        // Re-run with additional host1
        val multiHost = newConfig()
                .set("hosts", Arrays.asList("localhost:10022", "localhost:10023"))
        config = baseConfig().merge(multiHost)

        runInput(config, runResult.configDiff)

        assertRecords(record(1, "user1"))
    }

    @Test fun testResume() {
        val multiHost = newConfig()
                .set("hosts", Arrays.asList("localhost:10022", "localhost:10023"))
        val config = baseConfig().merge(multiHost)

        // Stop host2 temporarily
        stopContainer(CONTAINER_ID_HOST2)

        // Run (but will fail)
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
        val ignoreNotFoundHosts = newConfig()
                .set("hosts", Arrays.asList("localhost:10022", "localhost:10023"))
                .set("ignore_not_found_hosts", true)
        val config = baseConfig().merge(ignoreNotFoundHosts)

        // Stop host2
        stopContainer(CONTAINER_ID_HOST2)

        // Run (host2 will be ignored)
        val resumableResult = resume(config)

        assertThat<Boolean>(resumableResult.isSuccessful, `is`(true))
        assertRecords(record(1, "user1"))
    }

    @Test fun testCommandOptions() {
        val ignoreNotFoundHosts = newConfig()
                .set("hosts_command", "./src/test/resources/script/hosts.sh")
                .set("hosts_separator", "\n")
                .set("path_command", "echo '/mount/test_command.csv'")
        val config = baseConfig().merge(ignoreNotFoundHosts)

        runInput(config)

        assertRecords(
                record(1, "command_user1"),
                record(2, "command_user2")
        )
    }

    //////////////////////////////
    // Helpers
    //////////////////////////////

    private fun baseConfig(): ConfigSource {
        return ExtendedEmbulkTests.configFromResource("yaml/base.yml")
    }

    //////////////////////////////
    // Methods for Docker
    //////////////////////////////

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
        val containers = dockerClient.listContainersCmd().exec()
        for (container in containers) {
            for (name in container.names) {
                if (name.contains(containerId)) {
                    println("Found " + containerId)
                    return true
                }
            }
        }
        println("Not Found " + containerId)
        return false
    }
}
