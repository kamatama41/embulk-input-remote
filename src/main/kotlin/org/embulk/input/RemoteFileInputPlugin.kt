package org.embulk.input

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Optional
import org.embulk.config.Config
import org.embulk.config.ConfigDefault
import org.embulk.config.ConfigDiff
import org.embulk.config.ConfigInject
import org.embulk.config.ConfigSource
import org.embulk.config.Task
import org.embulk.config.TaskReport
import org.embulk.config.TaskSource
import org.embulk.input.remote.SSHClient
import org.embulk.spi.BufferAllocator
import org.embulk.spi.Exec
import org.embulk.spi.FileInputPlugin
import org.embulk.spi.TransactionalFileInput
import org.embulk.spi.util.InputStreamTransactionalFileInput
import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader


class RemoteFileInputPlugin : FileInputPlugin {
    interface PluginTask : Task {
        @get:Config("hosts")
        @get:ConfigDefault("[]")
        val hosts: List<String>

        @get:Config("hosts_command")
        @get:ConfigDefault("null")
        val hostsCommand: Optional<String>

        @get:Config("hosts_separator")
        @get:ConfigDefault("\" \"")
        val hostsSeparator: String

        @get:Config("default_port")
        @get:ConfigDefault("22")
        val defaultPort: Int

        @get:Config("path")
        @get:ConfigDefault("\"\"")
        val path: String

        @get:Config("path_command")
        @get:ConfigDefault("null")
        val pathCommand: Optional<String>

        @get:Config("auth")
        val authConfig: AuthConfig

        @get:Config("ignore_not_found_hosts")
        @get:ConfigDefault("false")
        val ignoreNotFoundHosts: Boolean

        @get:Config("done_targets")
        @get:ConfigDefault("[]")
        val doneTargets: List<Target>

        var targets: List<Target>

        @get:ConfigInject
        val bufferAllocator: BufferAllocator
    }

    interface AuthConfig : Task {
        @get:Config("type")
        @get:ConfigDefault("\"public_key\"")
        val type: String

        @get:Config("user")
        @get:ConfigDefault("null")
        val user: Optional<String>

        @get:Config("key_path")
        @get:ConfigDefault("null")
        val keyPath: Optional<String>

        @get:Config("password")
        @get:ConfigDefault("null")
        val password: Optional<String>

        @get:Config("skip_host_key_verification")
        @get:ConfigDefault("false")
        val skipHostKeyVerification: Boolean
    }

    private val log = Exec.getLogger(javaClass)

    override fun transaction(config: ConfigSource, control: FileInputPlugin.Control): ConfigDiff {
        val task = config.loadConfig(PluginTask::class.java)
        val targets = listTargets(task)
        log.info("Loading targets $targets")
        task.targets = targets

        // number of processors is same with number of targets
        val taskCount = targets.size
        return resume(task.dump(), taskCount, control)
    }

    override fun resume(taskSource: TaskSource, taskCount: Int, control: FileInputPlugin.Control): ConfigDiff {
        val task = taskSource.loadTask(PluginTask::class.java)

        control.run(taskSource, taskCount)

        return Exec.newConfigDiff().set("done_targets", task.targets)
    }

    override fun cleanup(taskSource: TaskSource, taskCount: Int, successTaskReports: MutableList<TaskReport>) {
    }

    override fun open(taskSource: TaskSource, taskIndex: Int): TransactionalFileInput {
        val task = taskSource.loadTask(PluginTask::class.java)
        val target = task.targets[taskIndex]

        return object : InputStreamTransactionalFileInput(task.bufferAllocator, { download(target, task) }) {
            override fun abort() {
            }

            override fun commit(): TaskReport {
                return Exec.newTaskReport()
            }
        }
    }

    private fun listTargets(task: PluginTask): List<Target> {
        val hosts = listHosts(task)
        val path = getPath(task)
        val doneTargets = task.doneTargets
        val ignoreNotFoundHosts = task.ignoreNotFoundHosts

        return hosts.map {
            val split = it.split(Regex(":"))
            val host = split[0]
            val port = if (split.size > 1) split[1].toInt() else task.defaultPort
            Target(host, port, path)
        }.filter {
            !doneTargets.contains(it)
        }.filter {
            !ignoreNotFoundHosts || try {
                exists(it, task)
            } catch (e: IOException) {
                log.warn("failed to check the file exists. $it", e)
                false
            }
        }
    }

    private fun listHosts(task: PluginTask): List<String> {
        return task.hostsCommand.transform {
            execCommand(it).split(task.hostsSeparator.toRegex())
        }.or(task.hosts)
    }

    private fun getPath(task: PluginTask): String {
        return task.pathCommand.transform { execCommand(it) }.or(task.path)
    }

    private fun execCommand(command: String?): String {
        val pb = ProcessBuilder("sh", "-c", command)    // TODO: windows
        log.info("Running command $command")
        val process = pb.start()
        process.inputStream.use { stream ->
            BufferedReader(InputStreamReader(stream)).use { brStdout ->
                val stdout = brStdout.readText()

                val code = process.waitFor()
                if (code != 0) {
                    throw IOException("Command finished with non-zero exit code. Exit code is $code")
                }

                return stdout.trim()
            }
        }
    }

    private fun exists(target: Target, task: PluginTask): Boolean {
        SSHClient.connect(target.host, target.port, task.authConfig).use { client ->
            val checkCmd = "ls ${target.path}"    // TODO: windows
            val timeout = 5 /* seconds */
            val commandResult = client.execCommand(checkCmd, timeout)

            if (commandResult.status != 0) {
                log.warn("Remote file not found. $target")
                return false
            }
            return true
        }
    }

    private fun download(target: Target, task: PluginTask): InputStream {
        SSHClient.connect(target.host, target.port, task.authConfig).use { client ->
            val stream = ByteArrayOutputStream()
            client.scpDownload(target.path, stream)
            return ByteArrayInputStream(stream.toByteArray())
        }
    }

    data class Target constructor(
            @JsonProperty("host") val host: String,
            @JsonProperty("port") val port: Int,
            @JsonProperty("path") val path: String) {

        override fun toString(): String {
            return "$host:$port:$path"
        }
    }
}
