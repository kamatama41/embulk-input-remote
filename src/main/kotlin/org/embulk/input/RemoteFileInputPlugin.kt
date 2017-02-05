package org.embulk.input

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Optional
import com.google.common.collect.ImmutableList
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
import java.util.Arrays


class RemoteFileInputPlugin : FileInputPlugin {
    interface PluginTask : Task {
        @Config("hosts")
        @ConfigDefault("[]")
        fun getHosts(): List<String>

        @Config("hosts_command")
        @ConfigDefault("null")
        fun getHostsCommand(): Optional<String>

        @Config("hosts_separator")
        @ConfigDefault("\" \"")
        fun getHostsSeparator(): String

        @Config("default_port")
        @ConfigDefault("22")
        fun getDefaultPort(): Int

        @Config("path")
        @ConfigDefault("\"\"")
        fun getPath(): String

        @Config("path_command")
        @ConfigDefault("null")
        fun getPathCommand(): Optional<String>

        @Config("auth")
        fun getAuthConfig(): AuthConfig

        @Config("ignore_not_found_hosts")
        @ConfigDefault("false")
        fun getIgnoreNotFoundHosts(): Boolean

        @Config("done_targets")
        @ConfigDefault("[]")
        fun getDoneTargets(): List<Target>

        fun getTargets(): List<Target>

        fun setTargets(targets: List<Target>)

        @ConfigInject
        fun getBufferAllocator(): BufferAllocator
    }

    interface AuthConfig : Task {
        @Config("type")
        @ConfigDefault("\"public_key\"")
        fun getType(): String

        @Config("user")
        @ConfigDefault("null")
        fun getUser(): Optional<String>

        @Config("key_path")
        @ConfigDefault("null")
        fun getKeyPath(): Optional<String>

        @Config("password")
        @ConfigDefault("null")
        fun getPassword(): Optional<String>

        @Config("skip_host_key_verification")
        @ConfigDefault("false")
        fun getSkipHostKeyVerification(): Boolean
    }

    private val log = Exec.getLogger(javaClass)

    override fun transaction(config: ConfigSource, control: FileInputPlugin.Control): ConfigDiff {
        val task = config.loadConfig(PluginTask::class.java)
        val targets = listTargets(task)
        log.info("Loading targets $targets")
        task.setTargets(targets)

        // number of processors is same with number of targets
        val taskCount = targets.size
        return resume(task.dump(), taskCount, control)
    }

    override fun resume(taskSource: TaskSource, taskCount: Int, control: FileInputPlugin.Control): ConfigDiff {
        val task = taskSource.loadTask(PluginTask::class.java)

        control.run(taskSource, taskCount)

        return Exec.newConfigDiff().set("done_targets", task.getTargets())
    }

    override fun cleanup(taskSource: TaskSource?, taskCount: Int, successTaskReports: MutableList<TaskReport>?) {
    }

    override fun open(taskSource: TaskSource, taskIndex: Int): TransactionalFileInput {
        val task = taskSource.loadTask(PluginTask::class.java)
        val target = task.getTargets()[taskIndex]

        return object : InputStreamTransactionalFileInput(task.getBufferAllocator(), { download(target, task) }) {
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

        val builder = ImmutableList.builder<Target>()
        val doneTargets = task.getDoneTargets()
        for (host in hosts) {
            val split = host.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()
            val targetHost = split[0]
            var targetPort = task.getDefaultPort()
            if (split.size > 1) {
                targetPort = Integer.valueOf(split[1])!!
            }
            val target = Target(targetHost, targetPort, path)

            if (!doneTargets.contains(target)) {
                if (task.getIgnoreNotFoundHosts()) {
                    try {
                        val exists = exists(target, task)
                        if (!exists) {
                            continue
                        }
                    } catch (e: IOException) {
                        log.warn("failed to check the file exists. $target", e)
                        continue
                    }

                }
                builder.add(target)
            }
        }
        return builder.build()
    }

    private fun listHosts(task: PluginTask): List<String> {
        val hostsCommand = task.getHostsCommand().orNull()
        if (hostsCommand != null) {
            val stdout = execCommand(hostsCommand).trim({ it <= ' ' })
            return Arrays.asList<String>(*stdout.split(task.getHostsSeparator().toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray())
        } else {
            return task.getHosts()
        }
    }

    private fun getPath(task: PluginTask): String {
        val pathCommand = task.getPathCommand().orNull()
        if (pathCommand != null) {
            return execCommand(pathCommand).trim({ it <= ' ' })
        } else {
            return task.getPath()
        }
    }

    private fun execCommand(command: String): String {
        val pb = ProcessBuilder("sh", "-c", command)    // TODO: windows
        log.info("Running command $command")
        val process = pb.start()
        process.inputStream.use { stream ->
            BufferedReader(InputStreamReader(stream)).use { brStdout ->
                val stdout = StringBuilder()
                for (line in brStdout.readLines()) {
                    stdout.append(line)
                }

                val code = process.waitFor()
                if (code != 0) {
                    throw IOException("Command finished with non-zero exit code. Exit code is $code")
                }

                return stdout.toString()
            }
        }
    }

    private fun exists(target: Target, task: PluginTask): Boolean {
        SSHClient.connect(target.host, target.port, task.getAuthConfig()).use { client ->
            val checkCmd = "ls ${target.path}"    // TODO: windows
            val timeout = 5/* second */
            val commandResult = client.execCommand(checkCmd, timeout)

            if (commandResult.status != 0) {
                log.warn("Remote file not found. $target")
                return false
            } else {
                return true
            }
        }
    }

    private fun download(target: Target, task: PluginTask): InputStream {
        SSHClient.connect(target.host, target.port, task.getAuthConfig()).use { client ->
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
