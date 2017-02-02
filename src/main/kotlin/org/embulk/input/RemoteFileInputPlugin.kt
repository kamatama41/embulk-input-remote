package org.embulk.input

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Optional
import com.google.common.collect.ImmutableList
import org.embulk.config.*
import org.embulk.input.remote.connect
import org.embulk.spi.BufferAllocator
import org.embulk.spi.Exec
import org.embulk.spi.FileInputPlugin
import org.embulk.spi.TransactionalFileInput
import org.embulk.spi.util.InputStreamFileInput
import org.embulk.spi.util.InputStreamTransactionalFileInput
import java.io.*
import java.util.*

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

        fun setDoneTargets(lastTarget: List<Target>)

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
        log.info("Loading targets {}", targets)
        task.setTargets(targets)

        // number of processors is same with number of targets
        val taskCount = targets.size
        return resume(task.dump(), taskCount, control)
    }

    override fun resume(taskSource: TaskSource, taskCount: Int, control: FileInputPlugin.Control): ConfigDiff {
        val task = taskSource.loadTask(PluginTask::class.java)

        control.run(taskSource, taskCount)

        val targets = ArrayList<Target>(task.getTargets())

        return Exec.newConfigDiff().set("done_targets", targets)
    }

    override fun cleanup(taskSource: TaskSource?, taskCount: Int, successTaskReports: MutableList<TaskReport>?) {
    }

    override fun open(taskSource: TaskSource, taskIndex: Int): TransactionalFileInput {
        val task = taskSource.loadTask(PluginTask::class.java)
        val target = task.getTargets()[taskIndex]

        return object : InputStreamTransactionalFileInput(
                task.getBufferAllocator(),
                InputStreamFileInput.Opener { download(target, task) }
        ) {
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
                        log.warn("failed to check the file exists. " + target.toString(), e)
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
        log.info("Running command {}", command)
        try {
            val process = pb.start()
            process.inputStream.use { stream ->
                BufferedReader(InputStreamReader(stream)).use { brStdout ->
                    val stdout = StringBuilder()
                    for (line in brStdout.readLines()) {
                        stdout.append(line)
                    }

                    val code = process.waitFor()
                    if (code != 0) {
                        throw IOException(String.format(
                                "Command finished with non-zero exit code. Exit code is %d.", code))
                    }

                    return stdout.toString()
                }
            }
        } catch (e: IOException) {
            throw RuntimeException(e)
        } catch (e: InterruptedException) {
            throw RuntimeException(e)
        }
    }

    private fun exists(target: Target, task: PluginTask): Boolean {
        connect(target.host, target.port, task.getAuthConfig()).use { client ->
            val checkCmd = "ls " + target.path    // TODO: windows
            val timeout = 5/* second */
            val commandResult = client.execCommand(checkCmd, timeout)

            if (commandResult.status != 0) {
                log.warn("Remote file not found. {}", target.toString())
                return false
            } else {
                return true
            }
        }
    }

    private fun download(target: Target, task: PluginTask): InputStream {
        connect(target.host, target.port, task.getAuthConfig()).use { client ->
            val stream = ByteArrayOutputStream()
            client.scpDownload(target.path, stream)
            return ByteArrayInputStream(stream.toByteArray())
        }
    }

    class Target {
        val host: String
        val port: Int
        val path: String

        @JsonCreator
        constructor(
                @JsonProperty("host") host: String,
                @JsonProperty("port") port: Int,
                @JsonProperty("path") path: String) {
            this.host = host
            this.port = port
            this.path = path
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other?.javaClass != javaClass) return false

            other as Target

            if (host != other.host) return false
            if (port != other.port) return false
            if (path != other.path) return false

            return true
        }

        override fun hashCode(): Int {
            var result = host.hashCode()
            result = 31 * result + port
            result = 31 * result + path.hashCode()
            return result
        }

        override fun toString(): String {
            return "$host:$port:$path"
        }
    }

}
