package org.embulk.input.remote

import net.schmizz.sshj.DefaultConfig
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.xfer.InMemoryDestFile
import net.schmizz.sshj.xfer.LocalDestFile
import org.embulk.input.RemoteFileInputPlugin
import java.io.Closeable
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.TimeUnit

class SSHClient private constructor(val client: net.schmizz.sshj.SSHClient) : Closeable {
    companion object {
        fun connect(host: String, port: Int, authConfig: RemoteFileInputPlugin.AuthConfig): SSHClient {
            val client = SSHClient(net.schmizz.sshj.SSHClient(DefaultConfig()))
            client.connectToHost(host, port, authConfig)
            return client
        }
    }

    private fun connectToHost(host: String, port: Int, authConfig: RemoteFileInputPlugin.AuthConfig) {
        if (authConfig.getSkipHostKeyVerification()) {
            client.addHostKeyVerifier(PromiscuousVerifier())
        }
        client.loadKnownHosts()
        client.connect(host, port)

        val type = authConfig.getType()
        val user = authConfig.getUser().or(System.getProperty("user.name"))

        when (type) {
            "password" -> {
                client.authPassword(user, authConfig.getPassword().get())
            }
            "public_key" -> {
                authConfig.getKeyPath().transform {
                    client.authPublickey(user, it)
                }.or {
                    client.authPublickey(user)
                }
            }
            else -> {
                throw UnsupportedOperationException("Unsupported auth type : $type")
            }
        }
    }

    fun execCommand(command: String, timeoutSecond: Int): CommandResult {
        client.startSession().use { session ->
            val cmd = session.exec(command)
            cmd.join(timeoutSecond.toLong(), TimeUnit.SECONDS)
            return CommandResult(cmd.exitStatus, cmd.inputStream)
        }
    }

    fun scpDownload(path: String, stream: OutputStream) {
        client.useCompression()
        client.newSCPFileTransfer().download(path, object : InMemoryDestFile() {
            override fun getOutputStream(): OutputStream {
                return stream
            }

            override fun getTargetDirectory(dirname: String): LocalDestFile {
                return this
            }
        })
    }

    override fun close() {
        client.close()
    }

    data class CommandResult(val status: Int, val stdout: InputStream)
}
