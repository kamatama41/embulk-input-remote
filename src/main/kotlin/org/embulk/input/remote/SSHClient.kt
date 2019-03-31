package org.embulk.input.remote

import net.schmizz.sshj.DefaultConfig
import net.schmizz.sshj.SSHClient as SSHJ
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.xfer.InMemoryDestFile
import net.schmizz.sshj.xfer.LocalDestFile
import java.io.Closeable
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.TimeUnit

class SSHClient private constructor(val client: SSHJ) : Closeable {
    companion object {
        fun connect(host: String, port: Int, authConfig: RemoteFileInputPlugin.AuthConfig): SSHClient {
            val client = SSHClient(SSHJ(DefaultConfig()))
            client.connectToHost(host, port, authConfig)
            return client
        }
    }

    private fun connectToHost(host: String, port: Int, authConfig: RemoteFileInputPlugin.AuthConfig) {
        if (authConfig.skipHostKeyVerification) {
            client.addHostKeyVerifier(PromiscuousVerifier())
        }
        if (authConfig.loadKnownHosts) {
            client.loadKnownHosts()
        }
        client.connect(host, port)

        val type = authConfig.type
        val user = authConfig.user.orElse(System.getProperty("user.name"))

        when (type) {
            "password" -> {
                client.authPassword(user, authConfig.password.get())
            }
            "public_key" -> {
                authConfig.keyPath.map {
                    client.authPublickey(user, it)
                }.orElseGet {
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
