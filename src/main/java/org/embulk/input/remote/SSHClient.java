package org.embulk.input.remote;

import com.hierynomus.sshj.signature.SignatureEdDSA;
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.signature.SignatureDSA;
import net.schmizz.sshj.signature.SignatureECDSA;
import net.schmizz.sshj.signature.SignatureRSA;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.InMemoryDestFile;
import net.schmizz.sshj.xfer.LocalDestFile;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SSHClient implements Closeable {

	private final net.schmizz.sshj.SSHClient client;

	public static SSHClient getInstance() {
		return new SSHClient(new net.schmizz.sshj.SSHClient(new DefaultConfig(){
			@Override
			protected void initSignatureFactories() {
				setSignatureFactories(Arrays.asList(
						new SignatureRSA.Factory(),
						new SignatureECDSA.Factory(),
						new SignatureDSA.Factory(),
						new SignatureEdDSA.Factory()
				));
			}
		}));
	}

	private SSHClient(net.schmizz.sshj.SSHClient client) {
		this.client = client;
	}

	public void connect(String host, int port, Map<String, String> authConfig) throws IOException {
		if (Boolean.valueOf(authConfig.get("skip_host_key_verification"))) {
			client.addHostKeyVerifier(new PromiscuousVerifier());
		}
		client.loadKnownHosts();
		client.connect(host, port);

		final String type = authConfig.get("type") != null ? authConfig.get("type") : "public_key";
		final String user = authConfig.get("user") != null ? authConfig.get("user") : System.getProperty("user.name");

		if ("password".equals(type)) {
			client.authPassword(user, authConfig.get("password"));
		} else if ("public_key".equals(type)) {
			final String keyPath = authConfig.get("key_path");
			if (keyPath == null) {
				client.authPublickey(user);
			} else {
				client.authPublickey(user, keyPath);
			}
		} else {
			throw new UnsupportedOperationException("Unsupported auth type : " + type);
		}
	}

	public CommandResult execCommand(String command, int timeoutSecond) throws IOException {
		try (final Session session = client.startSession()) {
			final Session.Command cmd = session.exec(command);
			cmd.join(timeoutSecond, TimeUnit.SECONDS);
			return new CommandResult(cmd.getExitStatus(), cmd.getInputStream());
		}
	}

	public void scpDownload(String path, OutputStream stream) throws IOException {
		client.useCompression();
		client.newSCPFileTransfer().download(path, new InMemoryDestFileImpl(stream));
	}

	private static class InMemoryDestFileImpl extends InMemoryDestFile {

		private OutputStream outputStream;

		public InMemoryDestFileImpl(OutputStream outputStream) {
			this.outputStream = outputStream;
		}

		@Override
		public OutputStream getOutputStream() throws IOException {
			return outputStream;
		}

		@Override
		public LocalDestFile getTargetDirectory(String dirname) throws IOException {
			return this;
		}
	}

	@Override
	public void close() throws IOException {
		if (client != null) {
			client.close();
		}
	}

	public static class CommandResult {
		int status;
		InputStream stdout;

		private CommandResult(int status, InputStream stdout) {
			this.status = status;
			this.stdout = stdout;
		}

		public int getStatus() {
			return status;
		}

		public InputStream getStdout() {
			return stdout;
		}
	}
}
