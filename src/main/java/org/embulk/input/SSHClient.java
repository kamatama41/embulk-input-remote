package org.embulk.input;

import net.schmizz.sshj.xfer.InMemoryDestFile;
import net.schmizz.sshj.xfer.LocalDestFile;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class SSHClient implements Closeable {
	
	private final net.schmizz.sshj.SSHClient client;

	public SSHClient() {
		this(new net.schmizz.sshj.SSHClient());
	}

	/* package for test */
	SSHClient(net.schmizz.sshj.SSHClient client) {
		this.client = client;
	}
	
	public void connect(String host, Map<String, String> authConfig) throws IOException {
		client.loadKnownHosts();
		
		client.connect(host);

		final String type = authConfig.get("type") != null ? authConfig.get("type") : "public_key";
		final String user = authConfig.get("user") != null ? authConfig.get("user") : System.getProperty("user.name");

		if ("password".equals(type)) {
			client.authPassword(user, authConfig.get("password"));
		} else if ("public_key".equals(type)) {
			final String key_path = authConfig.get("key_path");
			if (key_path == null) {
				client.authPublickey(user);
			} else {
				client.authPublickey(user, key_path);
			}
		} else {
			throw new UnsupportedOperationException("Unsupported auth type : " + type);
		}
	}

	public void scpDownload(String path, OutputStream stream) throws IOException {
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
		if(client != null) {
			client.close();
		}
	}
}
