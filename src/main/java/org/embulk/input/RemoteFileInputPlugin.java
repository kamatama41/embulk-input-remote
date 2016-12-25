package org.embulk.input;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.remote.SSHClient;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamTransactionalFileInput;
import org.slf4j.Logger;

import javax.annotation.Nullable;

public class RemoteFileInputPlugin
		implements FileInputPlugin {
	public interface PluginTask
			extends Task {
		@Config("hosts")
		@ConfigDefault("[]")
		List<String> getHosts();

		@Config("hosts_command")
		@ConfigDefault("null")
		Optional<String> getHostsCommand();

		@Config("hosts_separator")
		@ConfigDefault("\" \"")
		String getHostsSeparator();

		@Config("port")
		@ConfigDefault("22")
		int getPort();

		@Config("path")
		@ConfigDefault("\"\"")
		String getPath();

		@Config("path_command")
		@ConfigDefault("null")
		Optional<String> getPathCommand();

		@Config("auth")
		@ConfigDefault("{}")
		Map<String, String> getAuth();

		@Config("ignore_not_found_hosts")
		@ConfigDefault("false")
		boolean getIgnoreNotFoundHosts();

		@Config("last_target")
		@ConfigDefault("null")
		Optional<Target> getLastTarget();

		void setLastTarget(Optional<Target> lastTarget);

		List<Target> getTargets();

		void setTargets(List<Target> targets);

		@ConfigInject
		BufferAllocator getBufferAllocator();
	}

	private final Logger log = Exec.getLogger(getClass());

	@Override
	public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control) {
		PluginTask task = config.loadConfig(PluginTask.class);
		List<Target> targets = listTargets(task);
		log.info("Loading targets {}", targets);
		task.setTargets(targets);

		// number of processors is same with number of targets
		int taskCount = targets.size();
		return resume(task.dump(), taskCount, control);
	}

	private List<Target> listTargets(PluginTask task) {
		final List<String> hosts = listHosts(task);
		final String path = getPath(task);

		final ImmutableList.Builder<Target> builder = ImmutableList.builder();
		Target lastTarget = task.getLastTarget().orNull();
		for (String host : hosts) {
			Target target = new Target(host, path);

			if (lastTarget == null || target.compareTo(lastTarget) > 0) {
				if (task.getIgnoreNotFoundHosts()) {
					try {
						final boolean exists = exists(target, task);
						if (!exists) {
							continue;
						}
					} catch (IOException e) {
						log.warn("failed to check the file exists. " + target.toString(), e);
						continue;
					}
				}
				builder.add(target);
			}
		}
		return builder.build();
	}

	private List<String> listHosts(PluginTask task) {
		final String hostsCommand = task.getHostsCommand().orNull();
		if (hostsCommand != null) {
			final String stdout = execCommand(hostsCommand).trim();
			return Arrays.asList(stdout.split(task.getHostsSeparator()));
		} else {
			return task.getHosts();
		}
	}

	private String getPath(PluginTask task) {
		final String pathCommand = task.getPathCommand().orNull();
		if (pathCommand != null) {
			return execCommand(pathCommand).trim();
		} else {
			return task.getPath();
		}
	}

	private String execCommand(String command) {
		ProcessBuilder pb = new ProcessBuilder("sh", "-c", command);    // TODO: windows
		log.info("Running command {}", command);
		try {
			final Process process = pb.start();
			try (InputStream stream = process.getInputStream();
				 BufferedReader brStdout = new BufferedReader(new InputStreamReader(stream))
			) {
				String line;
				StringBuilder stdout = new StringBuilder();
				while ((line = brStdout.readLine()) != null) {
					stdout.append(line);
				}

				final int code = process.waitFor();
				if (code != 0) {
					throw new IOException(String.format(
							"Command finished with non-zero exit code. Exit code is %d.", code));
				}

				return stdout.toString();
			}
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ConfigDiff resume(TaskSource taskSource,
							 int taskCount,
							 FileInputPlugin.Control control) {
		PluginTask task = taskSource.loadTask(PluginTask.class);

		control.run(taskSource, taskCount);

		List<Target> targets = new ArrayList<>(task.getTargets());
		Collections.sort(targets);

		if (targets.isEmpty()) {
			return Exec.newConfigDiff();
		}

		return Exec.newConfigDiff().set("last_target", targets.get(targets.size() - 1));
	}

	@Override
	public void cleanup(TaskSource taskSource,
						int taskCount,
						List<TaskReport> successTaskReports) {
	}

	@Override
	public TransactionalFileInput open(TaskSource taskSource, int taskIndex) {
		final PluginTask task = taskSource.loadTask(PluginTask.class);
		final Target target = task.getTargets().get(taskIndex);

		return new InputStreamTransactionalFileInput(
				task.getBufferAllocator(),
				new InputStreamTransactionalFileInput.Opener() {
					@Override
					public InputStream open() throws IOException {
						return download(target, task);
					}
				}
		) {
			@Override
			public void abort() {

			}

			@Override
			public TaskReport commit() {
				return Exec.newTaskReport();
			}
		};
	}

	private boolean exists(Target target, PluginTask task) throws IOException {
		try (SSHClient client = SSHClient.getInstance()) {
			client.connect(target.getHost(), task.getPort(), task.getAuth());

			final String checkCmd = "ls " + target.getPath();    // TODO: windows
			final int timeout = 5/* second */;
			final SSHClient.CommandResult commandResult = client.execCommand(checkCmd, timeout);

			if(commandResult.getStatus() != 0) {
				log.warn("Remote file not found. {}", target.toString());
				return false;
			} else {
				return true;
			}
		}
	}

	private InputStream download(Target target, PluginTask task) throws IOException {
		try (SSHClient client = SSHClient.getInstance()) {
			client.connect(target.getHost(), task.getPort(), task.getAuth());
			final ByteArrayOutputStream stream = new ByteArrayOutputStream();
			client.scpDownload(target.getPath(), stream);
			return new ByteArrayInputStream(stream.toByteArray());
		}
	}

	public static class Target implements Comparable<Target> {
		private final String host;
		private final String path;

		@JsonCreator
		public Target(
				@JsonProperty("host") String host,
				@JsonProperty("path") String path) {
			this.host = host;
			this.path = path;
		}

		public String getHost() {
			return host;
		}

		public String getPath() {
			return path;
		}

		@Override
		public int compareTo(@Nullable Target other) {
			if (other == null) {
				throw new NullPointerException();
			}
			return this.toString().compareTo(other.toString());
		}

		@Override
		public String toString() {
			return host + ":" + path;
		}
	}
}
