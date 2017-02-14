Embulk::JavaPlugin.register_input(
  "remote", "org.embulk.input.remote.RemoteFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
