Embulk::JavaPlugin.register_input(
  "remote", "org.embulk.input.RemoteFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
