# Remote file input plugin for [Embulk](https://github.com/embulk/embulk)

This plugin load data from Remote hosts by SCP

## Overview

* **Plugin type**: file input
* **Resume supported**: yes
* **Cleanup supported**: yes

## Configuration

- **hosts**: Target hosts (list, default: [])
- **hosts_command**: Command for getting hosts(Windows not supported). If given the option, "hosts" is overwritten. (string, default: null)
- **hosts_separator**: Separator for "hosts_command" result (string, default: " ")
- **path**: Path of remote host (File or Directory) (string, default: "")
- **path_command**: Command for getting path (Windows not supported). If given the option "path" is overwritten. (string, default: null)
- **ignore_not_found_hosts**: If the option is true, Hosts which meet the following conditions are skipped. (Means they are not included into resume target.) (boolean, default: false)
  - Target file (or directory) isn't found
  - Occurred SSH error
- **auth**: SSH authentication setting (hash, default: {})
    - **user**: SSH username (string, default: execute user)
    - **type**: public_key or password (string, default: public_key)
    - **key_path**: Path of secret key (If you choose type "public_key") (string, default: "~/.ssh/id_rsa or id_dsa")
    - **password**: SSH password (If you choose type "password") (string)

## Example

```yaml
in:
  type: remote
  hosts:
    - host1
    - host2
#  hosts_command: echo 'host1,host2'
#  hosts_separator: ','
  path: /some/path/20150414125923
#  path_command: echo /some/path/`date "+%Y%m%d%H%M%S"`
  ignore_not_found_hosts: true
  auth:
    user: {username}
    type: public_key
    key_path: /usr/home/.ssh/id_rsa
#    type: password
#    password: {password}
```

## Note
When this plugin run on Linux, task is sometimes blocked.  
The cause is java.security.SecureRandom. Please try one of the followings.

### set JVM_OPTION "-Djava.security.egd"

```bash
$ export JAVA_TOOL_OPTIONS="-Djava.security.egd=file:/dev/./urandom"
$ embulk run config.yml
```

### rewrite ${JAVA_HOME}/jre/lib/security/java.security

```
# securerandom.source=file:/dev/random # before
securerandom.source=file:/dev/./urandom # after
```

### see also

http://stackoverflow.com/questions/137212/how-to-solve-performance-problem-with-java-securerandom

## Build

```
$ ./gradlew gem
```
