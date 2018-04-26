[![CircleCI](https://circleci.com/gh/kamatama41/embulk-input-remote.svg?style=svg)](https://circleci.com/gh/kamatama41/embulk-input-remote)

# Remote file input plugin for [Embulk](https://github.com/embulk/embulk)

This plugin load data from Remote hosts by SCP

## Overview

* **Plugin type**: file input
* **Resume supported**: yes
* **Cleanup supported**: yes

## Configuration

- **hosts**: Target hosts, its format should be `hostname` or `hostname:port` (its `port` overrides `default_port`) (list, default: [])
- **hosts_command**: Command to get `hosts` (Windows not supported). If given the option, `hosts` is overwritten. (string, default: null)
- **hosts_separator**: Separator for `hosts_command` result (string, default: " ")
- **default_port**: Port number for SSH (integer, default: 22)
- **path**: File or directory path of remote host (string, default: "")
- **path_command**: Command to get `path` (Windows not supported). If given the option, `path` is overwritten. (string, default: null)
- **ignore_not_found_hosts**: If true, hosts that meet the following conditions are skipped, which means they are not included into the the target of resuming.) (boolean, default: false)
  - Target file (or directory) is not found
  - An error that is related to SSH connectivity occurred 
- **auth**: SSH authentication setting (hash, default: {})
    - **user**: SSH username (string, default: current user)
    - **type**: `public_key` or `password` (string, default: public_key)
    - **key_path**: Path of your secret key (required when you choose `public_key` type) (string, default: `~/.ssh/id_rsa` or `id_dsa`")
    - **password**: Password of the `user` (required when you choose `password` type) (string)
    - **skip_host_key_verification**: If true, verification of host key will be skipped (boolean, default: false)

## Example

```yaml
in:
  type: remote
  hosts:
    - host1
    - host2:10022
  # hosts_command: echo 'host1,host2:10022'
  # hosts_separator: ','
  path: /some/path/20150414125923
  # path_command: echo /some/path/`date "+%Y%m%d%H%M%S"`
  ignore_not_found_hosts: true
  auth:
    user: a_user
    type: public_key
    key_path: /usr/home/.ssh/id_rsa
    # type: password
    # password: {password}
```

## Note
When running Embulk with this plugin on Linux, it might be blocked due to SecureRandom. Please try one of followings to resolve it.
(Note: it makes SecureRandom more insecure than default)

### set JVM_OPTION "-Djava.security.egd"

```bash
$ export JAVA_TOOL_OPTIONS="-Djava.security.egd=file:/dev/./urandom"
$ embulk run config.yml
```

### Update "securerandom.source" in ${JAVA_HOME}/jre/lib/security/java.security
```
# securerandom.source=file:/dev/random  # before
securerandom.source=file:/dev/./urandom # after
```

### see also

http://stackoverflow.com/questions/137212/how-to-solve-performance-problem-with-java-securerandom

## Development on local machine
Install Docker and Docker compose so that you can start SSH-able containers and run tests.

```sh
$ docker-compose up -d
$ docker-compose ps
          Name                 Command       State           Ports        
--------------------------------------------------------------------------
embulk-input-remote_host1   /entrypoint.sh   Up      0.0.0.0:10022->22/tcp
embulk-input-remote_host2   /entrypoint.sh   Up      0.0.0.0:10023->22/tcp

$ docker cp $(docker inspect --format="{{.Id}}" embulk-input-remote_host1):/home/ubuntu/.ssh/id_rsa_test .
$ chmod 400 id_rsa_test
$ ./gradlew test
```

## Build

```
$ ./gradlew gem
```
