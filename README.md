# Remote file input plugin for Embulk

This plugin load data from Remote hosts by SCP

## Overview

* **Plugin type**: file input
* **Resume supported**: yes
* **Cleanup supported**: yes

## Configuration

- **hosts**: Target hosts (list, default: [])
- **hosts_command**: Command to getting hosts(Windows not supported). If given the option, "hosts" is overwritten. (string, default: null)
- **hosts_separator**: Separator of "hosts_command" result (string, default: " ")
- **path**: Path of remote host (File or Directory) (string, default: "")
- **path_command**: Command to getting hosts (Windows not supported). If given the option "path" is overwritten. (string, default: null)
- **auth**: SSH authentication setting (map, default: {})
    - **user**: SSH username (string, default: executing username)
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
  path: echo /some/path/20150414125923
#  path_command: echo /some/path/`date "+%Y%m%d%H%M%S"`
  auth:
    user: {username}
    type: public_key
    key_path: /usr/home/.ssh/id_rsa
#    type: password
#    password: {password}
```


## Build

```
$ ./gradlew gem
```
