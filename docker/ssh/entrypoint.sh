#!/bin/bash

if [ ! -z "${SSH_PORT}" ]; then
  if ! [[ "${SSH_PORT}" =~ ^[0-9]+$ ]] ; then
    echo "SSH_PORT(${SSH_PORT}) must be a number."
    exit 1
  fi
  sed -ri "s/^#?Port 22$/Port ${SSH_PORT}/g" /etc/ssh/sshd_config
fi

/usr/sbin/sshd -D -e "$@"
