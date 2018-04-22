FROM ubuntu:16.04

RUN apt-get update && apt-get install -y openssh-server sudo whois
RUN mkdir -p /var/run/sshd && chmod 755 /var/run/sshd
# ADD ubuntu user, and set password and public key
ADD id_rsa_test.pub /home/ubuntu/.ssh/authorized_keys
RUN useradd ubuntu \
 && usermod -s /bin/bash -G adm,sudo -p $(echo passw0rd | mkpasswd -s -m sha-512) ubuntu \
 && mkdir -p /home/ubuntu && chown -R ubuntu.ubuntu /home/ubuntu

EXPOSE 22
COPY entrypoint.sh /
ENTRYPOINT [ "/entrypoint.sh" ]
