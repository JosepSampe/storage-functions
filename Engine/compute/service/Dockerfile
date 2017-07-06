FROM ubuntu:16.04

MAINTAINER Josep Sampe version: 0.0.1

# Install required packages
RUN apt-get update
RUN apt-get install python-pip python2.7 python-dev liberasurecode-dev libpq-dev python-dev libxml2-dev libxslt1-dev libldap2-dev libsasl2-dev libffi-dev -y
RUN apt-get install redis-server git docker.io -y

# Install Swift Proxy
RUN apt-get install software-properties-common -y
RUN add-apt-repository cloud-archive:newton
RUN apt-get update 
RUN apt-get dist-upgrade -y
RUN apt-get install python-openstackclient -y
RUN apt-get install liberasurecode1 liberasurecode-dev -y
RUN apt-get install libjerasure2 libjerasure-dev -y
RUN apt-get install swift swift-proxy python-swiftclient python-keystoneclient python-keystonemiddleware memcached -y

# Clone Zion GIT repository
RUN mkdir /home/zion
RUN git clone https://github.com/JosepSampe/blackeagle /home/zion/zion_framework
RUN mkdir /home/docker_device
RUN chown swift:swift /home/docker_device


# Update PIP and python-redis
RUN pip install --upgrade pip redis

# Install Zion via PIP
RUN pip install -U /home/zion/zion_framework/Engine/swift/middleware/

EXPOSE 8585

# ENTRYPOINT /home/zion/zion_framework/Engine/compute/service/docker_daemon.sh