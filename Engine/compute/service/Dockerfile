FROM ubuntu:16.04

MAINTAINER Josep Sampe version: 0.0.1

# Install required packages
RUN apt-get update && \
apt-get install python-pip python2.7 python-dev -y && \
apt-get install liberasurecode-dev libpq-dev python-dev libxml2-dev -y && \
apt-get install libxslt1-dev libldap2-dev libsasl2-dev libffi-dev -y && \
apt-get install redis-server git docker.io -y && \

# Install Swift Proxy
apt-get install software-properties-common -y && \
add-apt-repository cloud-archive:newton && \
apt-get update && \
apt-get dist-upgrade -y && \
apt-get install python-openstackclient -y && \
apt-get install liberasurecode1 liberasurecode-dev -y && \
apt-get install libjerasure2 libjerasure-dev -y && \
apt-get install swift swift-proxy python-swiftclient -y && \
apt-get install python-keystoneclient python-keystonemiddleware memcached -y && \
apt-get clean

# Clone Zion GIT repository
RUN mkdir /home/zion
RUN git clone https://github.com/JosepSampe/blackeagle /home/zion/zion_framework
RUN mkdir /var/log/be

# Update PIP and python-redis
RUN pip install --upgrade pip redis docker psutil

# Install Zion via PIP
RUN pip install --upgrade /home/zion/zion_framework/Engine/swift/middleware/

EXPOSE 8585

RUN chmod 777 /home/zion/zion_framework/Engine/compute/service/init_container.sh
ENTRYPOINT /home/zion/zion_framework/Engine/compute/service/init_container.sh
