#!/bin/bash

OPENSTACK_RELEASE=ussury

#########  PASSWORDS  #########
MYSQL_PASSWD=root
RABBITMQ_PASSWD=openstack
KEYSTONE_ADMIN_PASSWD=keystone
ZION_TENANT_PASSWD=zion
###############################

LOG=/tmp/zion_aio_installation.log
IP_ADDRESS=$(hostname -I | cut -d ' ' -f1)

###### Upgrade System ######
upgrade_system(){
    echo controller > /etc/hostname
    echo -e "127.0.0.1 \t localhost" > /etc/hosts
    echo -e "$IP_ADDRESS \t controller" >> /etc/hosts
    #ln -sf /run/systemd/resolve/resolv.conf /etc/resolv.conf

    add-apt-repository universe
    apt install software-properties-common -y
    add-apt-repository cloud-archive:$OPENSTACK_RELEASE -y
    apt update

    DEBIAN_FRONTEND=noninteractive apt -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" dist-upgrade
    unset DEBIAN_FRONTEND
    apt install python3-pip python3-openstackclient -y
}


###### Install Memcached ######
install_memcache_server(){
    apt install memcached python-memcache -y
    sed -i '/-l 127.0.0.1/c\-l controller' /etc/memcached.conf
    service memcached restart
}


###### Install RabbitMQ ######
install_rabbitmq_server(){
    apt install rabbitmq-server -y
    rabbitmqctl add_user openstack $RABBITMQ_PASSWD
    rabbitmqctl set_user_tags openstack administrator
    rabbitmqctl set_permissions openstack ".*" ".*" ".*"
    rabbitmq-plugins enable rabbitmq_management
}


###### Install MySQL ######
install_mysql_server(){

    export DEBIAN_FRONTEND=noninteractive
    debconf-set-selections <<< "mariadb-server-10.0 mysql-server/root_password password $MYSQL_PASSWD"
    debconf-set-selections <<< "mariadb-server-10.0 mysql-server/root_password_again password $MYSQL_PASSWD"
    apt install mariadb-server python3-pymysql -y
    unset DEBIAN_FRONTEND
    
    mysql -uroot -p$MYSQL_PASSWD -e "DELETE FROM mysql.user WHERE User='root' AND Host NOT IN ('localhost', '127.0.0.1', '::1')"
    mysql -uroot -p$MYSQL_PASSWD -e "DELETE FROM mysql.user WHERE User=''"
    mysql -uroot -p$MYSQL_PASSWD -e "FLUSH PRIVILEGES"
    
    cat <<-EOF >> /etc/mysql/mariadb.conf.d/99-openstack.cnf
    [mysqld]
    bind-address = 0.0.0.0
    default-storage-engine = innodb
    innodb_file_per_table = on
    max_connections = 4096
    collation-server = utf8_general_ci
    character-set-server = utf8
    EOF
    
    service mysql restart
}


###### Install Keystone ######
install_openstack_keystone(){
    mysql -uroot -p$MYSQL_PASSWD -e "CREATE DATABASE keystone"
    mysql -uroot -p$MYSQL_PASSWD -e "GRANT ALL PRIVILEGES ON keystone.* TO 'keystone'@'localhost' IDENTIFIED BY 'keystone'"
    mysql -uroot -p$MYSQL_PASSWD -e "GRANT ALL PRIVILEGES ON keystone.* TO 'keystone'@'%' IDENTIFIED BY 'keystone'"
    
    apt install keystone apache2 libapache2-mod-wsgi-py3 -y
    
    sed -i '/connection =/c\connection = mysql+pymysql://keystone:keystone@controller/keystone' /etc/keystone/keystone.conf
    sed -i '/#provider = fernet/c\provider = fernet' /etc/keystone/keystone.conf
    
    
    su -s /bin/sh -c "keystone-manage db_sync" keystone
    keystone-manage fernet_setup --keystone-user keystone --keystone-group keystone
    keystone-manage credential_setup --keystone-user keystone --keystone-group keystone
    keystone-manage bootstrap --bootstrap-password $KEYSTONE_ADMIN_PASSWD --bootstrap-admin-url http://controller:5000/v3/ --bootstrap-internal-url http://controller:5000/v3/ --bootstrap-public-url http://$IP_ADDRESS:5000/v3/ --bootstrap-region-id RegionOne
    
    echo "ServerName controller" >> /etc/apache2/apache2.conf
    rm -f /var/lib/keystone/keystone.db
    service apache2 restart
        
    cat <<-EOF >> admin-openrc
    export OS_USERNAME=admin
    export OS_PASSWORD=$KEYSTONE_ADMIN_PASSWD
    export OS_PROJECT_NAME=admin
    export OS_USER_DOMAIN_NAME=Default
    export OS_PROJECT_DOMAIN_NAME=Default
    export OS_AUTH_URL=http://controller:5000/v3
    export OS_IDENTITY_API_VERSION=3
    EOF
    
    source admin-openrc
    openstack role create user
    openstack role create ResellerAdmin
    
    openstack project create --domain default --description "Service Project" service
    openstack project create --domain default --description "Zion Test Project" zion
    
    openstack user create --domain default --password $ZION_TENANT_PASSWD zion
    
    openstack role add --project zion --user zion admin
    openstack role add --project zion --user zion ResellerAdmin
    openstack role add --domain default --user zion ResellerAdmin
    
    cat <<-EOF >> zion-openrc
    export OS_USERNAME=zion
    export OS_PASSWORD=$ZION_TENANT_PASSWD
    export OS_PROJECT_NAME=zion
    export OS_USER_DOMAIN_NAME=Default
    export OS_PROJECT_DOMAIN_NAME=Default
    export OS_AUTH_URL=http://controller:5000/v3
    export OS_IDENTITY_API_VERSION=3
    EOF
}

###### OpenStak Horizon ######
install_openstack_horizon() {
    apt install openstack-dashboard -y
    cat <<-EOF >> /etc/openstack-dashboard/local_settings.py
    
    OPENSTACK_API_VERSIONS = {
        "identity": 3,
    }
    LANGUAGES = (
        ('en', 'English'),
    )
    EOF
    
    sed -i '/OPENSTACK_HOST = "127.0.0.1"/c\OPENSTACK_HOST = "controller"' /etc/openstack-dashboard/local_settings.py
    sed -i '/OPENSTACK_KEYSTONE_URL = "http:\/\/%s:5000\/v2.0" % OPENSTACK_HOST/c\OPENSTACK_KEYSTONE_URL = "http:\/\/%s:5000\/v3" % OPENSTACK_HOST' /etc/openstack-dashboard/local_settings.py
    sed -i '/OPENSTACK_KEYSTONE_DEFAULT_ROLE = "_member_"/c\OPENSTACK_KEYSTONE_DEFAULT_ROLE = "user"' /etc/openstack-dashboard/local_settings.py
    #sed -i '/#LOGIN_REDIRECT_URL = WEBROOT/c\LOGIN_REDIRECT_URL = WEBROOT + "horizon/project/containers"' /etc/openstack-dashboard/local_settings.py
}

###### Install Swift ######
install_openstack_swift(){
    source admin-openrc
    openstack user create --domain default --password swift swift
    openstack role add --project service --user swift admin
    openstack service create --name swift --description "OpenStack Object Storage" object-store
    
    openstack endpoint create --region RegionOne object-store public http://$IP_ADDRESS:8080/v1/AUTH_%\(tenant_id\)s
    openstack endpoint create --region RegionOne object-store internal http://controller:8080/v1/AUTH_%\(tenant_id\)s
    openstack endpoint create --region RegionOne object-store admin http://controller:8080/v1
    
    apt install swift swift-proxy swift-account swift-container swift-object -y
    apt install python-swiftclient python-keystoneclient python-keystonemiddleware memcached -y
    apt install xfsprogs rsync -y
    
    mkdir /etc/swift
    chown $(whoami):$(whoami) /etc/swift
    curl -o /etc/swift/proxy-server.conf https://git.openstack.org/cgit/openstack/swift/plain/etc/proxy-server.conf-sample?h=stable/$OPENSTACK_RELEASE
    curl -o /etc/swift/account-server.conf https://git.openstack.org/cgit/openstack/swift/plain/etc/account-server.conf-sample?h=stable/$OPENSTACK_RELEASE
    curl -o /etc/swift/container-server.conf https://git.openstack.org/cgit/openstack/swift/plain/etc/container-server.conf-sample?h=stable/$OPENSTACK_RELEASE
    curl -o /etc/swift/object-server.conf https://git.openstack.org/cgit/openstack/swift/plain/etc/object-server.conf-sample?h=stable/$OPENSTACK_RELEASE
    curl -o /etc/swift/swift.conf https://git.openstack.org/cgit/openstack/swift/plain/etc/swift.conf-sample?h=stable/$OPENSTACK_RELEASE
    
    mkdir -p /srv/node/sda1
    mkdir -p /var/cache/swift
    chown -R root:swift /var/cache/swift
    chmod -R 775 /var/cache/swift
    chown -R swift:swift /srv/node
            
    cd /etc/swift
    swift-ring-builder account.builder create 10 1 1
    swift-ring-builder account.builder add --region 1 --zone 1 --ip controller --port 6202 --device sda1 --weight 100
    swift-ring-builder account.builder
    swift-ring-builder account.builder rebalance
    
    swift-ring-builder container.builder create 10 1 1
    swift-ring-builder container.builder add --region 1 --zone 1 --ip controller --port 6201 --device sda1 --weight 100
    swift-ring-builder container.builder
    swift-ring-builder container.builder rebalance
    
    swift-ring-builder object.builder create 10 1 1
    swift-ring-builder object.builder add --region 1 --zone 1 --ip controller --port 6200 --device sda1 --weight 100
    swift-ring-builder object.builder
    swift-ring-builder object.builder rebalance
    cd ~
    
    sed -i '/^pipeline =/ d' /etc/swift/proxy-server.conf
    sed -i '/# account_autocreate = false/c\account_autocreate = True' /etc/swift/proxy-server.conf
    sed -i '/# \[filter:authtoken]/c\[filter:authtoken]' /etc/swift/proxy-server.conf
    sed -i '/# paste.filter_factory = keystonemiddleware.auth_token:filter_factory/c\paste.filter_factory = keystonemiddleware.auth_token:filter_factory' /etc/swift/proxy-server.conf
    sed -i '/# auth_url = http:\/\/keystonehost:35357/c\auth_url = http://controller:5000' /etc/swift/proxy-server.conf
    sed -i '/# auth_plugin = password/c\auth_type = password' /etc/swift/proxy-server.conf
    sed -i '/# project_domain_id = default/c\project_domain_name = default' /etc/swift/proxy-server.conf
    sed -i '/# user_domain_id = default/c\user_domain_name = default' /etc/swift/proxy-server.conf
    sed -i '/# project_name = service/c\project_name = service' /etc/swift/proxy-server.conf
    sed -i '/# username = swift/c\username = swift' /etc/swift/proxy-server.conf
    sed -i '/# password = password/c\password = swift \nservice_token_roles_required = True' /etc/swift/proxy-server.conf
    sed -i '/# delay_auth_decision = False/c\delay_auth_decision = True \nmemcached_servers = controller:11211' /etc/swift/proxy-server.conf
    sed -i '/# \[filter:keystoneauth]/c\[filter:keystoneauth]' /etc/swift/proxy-server.conf
    sed -i '/# use = egg:swift#keystoneauth/c\use = egg:swift#keystoneauth' /etc/swift/proxy-server.conf
    sed -i '/# operator_roles = admin, swiftoperator/c\operator_roles = admin, swiftoperator' /etc/swift/proxy-server.conf
    sed -i '/# memcache_servers = 127.0.0.1:11211/c\memcache_servers = controller:11211' /etc/swift/proxy-server.conf
    
    sed -i '/# mount_check = true/c\mount_check = false' /etc/swift/account-server.conf
    sed -i '/# mount_check = true/c\mount_check = false' /etc/swift/container-server.conf
    sed -i '/# mount_check = true/c\mount_check = false' /etc/swift/object-server.conf
    
    sed -i '/# workers = auto/c\workers = 1' /etc/swift/proxy-server.conf
    sed -i '/# workers = auto/c\workers = 1' /etc/swift/object-server.conf
    
    sed -i '/name = Policy-0/c\name = AiO' /etc/swift/swift.conf
    
    systemctl stop swift-account-auditor swift-account-reaper swift-account-replicator swift-container-auditor swift-container-replicator swift-container-sync swift-container-updater swift-object-auditor swift-object-reconstructor swift-object-replicator swift-object-updater
    systemctl disable swift-account-auditor swift-account-reaper swift-account-replicator swift-container-auditor swift-container-replicator swift-container-sync swift-container-updater swift-object-auditor swift-object-reconstructor swift-object-replicator swift-object-updater
    swift-init all stop
    #usermod -u 1010 swift
    #groupmod -g 1010 swift
    
}

##### Install Storlets #####
install_storlets(){
    add-apt-repository -y ppa:webupd8team/java
    apt update
    apt install gcc openjdk-8-jdk openjdk-8-jre -y
    #echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
    #echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections
    #apt install oracle-java8-installer -y
    
    # Install Docker
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    apt-key fingerprint 0EBFCD88
    sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
    apt update
    apt install aufs-tools linux-image-generic apt-transport-https docker-ce ansible ant -y
    
    cat <<-EOF >> /etc/docker/daemon.json
    {
    "data-root": "/home/docker_device/docker"
    }
    EOF
    
    mkdir /home/docker_device
    chmod 777 /home/docker_device
    service docker stop
    service docker start
    
    # Install Storlets
    git clone https://github.com/openstack/storlets -b stable/pike
    pip install storlets/
    cd storlets
    ./install_libs.sh
    
    # Install host-side scripts
    mkdir /home/docker_device/scripts
    chown swift:swift /home/docker_device/scripts
    cp scripts/restart_docker_container /home/docker_device/scripts/
    cp scripts/send_halt_cmd_to_daemon_factory.py /home/docker_device/scripts/
    chown root:root /home/docker_device/scripts/*
    chmod 04755 /home/docker_device/scripts/*
    
    # Create Storlet docker runtime
    sed -i "/ansible-playbook \-s \-i deploy\/prepare_host prepare_storlets_install.yml/c\ansible-playbook \-s \-i deploy\/prepare_host prepare_storlets_install.yml --connection=local" install/storlets/prepare_storlets_install.sh
    install/storlets/prepare_storlets_install.sh dev host
    
    cd install/storlets/
    SWIFT_UID=$(id -u swift)
    SWIFT_GID=$(id -g swift)
    sed -i '/- role: docker_client/c\  #- role: docker_client' docker_cluster.yml
    sed -i '/"swift_user_id": "1003"/c\\t"swift_user_id": "'$SWIFT_UID'",' deploy/cluster_config.json
    sed -i '/"swift_group_id": "1003"/c\\t"swift_group_id": "'$SWIFT_GID'",' deploy/cluster_config.json
    ansible-playbook -s -i storlets_dynamic_inventory.py docker_cluster.yml --connection=local
    docker rmi ubuntu_16.04_jre8 ubuntu:16.04 ubuntu_16.04 -f
    cd ~

    cat <<-EOF >> /etc/swift/proxy-server.conf  
    
    [filter:storlet_handler]
    use = egg:storlets#storlet_handler
    storlet_container = storlet
    storlet_logcontainer = storletlog
    storlet_execute_on_proxy_only = false
    storlet_gateway_module = docker
    storlet_gateway_conf = /etc/swift/storlet_docker_gateway.conf
    execution_server = proxy
    EOF
    
    cat <<-EOF >> /etc/swift/object-server.conf
    
    [filter:storlet_handler]
    use = egg:storlets#storlet_handler
    storlet_container = storlet
    storlet_gateway_module = docker
    storlet_gateway_conf = /etc/swift/storlet_docker_gateway.conf
    storlet_execute_on_proxy_only = false
    execution_server = object
    storlet_daemon_thread_pool_size = 4
    EOF
    
    cat <<-EOF >> /etc/swift/storlet_docker_gateway.conf
    [DEFAULT]
    lxc_root = /home/docker_device/scopes
    cache_dir = /home/docker_device/cache/scopes
    log_dir = /home/docker_device/logs/scopes
    script_dir = /home/docker_device/scripts
    storlets_dir = /home/docker_device/storlets/scopes
    pipes_dir = /home/docker_device/pipes/scopes
    docker_repo = 
    restart_linux_container_timeout = 8
    storlet_timeout = 40
    EOF
    
    cp /etc/swift/proxy-server.conf /etc/swift/storlet-proxy-server.conf
    sed -i '/^pipeline =/ d' /etc/swift/storlet-proxy-server.conf
    sed -i '/\[pipeline:main\]/a pipeline = proxy-logging cache slo proxy-logging proxy-server' /etc/swift/storlet-proxy-server.conf
    rm -r storlets  
}


#### Install micro-controllers #####
install_microcontrollers(){
    
    apt install redis-server -y
    sed -i '/bind 127.0.0.1/c\bind 0.0.0.0' /etc/redis/redis.conf
    service redis restart
    pip install -U redis
    
    git clone https://github.com/JosepSampe/micro-controllers
    pip install -U micro-controllers/Engine/swift
    
    cat <<-EOF >> /etc/swift/proxy-server.conf
    
    [filter:vertigo_handler]
    use = egg:swift-vertigo#vertigo_handler
    execution_server = proxy
    redis_host = $IP_ADDRESS
    EOF
    
    cat <<-EOF >> /etc/swift/object-server.conf
    
    [filter:vertigo_handler]
    use = egg:swift-vertigo#vertigo_handler
    execution_server = object
    EOF
    

    sed -i '/^pipeline =/ d' /etc/swift/proxy-server.conf
    sed -i '/\[pipeline:main\]/a pipeline = catch_errors gatekeeper healthcheck proxy-logging cache container_sync bulk tempurl ratelimit authtoken keystoneauth copy container-quotas account-quotas vertigo_handler storlet_handler slo dlo versioned_writes symlink proxy-logging proxy-server' /etc/swift/proxy-server.conf
    
    sed -i '/^pipeline =/ d' /etc/swift/object-server.conf
    sed -i '/\[pipeline:main\]/a pipeline = healthcheck recon vertigo_handler storlet_handler object-server' /etc/swift/object-server.conf
    

    mkdir -p /opt/vertigo
    cp micro-controllers/Engine/runtime/bin/DockerDaemon.jar /opt/vertigo
    cp micro-controllers/Engine/runtime/lib/spymemcached-2.12.1.jar /opt/vertigo
    cp micro-controllers/Engine/runtime/lib/jedis-2.9.0.jar /opt/vertigo
    cp micro-controllers/Engine/runtime/utils/start_daemon.sh /opt/vertigo
    cp micro-controllers/Engine/runtime/utils/logback.xml /opt/vertigo
    cp micro-controllers/Engine/runtime/utils/docker_daemon.config /opt/vertigo
    cp micro-controllers/Engine/bus/DockerJavaFacade/bin/BusDockerJavaFacade.jar /opt/vertigo
    cp micro-controllers/Engine/bus/DockerJavaFacade/bin/libjbus.so /opt/vertigo
    cp micro-controllers/Engine/bus/TransportLayer/bin/bus.so /opt/vertigo
    
    sed -i "/swift_ip=/c\swift_ip=$IP_ADDRESS" /opt/vertigo/docker_daemon.config
    sed -i "/redis_ip=/c\redis_ip=$IP_ADDRESS" /opt/vertigo/docker_daemon.config
    
    swift-init main restart
}


##### Initialize tenant #####
initialize_tenant(){
    # Initialize Vertigo test tenant
    . vertigo-openrc
    PROJECT_ID=$(openstack token issue | grep -w project_id | awk '{print $4}')
    docker tag ubuntu_16.04_jre8_storlets ${PROJECT_ID:0:13}

    swift post storlet
    swift post microcontroller
    swift post dependency

    swift post -H "X-account-meta-storlet-enabled:True"
    
    mkdir -p /home/docker_device/vertigo/scopes/${PROJECT_ID:0:13}/
    cp /opt/vertigo/* /home/docker_device/vertigo/scopes/${PROJECT_ID:0:13}/
    chown -R swift:swift /home/docker_device/vertigo/scopes/
    
    gpasswd -a "$(whoami)" docker
    usermod -aG docker swift
    
    cat <<-EOF >> vertigo-openrc
    export STORAGE_URL=http://$IP_ADDRESS:8080/v1/AUTH_$PROJECT_ID
    export TOKEN=\$(openstack token issue | grep -w id | awk '{print \$4}')
    EOF

    rm -r micro-controllers 
}


##### Restart Main Services #####
restart_services(){
    swift-init main restart
    service apache2 restart
}


install_zion(){
    printf "\nStarting Installation. The script takes long to complete, be patient!\n"
    printf "See the full log at $LOG\n\n"
    
    printf "Upgrading Server System\t\t ... \t2%%"
    upgrade_system >> $LOG 2>&1; printf "\tDone!\n"
    
    printf "Installing Memcache Server\t ... \t4%%"
    install_memcache_server >> $LOG 2>&1; printf "\tDone!\n"
    printf "Installing RabbitMQ Server\t ... \t6%%"
    install_rabbitmq_server >> $LOG 2>&1; printf "\tDone!\n"
    printf "Installing MySQL Server\t\t ... \t8%%"
    install_mysql_server >> $LOG 2>&1; printf "\tDone!\n"
    
    printf "Installing OpenStack Keystone\t ... \t10%%"
    install_openstack_keystone >> $LOG 2>&1; printf "\tDone!\n"
        
    printf "Installing OpenStack Horizon\t ... \t30%%"
    install_openstack_horizon >> $LOG 2>&1; printf "\tDone!\n"
    #printf "Installing OpenStack Swift\t ... \t50%%"
    #install_openstack_swift >> $LOG 2>&1; printf "\tDone!\n"
    
    #printf "Installing Storlets\t\t ... \t70%%"
    #install_storlets >> $LOG 2>&1; printf "\tDone!\n"
    #printf "Installing Micro-controllers\t ... \t85%%"
    #install_microcontrollers >> $LOG 2>&1; printf "\tDone!\n"
    #printf "Initializing Test Tenant\t ... \t95%%"
    #initialize_tenant >> $LOG 2>&1; printf "\tDone!\n"
    
    #restart_services >> $LOG 2>&1;
    #printf "Micro-controllers installation\t ... \t100%%\tCompleted!\n\n"
    #printf "Access the Dashboard with the following URL: http://$IP_ADDRESS/horizon\n"
    #printf "Login with user: vertigo | password: $VERTIGO_TENANT_PASSWD\n\n"
}


update_zion(){
    printf "Updating Zion Installation.\n"
    printf "See the full log at $LOG\n\n"

    printf "Installing Swift middleware\t ... \t20%%"
    git clone https://github.com/JosepSampe/micro-controllers  >> $LOG 2>&1;
    pip install -U micro-controllers/Engine/swift  >> $LOG 2>&1;
    printf "\tDone!\n"
    
    printf "Installing Libraries\t\t ... \t85%%"
    mkdir -p /opt/vertigo
    cp micro-controllers/Engine/runtime/bin/DockerDaemon.jar /opt/vertigo
    cp micro-controllers/Engine/runtime/lib/spymemcached-2.12.1.jar /opt/vertigo
    cp micro-controllers/Engine/runtime/lib/jedis-2.9.0.jar /opt/vertigo
    cp micro-controllers/Engine/runtime/utils/start_daemon.sh /opt/vertigo
    cp micro-controllers/Engine/runtime/utils/logback.xml /opt/vertigo
    cp micro-controllers/Engine/runtime/utils/docker_daemon.config /opt/vertigo
    cp micro-controllers/Engine/bus/DockerJavaFacade/bin/BusDockerJavaFacade.jar /opt/vertigo
    cp micro-controllers/Engine/bus/DockerJavaFacade/bin/libjbus.so /opt/vertigo
    cp micro-controllers/Engine/bus/TransportLayer/bin/bus.so /opt/vertigo
    rm -rf micro-controllers

    sed -i '/swift_ip=/c\swift_ip='$IP_ADDRESS /opt/vertigo/docker_daemon.config
    sed -i '/redis_ip=/c\redis_ip='$IP_ADDRESS /opt/vertigo/docker_daemon.config

    . vertigo-openrc
    PROJECT_ID=$(openstack token issue | grep -w project_id | awk '{print $4}')
    mkdir -p /home/docker_device/vertigo/scopes/${PROJECT_ID:0:13}/
    cp /opt/vertigo/* /home/docker_device/vertigo/scopes/${PROJECT_ID:0:13}/
    chown -R swift:swift /home/docker_device/vertigo/scopes/
    printf "\tDone!\n"

    printf "Restarting services\t\t ... \t98%%"
    restart_services >> $LOG 2>&1; printf "\tDone!\n"
    printf "Updating Micro-controllers AiO\t ... \t100%%\tCompleted!\n\n"
}


usage(){
    echo "Usage: sudo ./aio_installation.sh install|update"
    exit 1
}


COMMAND="$1"
main(){
    if [[ `lsb_release -rs` == "20.04" ]]
    then
        case $COMMAND in
          "install" )
            install_zion
            ;;
          
          "update" )
            update_zion
            ;;

          * )
            install_zion
        esac
    else
        echo "Wrong ubuntu version, you must use Ubuntu 20.04"
    fi
    
}

main