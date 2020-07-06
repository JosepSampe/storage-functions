import os

host = "10.30.223.232"
user = "zion"
password = "zion"

os.system('sshpass -p %s ssh %s@%s "%s" > /dev/null' % (password, user, host, 'mkdir -p zion/swift/middleware'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/swift/middleware', user, host, 'zion/swift'))

#os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/compute/runtime/java/bin/ZionDockerDaemon-1.0.jar', user, host, '/opt/zion/runtime/java/'))

os.system('sshpass -p %s ssh %s@%s "%s" > /dev/null' % (password, user, host, 'mkdir -p /opt/zion/service'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/compute/service/zion_service.py', user, host, '/opt/zion/service'))

#os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/compute/runtime/java/start_daemon.sh', user, host, '/opt/zion/runtime/java/'))

# os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/compute/runtime/java/lib', user, host, 'josep/zion/runtime/java/'))
# os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/compute/runtime/java/logback.xml', user, host, 'josep/zion/runtime/java/'))

print("--> FILES UPLOADED")

os.system('sshpass -p %s ssh %s@%s "%s" > /dev/null' % (password, user, host, 'sudo swift-init main restart'))

print("--> FINISH")
