echo 127.0.0.1  $HOSTNAME > /etc/hosts
cp /home/swift/logback.xml /opt/storlets/logback.xml
export CLASSPATH=/opt/storlets/:/opt/storlets/logback-classic-1.1.2.jar:/opt/storlets/logback-core-1.1.2.jar:/opt/storlets/slf4j-api-1.7.7.jar:/opt/storlets/json_simple-1.1.jar:/home/swift/jedis-2.9.0.jar:/home/swift/spymemcached-2.12.1.jar:/home/swift/SBusJavaFacade.jar:/home/swift/blackeagle-runtime-1.0.jar
export LD_LIBRARY_PATH=/opt/storlets
wk=$1
if [ -z "$1" ]
  then
    wk=1 
fi
workers=$(($wk-1))
if (($workers > 0))
then
for i in $(eval echo "{1..$workers}")
do
/usr/bin/java com.urv.blackeagle.runtime.daemon.DockerDaemon /mnt/channels/function_pipe_$i TRACE 5 $HOSTNAME &
done
fi
/usr/bin/java com.urv.blackeagle.runtime.daemon.DockerDaemon /mnt/channels/function_pipe_0 TRACE 5 $HOSTNAME