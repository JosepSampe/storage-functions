echo 127.0.0.1  $HOSTNAME > /etc/hosts
cp /home/swift/logback.xml /opt/storlets/logback.xml
export CLASSPATH=/home/swift/*:/home/swift/assync-http-client/*
export LD_LIBRARY_PATH=/home/swift/
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
