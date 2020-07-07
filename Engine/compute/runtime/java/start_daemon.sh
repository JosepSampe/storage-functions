echo 127.0.0.1  $HOSTNAME > /etc/hosts
export CLASSPATH=/opt/zion/runtime/java/:/opt/zion/runtime/java/*:/opt/zion/runtime/java/lib/*:/opt/zion/runtime/java/lib/assync-http-client/*
export LD_LIBRARY_PATH=/opt/zion/runtime/java/lib

java com.urv.zion.runtime.daemon.ZionDockerDaemon /opt/zion/channel/pipe TRACE 4 $HOSTNAME
