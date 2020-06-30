echo 127.0.0.1  $HOSTNAME > /etc/hosts
export CLASSPATH=/opt/be/runtime/java/:/opt/be/runtime/java/*:/opt/be/runtime/java/lib/*:/opt/be/runtime/java/assync-http-client/*
export LD_LIBRARY_PATH=/opt/be/runtime/java/lib/

/usr/bin/java com.urv.zion.runtime.daemon.DockerDaemon /mnt/channels/docker_pipe_$1 TRACE 4 $HOSTNAME
