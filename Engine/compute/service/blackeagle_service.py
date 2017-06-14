from blackeagle.common.utils import get_object_metadata
#from blackeagle.gateways.docker.bus import Bus
#from blackeagle.gateways.docker.datagram import Datagram
from bus import Bus
from datagram import Datagram
from daemonize import Daemonize
from gevent import Greenlet
from docker.errors import NotFound
from gevent.timeout import Timeout
from subprocess import Popen
import shutil
import gevent
import logging
import operator
import random
import docker
import redis
import psutil
import json
import time
import sys
import os


REDIS_CONN_POOL = redis.ConnectionPool(host='localhost', port=6379, db=10)
# CPU
TOTAL_CPUS = psutil.cpu_count()
HIGH_CPU_THRESHOLD = 20
LOW_CPU_THRESHOLD = 0.10
WORKERS = 2
WORKER_TIMEOUT = 20  # seconds
# DIRS
ZION_DIR = '/opt/zion'
MAIN_DIR = '/home/docker_device/blackeagle/'
WORKERS_DIR = MAIN_DIR+'workers/'
FUNCTIONS_DIR = MAIN_DIR+'functions/'
POOL_DIR = MAIN_DIR+'docker_pool/'
DOCKER_IMAGE = '192.168.2.1:5001/blackeagle'
# Headers
TIMEOUT_HEADER = "X-Object-Meta-Function-Timeout"
MEMORY_HEADER = "X-Object-Meta-Function-Memory"
MAIN_HEADER = "X-Object-Meta-Function-Main"
# create logger with 'be_service'
logger = logging.getLogger('be_service')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('/var/log/be/be-service.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

swift_uid = shutil._get_uid('swift')
swift_gid = shutil._get_gid('swift')


class Container(Greenlet):

    def __init__(self, cid):
        Greenlet.__init__(self)
        self.id = str(cid)
        self.name = "zion_"+str(cid)
        self.stopped = False
        self.container = None
        self.docker_dir = POOL_DIR+self.name
        self.channel_dir = self.docker_dir+'/channel'
        self.r = redis.Redis(connection_pool=REDIS_CONN_POOL)
        self.c = docker.from_env()

        self.function = None
        self.monitoing_info = None

        self._create_directory_structure()
        self._start_container()

    def _create_directory_structure(self):
        logger.info("Creating container structure: "+self.docker_dir)
        if not os.path.exists(self.docker_dir):
            p = Popen(['cp', '-p', '-R', ZION_DIR, self.docker_dir])
            p.wait()
            os.makedirs(self.channel_dir)
            os.chown(self.channel_dir, swift_uid, swift_gid)

    def _start_container(self):
        logger.info("Starting container: "+self.name)
        command = 'debug "/opt/zion/runtime/java/start_daemon.sh"'
        vols = {'/dev/log': {'bind': '/dev/log', 'mode': 'rw'},
                self.docker_dir: {'bind': '/opt/zion', 'mode': 'rw'}}
        self.container = self.c.containers.run(DOCKER_IMAGE, command, cpuset_cpus=self.id,
                                               name=self.name, volumes=vols, detach=True)
        self.r.rpush("available_dockers", self.name)

    def _run(self):
        try:
            for stats in self.c.api.stats(self.name, decode=True):
                if not self.stopped:
                    try:
                        cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - \
                            stats["precpu_stats"]["cpu_usage"]["total_usage"]
                        system_delta = stats["cpu_stats"]["system_cpu_usage"] - \
                            stats["precpu_stats"]["system_cpu_usage"]
                        total_cpu_usage = cpu_delta / float(system_delta) * 100 * TOTAL_CPUS
                        self.monitoring_info[self.function][self.name] = float("{0:.2f}".format(total_cpu_usage))
                    except:
                        pass
                gevent.sleep(0)
            msg = '404 Client Error: Not Found ("No such container: '+self.name+'")'
            self.stop(msg)
        except NotFound as e:
            self.stop(e)
        except KeyboardInterrupt:
            exit()

    def load_function(self, function, worker_dir):
        # move function to docker directory
        _, scope, function = function.split('/')
        logger.info("Loading Function '"+function+"' to docker "+self.name)
        bin_function_path = os.path.join(FUNCTIONS_DIR, scope, 'bin', function)
        worker_function_path = os.path.join(worker_dir, 'function')
        p = Popen(['cp', '-p', '-R', bin_function_path, worker_function_path])
        p.wait()

        function_obj_name = function+'.tar.gz'
        cached_function_obj = os.path.join(FUNCTIONS_DIR, scope, 'cache',
                                           function_obj_name)
        function_metadata = get_object_metadata(cached_function_obj)

        if MEMORY_HEADER not in function_metadata or TIMEOUT_HEADER not in \
           function_metadata or MAIN_HEADER not in function_metadata:
            raise ValueError("Error Getting Function memory and timeout values")
        else:
            memory = int(function_metadata[MEMORY_HEADER])
            main_class = function_metadata[MAIN_HEADER]

        function_log_name = function+'.log'
        function_log_obj = os.path.join(FUNCTIONS_DIR, scope, 'logs', function,
                                        function_log_name)
        function_log = open(function_log_obj, 'a')

        # Execute function
        self.fds = list()
        self.fdmd = list()
        self.fds.append(function_log)
        md = dict()
        md['function'] = function+'.tar.gz'
        md['main_class'] = main_class
        self.fdmd.append(md)
        dtg = Datagram()
        dtg.set_files(self.fds)
        dtg.set_metadata(self.fdmd)
        dtg.set_command(1)
        # Send datagram to function worker
        channel = os.path.join(self.channel_dir, 'pipe')
        rc = Bus.send(channel, dtg)
        if (rc < 0):
            raise Exception("Failed to send execute command")
        function_log.close()

        # TODO: Update docker memory

    def stop(self, message):
        if not self.stopped:
            self.stopped = True
            self.r.zrem(self.function, self.name)
            del self.monitoring_info[self.function][self.name]
            if len(self.monitoring_info[self.function]) == 0:
                del self.monitoring_info[self.function]
            self.container.remove(force=True)
            logger.warning(message)


def start_worker(containers, function):
    r = redis.Redis(connection_pool=REDIS_CONN_POOL)
    docker_id = r.lpop('available_dockers')
    if docker_id:
        logger.info("Starting new Function worker for: "+function)
        c_id = int(docker_id.replace('zion_', ''))
        worker_dir = os.path.join(MAIN_DIR, function, docker_id)
        p = Popen(['ln', '-s', POOL_DIR+docker_id, worker_dir])
        p.wait()
        container = containers[c_id]
        container.load_function(function, worker_dir)
        r.zadd(function, docker_id, 0)


def worker_timeout_checker(containers, workers_to_kill, monitoring_info):
    while True:
        if not workers_to_kill:
            gevent.sleep(1)
            continue
        for function in workers_to_kill.keys():
            workers = workers_to_kill[function]
            for worker in workers.keys():
                logger.info(worker+" timeout: "+str(workers[worker]))
                workers[worker] -= 1
                if workers[worker] == 0:
                    docker_id = int(worker.replace('zion_', ''))
                    docker = containers[docker_id]
                    docker.stop(function+" worker timeout, killing docker '"+worker+"'")
                    del workers_to_kill[function][worker]
                    # docker.regenerate()
                    # TODO: start_container()
            if function in workers_to_kill and len(workers_to_kill[function]) == 0:
                del workers_to_kill[function]
        gevent.sleep(0)


def monitoring_info_auditor(containers, monitoring_info):
    r = redis.Redis(connection_pool=REDIS_CONN_POOL)
    workers_to_kill = dict()

    # Worker timeout checker
    Greenlet.spawn(worker_timeout_checker, containers, workers_to_kill, monitoring_info)

    while True:
        if not monitoring_info:
            gevent.sleep(1)
            continue

        logger.info(monitoring_info)

        for function in monitoring_info:
            if function not in workers_to_kill:
                workers_to_kill[function] = dict()

            function_cpu_usage = 0
            workers = monitoring_info[function]
            total_function_workers = len(workers)
            active_function_workers = total_function_workers - len(workers_to_kill[function])
            sorted_workers = sorted(workers.items(), key=operator.itemgetter(1))
            for worker in sorted_workers:
                docker = worker[0]
                worker_cpu_usage = worker[1]
                if docker not in workers_to_kill[function]:
                    function_cpu_usage += worker_cpu_usage
                    # r.zadd(function, docker, worker_cpu_usage)
                if docker in workers_to_kill[function] and worker_cpu_usage > LOW_CPU_THRESHOLD:
                    del workers_to_kill[function][docker]
                    r.zadd(function, docker, worker_cpu_usage)

            if active_function_workers == 0:
                continue

            mean_fucntion_cpu_usage = function_cpu_usage / active_function_workers
            logger.info(mean_fucntion_cpu_usage)

            # Scale Up
            if mean_fucntion_cpu_usage > HIGH_CPU_THRESHOLD:
                if len(workers_to_kill[function]) > 0:
                    docker = random.sample(workers_to_kill[function], 1)[0]
                    logger.info("Reusing worker: "+docker)
                    del workers_to_kill[function][docker]
                else:
                    start_worker(containers, function)
                continue

            # Scale Down
            if active_function_workers > 1:
                if mean_fucntion_cpu_usage < ((active_function_workers-1)*HIGH_CPU_THRESHOLD):
                    if docker not in workers_to_kill[function]:
                        logger.info("Underutilized intermediate worker: "+docker)
                        r.zrem(function, docker)
                        workers_to_kill[function][docker] = WORKER_TIMEOUT
                else:
                    if docker in workers_to_kill[function]:
                        logger.info("Reusing worker: "+docker)
                        del workers_to_kill[function][docker]

            if active_function_workers == 1:
                if mean_fucntion_cpu_usage < LOW_CPU_THRESHOLD:
                    if docker not in workers_to_kill[function]:
                        logger.info("Underutilized last worker: "+docker)
                        workers_to_kill[function][docker] = WORKER_TIMEOUT

        gevent.sleep(0)


def monitoring(containers):
    r = redis.Redis(connection_pool=REDIS_CONN_POOL)
    monitoring_info = dict()

    logger.info("Starting monitoring thread")

    # Check monitoring info, and spawn new workers
    Greenlet.spawn(monitoring_info_auditor, containers, monitoring_info)

    while True:
        # Check for new workers
        functions = r.keys('workers*')
        if not functions:
            gevent.sleep(1)
            continue
        for function in functions:
            workers = r.zrange(function, 0, -1)
            if function not in monitoring_info:
                monitoring_info[function] = dict()
            for worker in workers:
                if worker not in monitoring_info[function]:
                    c_id = int(worker.replace('zion_', ''))
                    monitoring_info[function][worker] = 1
                    container = containers[c_id]
                    # Start monitoring of container
                    container.function = function
                    container.monitoring_info = monitoring_info
                    container.start()

        gevent.sleep(0)


def stop_containers():
    c = docker.from_env()
    r = redis.Redis(connection_pool=REDIS_CONN_POOL)
    for container in c.containers.list(all=True):
        if container.name.startswith("zion"):
            logger.info("Killing container: "+container.name)
            container.remove(force=True)
    r.delete("available_dockers")
    workers_list = r.keys('workers*')
    for workers_list_id in workers_list:
        r.delete(workers_list_id)
    if os.path.exists(WORKERS_DIR):
        shutil.rmtree(WORKERS_DIR)
    if os.path.exists(POOL_DIR):
        shutil.rmtree(POOL_DIR)


def start_containers(containers):
    if not os.path.exists(WORKERS_DIR):
        os.makedirs(WORKERS_DIR)
    os.chown(WORKERS_DIR, swift_uid, swift_gid)
    if not os.path.exists(POOL_DIR):
        os.makedirs(POOL_DIR)
    os.chown(POOL_DIR, swift_uid, swift_gid)
    for cid in range(WORKERS):
        worker = Container(cid)
        containers[cid] = worker


def main():
    containers = dict()
    try:
        # Kill all already started Zion containers
        stop_containers()
        # Start base containers
        start_containers(containers)
        # Start monitoring
        monitor = Greenlet.spawn(monitoring, containers)

        gevent.joinall([monitor])
    except KeyboardInterrupt:
        stop_containers()
        exit()


if __name__ == '__main__':
    main()
    """
    myname = os.path.basename(sys.argv[0])
    pidfile = '/tmp/%s' % myname
    daemon = Daemonize(app=myname, pid=pidfile, action=main)
    daemon.start()
    """
