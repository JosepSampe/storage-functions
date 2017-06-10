from daemonize import Daemonize
from gevent import Greenlet
from docker.errors import NotFound
from gevent.timeout import Timeout
from subprocess import Popen
import shutil
import gevent
import logging
import operator
import docker
import redis
import psutil
import json
import time
import sys
import os


TOTAL_CPUS = psutil.cpu_count()
REDIS_CONN_POOL = redis.ConnectionPool(host='localhost', port=6379, db=10)
CPU_THRESHOLD = 30
WORKERS = 2
ZION_DIR = '/opt/zion'
MAIN_DIR = '/home/docker_device/blackeagle/'
WORKERS_DIR = MAIN_DIR+'workers/'
POOL_DIR = MAIN_DIR+'docker_pool/'
DOCKER_IMAGE = '192.168.2.1:5001/blackeagle'

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

    def __init__(self, cid, monitoring):
        Greenlet.__init__(self)
        self.id = str(cid)
        self.name = "zion_"+str(cid)
        self.monitoring = monitoring
        self.status = "free"
        self.contaoner = None
        self.docker_dir = POOL_DIR+self.name
        self.channel_dir = self.docker_dir+'/channel'
        self.function_dir = self.docker_dir+'/function'
        self.r = redis.Redis(connection_pool=REDIS_CONN_POOL)
        self.c = docker.from_env()

        self._create_directory_structure()

    def _create_directory_structure(self):
        logger.info("Creating container structure: "+self.docker_dir)
        if not os.path.exists(self.docker_dir):
            p = Popen(['cp', '-p', '-R', ZION_DIR, self.docker_dir])
            p.wait()
            os.makedirs(self.channel_dir)
            os.chown(self.channel_dir, swift_uid, swift_gid)
            os.makedirs(self.function_dir)
            os.chown(self.function_dir, swift_uid, swift_gid)

    def _start_container(self):
        logger.info("Starting container: "+self.name)
        command = 'debug "/opt/zion/runtime/java/start_daemon.sh"'
        vols = {'/dev/log': {'bind': '/dev/log', 'mode': 'rw'},
                self.docker_dir: {'bind': '/opt/zion', 'mode': 'rw'}}
        self.container = self.c.containers.run(DOCKER_IMAGE, command, cpuset_cpus=self.id,
                                               name=self.name, volumes=vols, detach=True)
        self.r.rpush("available_dockers", self.name)

    def _run(self):
        self._start_container()
        while True:
            gevent.sleep(1)
        """
        try:
            for stats in c.api.stats(c_name, decode=True):
                try:
                    cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - \
                        stats["precpu_stats"]["cpu_usage"]["total_usage"]
                    system_delta = stats["cpu_stats"]["system_cpu_usage"] - \
                        stats["precpu_stats"]["system_cpu_usage"]
                    total_cpu_usage = cpu_delta / float(system_delta) * 100 * TOTAL_CPUS
                    monitoring_info[redis_key][c_name] = float("{0:.2f}".format(total_cpu_usage))
                    gevent.sleep(0)
                except:
                    pass
            r.zrem(redis_key, c_name)
            del monitoring_info[redis_key]
            logger.error('404 Client Error: Not Found ("No such container: '+c_name+'")')
        except NotFound as e:
            r.zrem(redis_key, c_name)
            del monitoring_info[redis_key]
            logger.error(str(e))
        except KeyboardInterrupt:
            exit()
        """


def monitor_container_cpu(c_name, redis_key, monitoring_info):
    r = redis.Redis(connection_pool=REDIS_CONN_POOL)
    c = docker.from_env()
    try:
        for stats in c.api.stats(c_name, decode=True):
            try:
                cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - \
                    stats["precpu_stats"]["cpu_usage"]["total_usage"]
                system_delta = stats["cpu_stats"]["system_cpu_usage"] - \
                    stats["precpu_stats"]["system_cpu_usage"]
                total_cpu_usage = cpu_delta / float(system_delta) * 100 * TOTAL_CPUS
                monitoring_info[redis_key][c_name] = float("{0:.2f}".format(total_cpu_usage))
                gevent.sleep(0)
            except:
                pass
        r.zrem(redis_key, c_name)
        del monitoring_info[redis_key]
        logger.error('404 Client Error: Not Found ("No such container: '+c_name+'")')
    except NotFound as e:
        r.zrem(redis_key, c_name)
        del monitoring_info[redis_key]
        logger.error(str(e))
    except KeyboardInterrupt:
        exit()


def monitoring_info_auditor(monitoring_info):
    r = redis.Redis(connection_pool=REDIS_CONN_POOL)
    try:
        while True:
            if not monitoring_info:
                gevent.sleep(1)
                continue
            logger.info(monitoring_info)
            for function in monitoring_info:
                workers = monitoring_info[function]
                sorted_workers = sorted(workers.items(), key=operator.itemgetter(1))
                for worker in sorted_workers:
                    docker = worker[0]
                    cpu_usage = worker[1]
                    r.zadd(function, docker, cpu_usage)
            gevent.sleep(0)
    except KeyboardInterrupt:
        exit()


def monitoring():
    r = redis.Redis(connection_pool=REDIS_CONN_POOL)
    monitoring_info = dict()
    threads = list()

    # Check monitoring info, and spawn new workers
    threads.append(Greenlet.spawn(monitoring_info_auditor, monitoring_info))

    try:
        while True:
            # Check for new workers, and spawn container monitoring
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
                        monitoring_info[function][worker] = 0
                        threads.append(Greenlet.spawn(monitor_container_cpu,
                                                      worker,
                                                      function,
                                                      monitoring_info))
            gevent.sleep(0)
    except KeyboardInterrupt:
            exit()


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


def start_containers(containers, monitoring):
    if not os.path.exists(WORKERS_DIR):
        os.makedirs(WORKERS_DIR)
    os.chown(WORKERS_DIR, swift_uid, swift_gid)
    if not os.path.exists(POOL_DIR):
        os.makedirs(POOL_DIR)
    os.chown(POOL_DIR, swift_uid, swift_gid)
    for cid in range(WORKERS):
        worker = Container(cid, monitoring)
        containers.append(worker)
        worker.start()


def main():
    containers = list()
    monitoring = dict()
    # Kill all already started Zion containers
    stop_containers()
    # Start base containers
    start_containers(containers, monitoring)
    # Start monitoring
    # containers.append(Greenlet.spawn(monitoring))

    try:
        gevent.joinall(containers)
    except KeyboardInterrupt:
        exit()


if __name__ == '__main__':
    main()
    """
    myname = os.path.basename(sys.argv[0])
    pidfile = '/tmp/%s' % myname
    daemon = Daemonize(app=myname, pid=pidfile, action=main)
    daemon.start()
    """
