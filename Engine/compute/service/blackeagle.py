from daemonize import Daemonize
from gevent import Greenlet
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


def monitor_container_cpu(container_name, redis_key, monitoring_info):
    running = True
    c = docker.from_env()
    while running:
        try:
            for stats in c.api.stats(container_name, decode=True):
                try:
                    cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - \
                        stats["precpu_stats"]["cpu_usage"]["total_usage"]
                    system_delta = stats["cpu_stats"]["system_cpu_usage"] - \
                        stats["precpu_stats"]["system_cpu_usage"]
                    total_cpu_usage = cpu_delta / float(system_delta) * 100 * TOTAL_CPUS
                    monitoring_info[redis_key][container_name] = float("{0:.2f}".format(total_cpu_usage))
                    gevent.sleep(0)
                except:
                    pass
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
                        threads.append(Greenlet.spawn(monitor_container_cpu,
                                                      worker,
                                                      function,
                                                      monitoring_info))
            gevent.sleep(0)
    except KeyboardInterrupt:
            exit()


def main():
    threads = list()
    c = docker.from_env()
    # start base containers

    # Start monitoring
    threads.append(Greenlet.spawn(monitoring))

    gevent.joinall(threads)


if __name__ == '__main__':
    main()
    """
    myname = os.path.basename(sys.argv[0])
    pidfile = '/tmp/%s' % myname
    daemon = Daemonize(app=myname, pid=pidfile, action=main)
    daemon.start()
    """
