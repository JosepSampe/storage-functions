from zion.gateways.docker.bus import Bus
from zion.gateways.docker.datagram import Datagram
import shutil
import random
import os


class Worker:
    """
    Worker main class.
    """

    def __init__(self, conf, account, logger, redis, function):
        self.conf = conf
        self.account = account
        self.redis = redis
        self.function = function
        self.logger = logger
        self.function_name = function.get_name()
        self.function_obj = function.get_obj_name()

        self.scope = self.account[5:18]

        self.worker_key = os.path.join('workers', self.scope, self.function_name)
        # Dirs
        self.main_dir = self.conf["main_dir"]
        self.workers_dir = self.conf["workers_dir"]
        self.docker_dir = self.conf["docker_pool_dir"]

        if not self._get_available_worker():
            self._get_available_docker()
            self._link_worker_to_docker()
            self._link_worker_to_function()
            self._initiate_function()

    def _get_available_worker(self):
        # docker_id = self.redis.zrange(self.worker_key, 0, 0)
        self.logger.info("Worker - Getting available worker")
        workers = self.redis.zrange(self.worker_key, 0, -1)

        if workers:
            docker_id = random.sample(workers, 1)[0].decode()
            if docker_id:
                docker_id = docker_id
                worker_path = os.path.join(self.main_dir, self.workers_dir,
                                           self.scope, self.function_name, docker_id)
                self.worker_channel = os.path.join(worker_path, 'channel', 'pipe')
                self.logger.info("Worker - There is an available worker for "+self.function_obj+" in "+docker_id)
                return True
        else:
            self.logger.info("Worker - There are no available workers for "+self.function_obj)
            return False

    def _get_available_docker(self):
        self.logger.info("Worker - Getting available docker from pool")

        self.docker_id = self.redis.lpop('available_dockers').decode()

        if self.docker_id:
            self.logger.info("Worker - Got docker '"+self.docker_id+"' from docker pool")
        else:
            msg = "Worker - No dockers available in the docker pool"
            self.logger.error(msg)
            raise ValueError(msg)

    def _link_worker_to_docker(self):
        self.logger.info("Worker - Linking worker to docker")
        self.worker_path = os.path.join(self.main_dir, self.workers_dir,
                                        self.scope, self.function_name)
        if not os.path.exists(self.worker_path):
            os.makedirs(self.worker_path)

        docker_path = os.path.join(self.main_dir, self.docker_dir, self.docker_id)
        worker_docker_link = os.path.join(self.worker_path, self.docker_id)
        self.logger.info("Worker - Worker path: " + worker_docker_link)
        try:
            os.symlink(docker_path, worker_docker_link)
        except:
            os.remove(worker_docker_link)
            os.symlink(docker_path, worker_docker_link)

        self.worker_channel = os.path.join(worker_docker_link, 'channel', 'pipe')

        self.redis.zadd(self.worker_key, {self.docker_id: 0})

    def _link_worker_to_function(self):
        self.logger.info("Worker - Linking function to worker")
        function_bin_path = self.function.get_bin_path()
        worker_function_link = os.path.join(self.worker_path, self.docker_id, 'function')

        if os.path.exists(worker_function_link):
            shutil.rmtree(worker_function_link)

        try:
            shutil.copytree(function_bin_path, worker_function_link)
        except:
            shutil.copy2(function_bin_path, worker_function_link)

    def _initiate_function(self):
        """
        Initiates the function into the attached docker
        """
        self.logger.info("Worker - Starting execution")
        self.fds = list()
        self.fdmd = list()

        self.function.open_log()
        self.fds.append(self.function.get_logfd())

        md = dict()
        md['function'] = self.function.get_obj_name()
        md['main_class'] = self.function.get_main_class()
        self.fdmd.append(md)

        dtg = Datagram()
        dtg.set_files(self.fds)
        dtg.set_metadata(self.fdmd)
        dtg.set_command(1)

        # Send datagram to function worker
        self.logger.info("Worker - Pipe: " + self.worker_channel)
        rc = Bus.send(self.worker_channel, dtg)
        if (rc < 0):
            raise Exception("Failed to send execute command")
        self.function.close_log()

    def get_channel(self):
        return self.worker_channel
