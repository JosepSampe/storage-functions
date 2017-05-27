import os


class Worker(object):
    """
    Worker main class.
    """

    def __init__(self, be, scope, redis, function):
        self.conf = be.conf
        self.scope = scope
        self.redis = redis
        self.function = function
        self.function_name = function.get_name()
        # Dirs
        self.main_dir = self.conf["main_dir"]
        self.workers_dir = self.conf["workers_dir"]
        self.docker_dir = self.conf["docker_pool_dir"]

        self._get_available_docker()
        self._link_worker_to_docker()
        self._link_worker_to_function()
        self._execute()

    def _get_available_docker(self):
        # self.docker_id = self.redis.lpop('available_dockers')
        self.docker_id = "zion_0"

    def _link_worker_to_docker(self):
        self.worker_path = os.path.join(self.main_dir, self.workers_dir,
                                        self.scope, self.function_name)

        if not os.path.exists(self.worker_path):
            os.makedirs(self.worker_path)

        docker_path = os.path.join(self.main_dir, self.docker_dir, self.docker_id)
        worker_docker_link = os.path.join(self.worker_path, self.docker_id)

        try:
            os.symlink(docker_path, worker_docker_link)
        except Exception:
            os.remove(worker_docker_link)
            os.symlink(docker_path, worker_docker_link)

        self.worker_channel = os.path.join(worker_docker_link, 'channel', 'pipe')

    def _link_worker_to_function(self):
        function_bin_path = self.function.get_bin_path()
        worker_function_link = os.path.join(self.worker_path, self.docker_id, 'function')

        try:
            os.symlink(function_bin_path, worker_function_link)
        except Exception:
            os.remove(worker_function_link)
            os.symlink(function_bin_path, worker_function_link)

    def _execute(self):
        """
        Executes the function into the attached worker
        """
        # TODO: Send execute command to docker
        pass

    def get_channel(self):
        return self.worker_channel
