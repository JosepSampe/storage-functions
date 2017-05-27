import os


class Worker(object):
    """
    Worker main class.
    """

    def __init__(self, conf, scope, redis, function):
        self.conf = conf
        self.function_name = function.get_name()
        self.redis = redis
        self.scope = scope
        self.main_dir = conf["main_dir"]
        self.workers_dir = conf["workers_dir"]

        self._get_available_docker()
        self._init_path()

    def _get_available_docker(self):
        self.docker_id = self.redis.lpop('available_dockers')

    def _init_path(self):
        path = os.path.join(self.main_dir, self.workers_dir, self.scope,
                            self.function_name, self.docker_id)

        if not os.path.exists(path):
            os.makedirs(path)
