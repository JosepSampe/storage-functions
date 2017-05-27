from blackeagle.gateways.docker.protocol import Protocol
from blackeagle.gateways.docker.function import Function
from blackeagle.gateways.docker.worker import Worker
import redis
import os


class DockerGateway():

    def __init__(self, request, response, conf, logger, account):
        self.req = request
        self.response = response
        self.conf = conf
        self.logger = logger
        self.account = account
        self.method = self.req.method.lower()
        self.scope = account[5:18]
        self.functions_container = conf["functions_container"]
        self.execution_server = conf["execution_server"]

        self._connect_redis()

    def _connect_redis(self):
        self.redis_host = self.conf.get('redis_host')
        self.redis_port = self.conf.get('redis_port')
        self.redis_db = self.conf.get('redis_db')

        self.redis = redis.StrictRedis(self.redis_host,
                                       self.redis_port,
                                       self.redis_db)

    def _get_worker(self, function_name):
        key = os.path.join(self.scope, function_name)
        # worker = self.redis.get(key+"*")
        worker = None
        return worker

    def _get_object_stream(self):
        if self.method == 'get':
            return self.response.app_iter
        if self.method == 'put':
            return self.req.environ['wsgi.input']

    def _get_object_metadata(self):
        headers = dict()
        if self.method == "get":
            headers = self.response.headers
        elif self.method == "put":
            if 'Content-Length' in self.req.headers:
                headers['Content-Length'] = self.req.headers['Content-Length']
            if 'Content-Type' in self.req.headers:
                headers['Content-Type'] = self.req.headers['Content-Type']
            for header in self.req.headers:
                if header.startswith('X-Object'):
                    headers[header] = self.req.headers[header]

        return headers

    def execute_function(self, function_info):
        """
        Executes the function.

        :param function_info: function information
        :returns: response from the function
        """
        # 1st. Get necessary data
        object_stream = self._get_object_stream()
        object_metadata = self._get_object_metadata()
        request_headers = dict(self.req.headers)
        f_name = function_info.keys()[0]
        function_parameters = function_info[f_name]

        # 2nd. Create function Instance
        function = Function(self.conf, self.scope, f_name)

        # 3rd. Get already started worker or create it
        worker = self._get_worker()
        if not worker:
            worker = Worker(self.conf, self.scope, self.redis, function)

        """
        # 3rd. Create function communication protocol
        protocol = Protocol(function, worker, object_stream,
                            object_metadata, request_headers,
                            function_parameters, self.logger)

        return protocol.comunicate()
        """

        function_response = dict()
        function_response['command'] = 'RC'

        return function_response
