from zion.gateways.docker.protocol import Protocol
from zion.gateways.docker.function import Function
from zion.gateways.docker.worker import Worker
import time


class DockerGateway:

    def __init__(self, conf, app, req, response, account, logger, redis):
        self.conf = conf
        self.app = app
        self.req = req
        self.response = response
        self.account = account
        self.logger = logger
        self.redis = redis
        self.method = self.req.method.lower()
        self.functions_container = self.conf["functions_container"]
        self.execution_server = self.conf["execution_server"]

        self.logger.info('DockerGateway - DockerGateway instance created')

    def _get_object_stream(self):
        self.logger.info('DockerGateway - Getting object stream')
        if self.method == 'get':
            return self.response.app_iter
        if self.method == 'put':
            return self.req.environ['wsgi.input']

    def _get_object_metadata(self):
        self.logger.info('DockerGateway - Getting object metadata')
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
        self.logger.info('DockerGateway - Executing function')
        object_stream = self._get_object_stream()
        object_metadata = self._get_object_metadata()
        request_headers = dict(self.req.headers)

        f_name = list(function_info.keys())[0]

        if function_info[f_name]:
            function_parameters = function_info[f_name]
        else:
            function_parameters = dict()

        time1 = time.time()
        function = Function(self.conf, self.app, self.req, self.account, self.logger, f_name)
        time2 = time.time()
        fc = time2-time1
        self.logger.info('------> FUNCTION took %0.6f s' % ((time2-time1)))

        time1 = time.time()
        worker = Worker(self.conf, self.account, self.logger, self.redis, function)
        time2 = time.time()
        wkr = time2-time1
        self.logger.info('------> WORKER took %0.6f s' % ((time2-time1)))

        time1 = time.time()
        protocol = Protocol(self.logger, worker, object_stream, object_metadata,
                            request_headers, function_parameters)
        resp = protocol.comunicate()
        time2 = time.time()
        ptc = time2-time1
        self.logger.info('-----> PROTOCOL took %0.6f s' % ((time2-time1)))

        total = fc + wkr + ptc

        fl = open("/tmp/zion.times", "a")
        fl.write("%0.6f\t%0.6f\t%0.6f : \t%0.6f\n" % ((fc, wkr, ptc, total)))
        fl.close()

        # return {"command": "RC"}
        return resp
