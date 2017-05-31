from blackeagle.gateways.docker.protocol import Protocol
from blackeagle.gateways.docker.function import Function
from blackeagle.gateways.docker.worker import Worker
import pickle
import time
import os


class DockerGateway():

    def __init__(self, be):
        self.be = be
        self.req = be.req
        self.conf = be.conf
        self.logger = be.logger
        self.account = be.account
        self.redis = be.redis
        self.method = self.req.method.lower()
        self.scope = self.account[5:18]
        self.functions_container = self.conf["functions_container"]
        self.execution_server = self.conf["execution_server"]

    def _get_object_stream(self):
        if self.method == 'get':
            return self.be.response.app_iter
        if self.method == 'put':
            return self.req.environ['wsgi.input']

    def _get_object_metadata(self):
        headers = dict()
        if self.method == "get":
            headers = self.be.response.headers
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
        object_stream = self._get_object_stream()
        object_metadata = self._get_object_metadata()
        request_headers = dict(self.req.headers)
        f_name = function_info.keys()[0]
        if function_info[f_name]:
            function_parameters = eval(function_info[f_name])
        else:
            function_parameters = dict()

        time1 = time.time()
        function = Function(self.be, self.scope, f_name)
        time2 = time.time()
        print '------ FUNCTION took %0.6f s' % ((time2-time1))

        time1 = time.time()
        worker = Worker(self.be, self.scope, self.redis, function)
        time2 = time.time()
        print '------ WORKER took %0.6f s' % ((time2-time1))

        time1 = time.time()
        protocol = Protocol(worker, object_stream, object_metadata,
                            request_headers, function_parameters, self.be)
        resp = protocol.comunicate()
        time2 = time.time()
        print '----- PROTOCOL took %0.6f s' % ((time2-time1))

        # return {"command": "RC"}
        return resp
