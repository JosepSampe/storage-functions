from blackeagle.handlers import BaseHandler
from blackeagle.common.utils import DataIter
from blackeagle.handlers.base import NotFunctionRequest

from swift.common.swob import HTTPNotFound, HTTPUnauthorized, Response
from swift.common.utils import public
from swift.common.wsgi import make_subrequest
import os
import random
import redis

from swiftclient.client import http_connection, quote


class ProxyHandler(BaseHandler):

    def __init__(self, request, conf, app, logger):
        super(ProxyHandler, self).__init__(
            request, conf, app, logger)

        self.functions_container = self.conf["functions_container"]
        self.compute_nodes = self.conf["compute_nodes"]
        self.req.headers['function-enabled'] = True

        self.redis_host = conf.get('redis_host')
        self.redis_port = conf.get('redis_port')
        self.redis_db = conf.get('redis_db')

        self.metadata_server = redis.StrictRedis(self.redis_host,
                                                 self.redis_port,
                                                 self.redis_db)

    def _parse_vaco(self):
        return self.req.split_path(3, 4, rest_with_last=True)

    def _get_functions(self):
        self.function_list = dict()
        self.function_data = dict()
        if self.obj:
            key = self.req.path
            self.function_list = self.metadata_server.hgetall(key)

        key = os.path.join('/', self.api_version, self.account, self.container)
        self.parent_function_list = self.metadata_server.hgetall(key)

        self.function_data.update(self.parent_function_list)
        self.function_data.update(self.function_list)

    def handle_request(self):
        if hasattr(self, self.method) and self.is_valid_request:
            try:
                self._get_functions()
                handler = getattr(self, self.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                raise NotFunctionRequest()
            return handler()
        else:
            raise NotFunctionRequest()

    def _verify_access(self, cont, obj):
        """
        Verifies access to the specified object in swift
        :param cont: swift container name
        :param obj: swift object name
        :raise HTTPNotFound: if the object doesn't exists in swift
        :return response: Object response
        """
        if obj:
            path = os.path.join('/', self.api_version, self.account, cont, obj)
        else:
            path = os.path.join('/', self.api_version, self.account, cont)
        self.logger.debug('Verifying access to %s' % path)

        new_env = dict(self.req.environ)
        if 'HTTP_TRANSFER_ENCODING' in new_env.keys():
            del new_env['HTTP_TRANSFER_ENCODING']

        auth_token = self.req.headers.get('X-Auth-Token')
        sub_req = make_subrequest(new_env, 'HEAD', path,
                                  headers={'X-Auth-Token': auth_token},
                                  swift_source='function_middleware')

        resp = sub_req.get_response(self.app)

        if not resp.is_success:
            if resp.status_int == 401:
                raise HTTPUnauthorized('Unauthorized to access to this '
                                       'resource: ' + path + '\n')
            else:
                raise HTTPNotFound('There was an error: "' + path +
                                   ' doesn\'t exists in Swift.\n')

    def _get_function_set_data(self):
        params = dict()
        header = [i for i in self.available_set_headers
                  if i in self.req.headers.keys()]
        if len(header) > 1:
            raise HTTPUnauthorized('The system can only set 1 '
                                   'function at a time.\n')

        trigger = header[0].lower().replace('-manifest', '').rsplit('-', 1)[1]
        function = self.req.headers[header[0]]+".tar.gz"

        if self.req.body:
            params = self.req.body

        return trigger, function, params

    def _get_function_unset_data(self):
        header = [i for i in self.available_unset_headers
                  if i in self.req.headers.keys()]
        if len(header) > 1:
            raise HTTPUnauthorized('The system can only unset 1 '
                                   'function at a time.\n')

        trigger = header[0].lower().replace('-manifest', '').rsplit('-', 2)[1]
        function = self.req.headers[header[0]]+".tar.gz"

        return trigger, function

    def _set_function(self):
        """
        Sets the specified function to the trigger of an object or a container
        """
        trigger, function, params = self._get_function_set_data()
        # Verify access to the function
        self._verify_access(self.functions_container, function)
        function_data = dict()
        function_data[function] = params
        key = self.req.path

        self._verify_access(self.container, self.obj)
        self.metadata_server.hset(key, trigger, function_data)

        msg = 'Function "' + function.replace('.tar.gz', '') + '" correctly ' \
              'assigned to the "' + trigger + '" trigger.\n'

        return Response(body=msg, headers={'etag': ''}, request=self.req)

    def _unset_function(self):
        """
        Unsets the specified function from the trigger of an object or a container
        """
        trigger, function = self._get_function_unset_data()
        key = self.req.path
        function_data = self.metadata_server.hgetall(key)
        if trigger in function_data:
            self.metadata_server.hdel(key, trigger)
            del function_data[trigger]
            if not function_data:
                self.metadata_server.delete(key)
            msg = 'Function "' + function.replace('.tar.gz', '') + '" correctly '\
                  ' removed from the "' + trigger + '" trigger.\n'
        else:
            msg = 'Error: Function "' + function.replace('.tar.gz', '') + '" not'\
                  ' assigned to the "' + trigger + '" trigger.\n'

        return Response(body=msg, headers={'etag': ''},
                        request=self.req)

    def _handle_get_trough_compute_node(self):
        self.req.headers['function_data'] = self.function_data

        compute_nodes = self.compute_nodes.split(',')
        compute_node = random.sample(compute_nodes, 1)

        self.logger.info('Forwarding request to a compute node: ' +
                         compute_node[0])

        url = os.path.join('http://', compute_node[0], self.api_version, self.account)
        parsed, conn = http_connection(url)
        path = '%s/%s/%s' % (parsed.path, quote(self.container), quote(self.obj))

        if 'Content-Type' in self.req.headers:
            self.req.headers.pop('Content-Type')
        if 'X-Domain-Name' in self.req.headers:
            self.req.headers.pop('X-Domain-Name')
        if 'X-Domain-Id' in self.req.headers:
            self.req.headers.pop('X-Domain-Id')

        conn.request(self.method, path, '', self.req.headers)
        resp = conn.getresponse()
        resp_headers = {}
        for header, value in resp.getheaders():
            resp_headers[header] = value

        response = Response(app_iter=DataIter(resp, 5),
                            headers=resp_headers,
                            request=self.req)

        return response

    @public
    def GET(self):
        """
        GET handler on Proxy
        """
        if self.function_data:
            self.logger.info('There are functions to execute: ' +
                             str(self.function_data))
            response = self._handle_get_trough_compute_node()
        else:
            response = self.req.get_response(self.app)

        if 'Content-Length' not in response.headers and \
           'Transfer-Encoding' in response.headers:
            response.headers.pop('Transfer-Encoding')

        return response

    @public
    def PUT(self):
        """
        PUT handler on Proxy
        """
        # TODO: Validate function PUT
        if self.function_data:
            self.logger.info('There are functions to execute: ' +
                             str(self.function_data))
            response = self._handle_put_trough_compute_node()
        else:
            response = self.req.get_response(self.app)

        return response

    @public
    def POST(self):
        """
        POST handler on Proxy
        """
        if self.is_function_set:
            self.logger.info('Setting function')
            response = self._set_function()
        elif self.is_function_unset:
            self.logger.info('Unsetting function')
            response = self._unset_function()
        else:
            response = self.req.get_response(self.app)

        return response

    @public
    def HEAD(self):
        """
        HEAD handler on Proxy
        """
        response = self.req.get_response(self.app)

        if self.conf['function_visibility']:
            if self.function_list:
                for trigger in self.function_list:
                    data = self.function_list[trigger]
                    response.headers['Function-'+trigger] = data

            if self.parent_function_list:
                for trigger in self.parent_function_list:
                    data = self.parent_function_list[trigger]
                    response.headers['Function-'+trigger+'-Container'] = data

        return response
