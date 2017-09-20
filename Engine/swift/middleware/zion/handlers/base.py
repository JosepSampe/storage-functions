from zion.gateways import DockerGateway
from zion.common.utils import DataFdIter

from swift.common.swob import Response
import os


class NotFunctionRequest(Exception):
    pass


def _request_instance_property():
    """
    Set and retrieve the request instance.
    This works to force to tie the consistency between the request path and
    self.vars (i.e. api_version, account, container, obj) even if unexpectedly
    (separately) assigned.
    """

    def getter(self):
        return self._request

    def setter(self, request):
        self._request = request
        try:
            self._extract_vaco()
        except ValueError:
            raise NotFunctionRequest()

    return property(getter, setter,
                    doc="Force to tie the request to acc/con/obj vars")


class BaseHandler(object):
    """
    This is an abstract handler for Proxy/Compute Server middleware
    """
    req = _request_instance_property()

    def __init__(self, req, conf, app, logger, redis):
        """
        :param req: swob.Request instance
        :param conf: gatway conf dict
        """
        self.req = req
        self.conf = conf
        self.app = app
        self.logger = logger
        self.redis = redis
        self.method = self.req.method
        self.execution_server = conf["execution_server"]
        self.functions_container = conf.get('function_container')
        self.available_set_headers = ['X-Function-Onput',
                                      'X-Function-Onget',
                                      'X-Function-Onget-Before',
                                      'X-Function-Onget-Manifest',
                                      'X-Function-Ondelete']
        self.available_unset_headers = ['X-Function-Onput-Delete',
                                        'X-Function-Onget-Delete',
                                        'X-Function-Onget-Before-Delete',
                                        'X-Function-Onget-Manifest-Delete',
                                        'X-Function-Ondelete-Delete',
                                        'X-Function-Delete']
        self.function_methods = ['GET', 'PUT', 'DELETE']
        self.get_keys = ['onget', 'onget-before', 'onget-manifest']
        self.put_keys = ['onput']
        self.del_keys = ['ondelete']
        self.mandatory_function_metadata = ['Language', 'Memory',
                                            'Timeout', 'Main']

    def _setup_docker_gateway(self, response=None):
        self.req.headers['X-Current-Server'] = self.execution_server
        self.req.headers['X-Method'] = self.method.lower()
        self.req.headers['X-Current-Location'] = os.path.join("/", self.api_version,
                                                              self.account, self.container)
        self.req.headers['X-Project-Id'] = self.account.replace('AUTH_', '')
        self.req.headers['X-Container'] = self.container
        self.req.headers['X-Object'] = self.obj

        self.docker_gateway = DockerGateway(self)

    def _extract_vaco(self):
        """
        Set version, account, container, obj vars from self._parse_vaco result
        :raises ValueError: if self._parse_vaco raises ValueError while
                            parsing, this method doesn't care and raise it to
                            upper caller.
        """
        self._api_version, self._account, self._container, self._obj = \
            self._parse_vaco()

    @property
    def api_version(self):
        return self._api_version

    @property
    def account(self):
        return self._account

    @property
    def container(self):
        return self._container

    @property
    def obj(self):
        return self._obj

    def _parse_vaco(self):
        """
        Parse method of path from self.req which depends on child class
        (Proxy or Object)

        :return tuple: a string tuple of (version, account, container, object)
        """
        raise NotImplementedError()

    def handle_request(self):
        """
        Run Function middleware
        """
        raise NotImplementedError()

    @property
    def is_range_request(self):
        """
        Determines whether the request is a byte-range request
        """
        return 'Range' in self.req.headers

    @property
    def is_functions_container_request(self):
        """
        Determines whether the request is over the functions swift container
        """
        return self.container in self.functions_container and self.method != 'PUT'

    @property
    def is_function_object_put(self):
        return (self.container in self.functions_container and self.obj and
                self.method == 'PUT')

    def is_slo_object(self, resp):
        """ Determines whether the requested object is an SLO object """
        return 'X-Static-Large-Object' in resp.headers and \
            resp.headers['X-Static-Large-Object'] == 'True'

    @property
    def is_function_for_manifest(self):
        return 'X-Function-Onget-Manifest' in self.req.headers \
            or 'X-Function-Onget-Manifest-Delete' in self.req.headers

    @property
    def is_slo_get_request(self):
        """
        Determines from a GET request and its  associated response
        if the object is a SLO
        """
        return self.req.params.get('multipart-manifest') == 'get'

    @property
    def is_copy_request(self):
        """
        Determines from a GET request if is a copy request
        """
        return 'X-Copy-From' in self.req.headers

    @property
    def is_function_enabled(self):
        return self.req.headers['function-enabled'] == 'True'

    @property
    def is_function_set_to_container(self):
        return not self.obj and self.method == 'POST'

    @property
    def is_head_request(self):
        return self.method == 'HEAD'

    @property
    def is_valid_request(self):
        """
        Determines if is a valid request
        """
        mandatory = all([not self.is_copy_request,
                         not self.is_slo_get_request,
                         self.is_function_enabled,
                         not self.is_functions_container_request])

        optional = any([self.is_function_set_to_container,
                        self.is_head_request,
                        self.is_function_object_put])

        return any([mandatory, optional])

    @property
    def is_function_set(self):
        return any((True for x in self.available_set_headers
                    if x in self.req.headers.keys()))

    @property
    def is_function_unset(self):
        return any((True for x in self.available_unset_headers
                    if x in self.req.headers.keys()))

    def is_slo_response(self, resp):
        self.logger.debug(
            'Verify if {0}/{1}/{2} is an SLO assembly object'.format(
                self.account, self.container, self.obj))
        is_slo = 'X-Static-Large-Object' in resp.headers
        if is_slo:
            self.logger.debug(
                '{0}/{1}/{2} is indeed an SLO assembly '
                'object'.format(self.account, self.container, self.obj))
        else:
            self.logger.debug(
                '{0}/{1}/{2} is NOT an SLO assembly object'.format(
                    self.account, self.container, self.obj))
        return is_slo

    def _process_function_response_onput(self, f_data):
        """
        Processes the data returned from the function
        """
        if f_data['command'] == 'DW':
            # Data Write from function
            new_fd = f_data['fd']  # Data from function fd
            self.req.environ['wsgi.input'] = DataFdIter(new_fd)
            if 'request_headers' in f_data:
                self.req.headers.update(f_data['request_headers'])
            if 'object_metadata' in f_data:
                self.req.headers.update(f_data['object_metadata'])

        elif f_data['command'] == 'RC':
            # Request Continue: normal req. execution
            if 'request_headers' in f_data:
                self.req.headers.update(f_data['request_headers'])
            if 'object_metadata' in f_data:
                self.req.headers.update(f_data['object_metadata'])

        elif f_data['command'] == 'RR':
            # Request Rewire to another object
            pass
            # TODO

        elif f_data['command'] == 'RE':
            # Request Error
            msg = f_data['message']
            return Response(body=msg + '\n', headers={'etag': ''},
                            request=self.req)

        response = self.req.get_response(self.app)

        if 'response_headers' in f_data:
            response.headers.update(f_data['response_headers'])

        return response

    def _process_function_response_onget(self, f_data):
        """
        Processes the response from the function
        """
        if f_data['command'] == 'DW':
            # Data Write from function
            new_fd = f_data['fd']
            self.response.app_iter = DataFdIter(new_fd)
            if 'object_metadata' in f_data:
                self.response.headers.update(f_data['object_metadata'])
            if 'response_headers' in f_data:
                self.response.headers.update(f_data['response_headers'])

            if 'Content-Length' in self.response.headers:
                self.response.headers.pop('Content-Length')
            if 'Transfer-Encoding' in self.response.headers:
                self.response.headers.pop('Transfer-Encoding')
            if 'Etag' in self.response.headers:
                self.response.headers['Etag'] = ''

        elif f_data['command'] == 'RC':
            # Request Continue: normal req. execution
            if 'object_metadata' in f_data:
                self.response.headers.update(f_data['object_metadata'])
            if 'response_headers' in f_data:
                self.response.headers.update(f_data['response_headers'])

        elif f_data['command'] == 'RR':
            # Request Rewire to another object
            pass
            # TODO

        elif f_data['command'] == 'RE':
            # Request Error
            msg = f_data['message']
            self.response = Response(body=msg + '\n',
                                     headers={'etag': ''},
                                     request=self.req)

    def apply_function_onput(self):
        """
        Call gateway module to get result of function execution
        in PUT flow
        """
        if self.function_data:
            function_info = eval(self.function_data['onput'])
            self.logger.info('There are functions to execute: ' +
                             str(self.function_data))
            self._setup_docker_gateway()
            function_resp = self.docker_gateway.execute_function(function_info)

            return self._process_function_response_onput(function_resp)
        else:
            return self.req.get_response(self.app)

    def apply_function_onget(self):
        """
        Call gateway module to get result of function execution
        in GET flow
        """
        if self.function_data:
            function_info = eval(self.function_data['onget'])
            self.logger.info('There are functions to execute: ' +
                             str(self.function_data))
            self._setup_docker_gateway()
            function_resp = self.docker_gateway.execute_function(function_info)
            self._process_function_response_onget(function_resp)

        if 'Content-Length' not in self.response.headers:
            self.response.headers['Content-Length'] = None
            if 'Transfer-Encoding' in self.response.headers:
                self.response.headers.pop('Transfer-Encoding')
