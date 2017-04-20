from blackeagle.handlers import BaseHandler
from blackeagle.common.utils import verify_access
from blackeagle.common.utils import set_function_container
from blackeagle.common.utils import unset_function_from_container
from blackeagle.common.utils import DataIter
from blackeagle.common.utils import get_function_list_object

from swift.common.swob import HTTPMethodNotAllowed, HTTPNotFound
from swift.common.swob import HTTPUnauthorized, Response
from swift.common.utils import public, cache_from_env
from swift.common.wsgi import make_subrequest
import os

from eventlet import Timeout
from swiftclient.client import http_connection, quote
from swift.common.direct_client import gen_headers
from swift.common.bufferedhttp import http_connect


class ProxyHandler(BaseHandler):

    def __init__(self, request, conf, app, logger):
        super(ProxyHandler, self).__init__(
            request, conf, app, logger)

        self.function_container = self.conf["function_container"]
        self.memcache = None
        self.req.headers['function-enabled'] = True
        self.memcache = cache_from_env(self.req.environ)

    def _parse_vaco(self):
        return self.req.split_path(3, 4, rest_with_last=True)

    def handle_request(self):
        if hasattr(self, self.req.method) and self.is_valid_request:
            try:
                handler = getattr(self, self.req.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                return HTTPMethodNotAllowed(request=self.req)
            return handler()
        else:
            return self.req.get_response(self.app)
            # return HTTPMethodNotAllowed(request=self.req)

    def _augment_empty_request(self):
        """
        Auxiliary function that sets the content-length header and the body
        of the request in such cases that the user doesn't send the metadata
        file when he assign a function to an object.
        """
        if 'Content-Length' not in self.req.headers:
            self.req.headers['Content-Length'] = 0
            self.req.body = ''

    def _verify_access(self, cont, obj):
        """
        Verifies access to the specified object in swift
        :param cont: swift container name
        :param obj: swift object name
        :raise HTTPNotFound: if the object doesn't exists in swift
        :return response: Object response
        """
        path = os.path.join('/', self.api_version, self.account, cont, obj)
        response = verify_access(self, path)

        if not response.is_success:
            if response.status_int == 401:
                raise HTTPUnauthorized('Unauthorized to access to this '
                                       'resource: ' + cont + '/' + obj + '\n')
            else:
                raise HTTPNotFound('Object error: "' + cont + '/' +
                                   obj + '" doesn\'t exists in Swift.\n')
        else:
            return response

    def _augment_object_list(self, obj_list):
        """
        Checks the object list and creates those pseudo-folders that are not in
        the obj_list, but there are objects within them.
         :param obj_list: object list
        """
        for obj in obj_list:
            if '/' in obj:
                obj_split = obj.rsplit('/', 1)
                pseudo_folder = obj_split[0] + '/'
                if pseudo_folder not in obj_list:
                    path = os.path.join('/', self.api_version, self.account,
                                        self.container, pseudo_folder)
                    new_env = dict(self.req.environ)
                    auth_token = self.req.headers.get('X-Auth-Token')
                    sr = make_subrequest(new_env, 'PUT', path,
                                         headers={'X-Auth-Token': auth_token,
                                                  'Content-Length': 0},
                                         swift_source='function_middleware')
                    response = sr.get_response(self.app)
                    if response.is_success:
                        obj_list.append(pseudo_folder)
                    else:
                        raise ValueError("Error creating pseudo-folder")

    def _get_object_list(self, path):
        """
        Gets an object list of a specified path. The path may be '*', which
        means it will return all objects inside the container or a pseudo
        folder.
        :param path: pseudo-folder path (ended with *), or '*'
        :return: list of objects
        """
        obj_list = list()

        dest_path = os.path.join('/', self.api_version, self.account,
                                 self.container)
        new_env = dict(self.req.environ)
        auth_token = self.req.headers.get('X-Auth-Token')

        if path == '*':
            # All objects inside a container hierarchy
            obj_list.append('')
        else:
            # All objects inside a pseudo-folder hierarchy
            obj_split = self.obj.rsplit('/', 1)
            pseudo_folder = obj_split[0] + '/'
            new_env['QUERY_STRING'] = 'prefix='+pseudo_folder

        sr = make_subrequest(new_env, 'GET', dest_path,
                             headers={'X-Auth-Token': auth_token},
                             swift_source='function_middleware')
        response = sr.get_response(self.app)
        for obj in response.body.split('\n'):
            if obj != '':
                obj_list.append(obj)

        self._augment_object_list(obj_list)

        return obj_list

    def _set_function(self):
        """
        Process both function assignation over an object or a group of objects
        """
        self.req.method = 'PUT'
        obj_list = list()
        _, function = self.get_function_assignation_data()
        self._verify_access(self.function_container, function)

        if '*' in self.obj:
            obj_list = self._get_object_list(self.obj)
        else:
            obj_list.append(self.obj)

        specific_md = self.req.body

        if self.obj == '*':
            # Save function information into container metadata
            trigger, function = self.get_function_assignation_data()
            set_function_container(self, trigger, function)

        for obj in obj_list:
            self.req.body = specific_md
            response = self._verify_access(self.container, obj)
            new_path = os.path.join('/', self.api_version, self.account,
                                    self.container, obj)
            self.req.environ['PATH_INFO'] = new_path
            self._augment_empty_request()

            response = self.req.get_response(self.app)
            if not response.is_success:
                # TODO: send back an error message
                break

        return response

    def _unset_function(self):
        """
        Unset a specified function from the trigger of an object or a group
        of objects.
        """
        self.req.method = 'PUT'
        obj_list = list()

        if '*' in self.obj:
            obj_list = self._get_object_list(self.obj)
        else:
            obj_list.append(self.obj)

        if self.obj == '*':
            # Deletes the assignation information from the container
            trigger, function = self.get_function_deletion_data()
            unset_function_from_container(self, trigger, function)

        for obj in obj_list:
            response = self._verify_access(self.container, obj)
            new_path = os.path.join('/', self.api_version, self.account,
                                    self.container, obj)
            self.req.environ['PATH_INFO'] = new_path
            self._augment_empty_request()

            response = self.req.get_response(self.app)
            if not response.is_success:
                # TODO: send back an error message
                break

        return response

    def _get_parent_container_metadata(self):
        """
        Makes a HEAD to the parent pseudo-folder or container
        in order to get the function list to execute.
        :return: metadata dictionary
        """
        obj_split = self.obj.rsplit('/', 1)

        if len(obj_split) > 1:
            # object parent is pseudo-foldder
            psudo_folder = obj_split[0] + '/'
            dest_path = os.path.join('/', self.api_version, self.account,
                                     self.container, psudo_folder)
        else:
            # object parent is container
            dest_path = os.path.join('/', self.api_version, self.account,
                                     self.container)

        # We first try to get the function execution list from Memcache
        function_metadata = self.memcache.get("function_md"+dest_path)
        if function_metadata:
            return function_metadata

        # If the function execution list is not in Memcache, it gets it
        # from Swift
        new_env = dict(self.req.environ)
        auth_token = self.req.headers.get('X-Auth-Token')
        sub_req = make_subrequest(new_env, 'HEAD', dest_path,
                                  headers={'X-Auth-Token': auth_token},
                                  swift_source='function_middleware')
        response = sub_req.get_response(self.app)

        function_metadata = dict()
        if response.is_success:
            for key in response.headers:
                if 'Sysmeta-Function' in key:
                    k = key.replace('Container', 'Object')
                    if 'Function-List' in k:
                        function = eval(response.headers[key])
                        function_metadata[k] = function
                    else:
                        function_metadata[k] = response.headers[key]

        if function_metadata:
            self.memcache.set("function_md"+dest_path, function_metadata)
        else:
            function_metadata = None

        return function_metadata

    def _handle_request_trough_middlebox(self, response):
        node = 'compute1'
        port = '8080'
        url = os.path.join('http://', node+':'+port, self.api_version, self.account)
        parsed, conn = http_connection(url)
        path = '%s/%s/%s' % (parsed.path, quote(self.container), quote(self.obj))
        self.req.headers['Middlebox'] = response.headers.pop('Middlebox')
        conn.request(self.method, path, '', self.req.headers)
        resp = conn.getresponse()
        resp_headers = {}
        for header, value in resp.getheaders():
            resp_headers[header] = value
        response.headers.update(resp_headers)
        response.app_iter = DataIter(resp, 5)

        return response

    def _get_response_from_middlebox(self):
        self.logger.info('I am the Middlebox')
        response_timeout = 5
        path = '/%s/%s/%s' % (self.account, self.container, self.obj)
        data = eval(self.req.headers['Middlebox'])
        self.req.headers['X-Backend-Storage-Policy-Index'] = data['policy']
        storage_node = data['storage_node']
        storage_port = data['storage_port']
        device = data['device']
        part = data['part']
        with Timeout(response_timeout):
            conn = http_connect(storage_node, storage_port, device, part,
                                'GET', path, headers=gen_headers(self.req.headers))
        with Timeout(response_timeout):
            resp = conn.getresponse()
        resp_headers = {}
        for header, value in resp.getheaders():
            resp_headers[header] = value.replace('"', '')
        response = Response(app_iter=DataIter(resp, 10),
                            headers=resp_headers,
                            request=self.req)

        return response

    @public
    def GET(self):
        """
        GET handler on Proxy
        """
        if self.is_middlebox_request:
            # I am a middlewbox
            response = self._get_response_from_middlebox()
        else:
            response = self.req.get_response(self.app)
        # self.req = self.apply_function_on_pre_get()

        response = self.apply_function_on_post_get(response)

        if 'Middlebox' in response.headers:
            # The Object Storage node does not have enough resources, it will
            # get the object through the Middlebox.
            response = self._handle_request_trough_middlebox(response)

        if 'Content-Length' not in response.headers and \
           'Transfer-Encoding' in response.headers:
            response.headers.pop('Transfer-Encoding')

        return response

    @public
    def PUT(self):
        """
        PUT handler on Proxy
        """
        if self.is_function_set:
            response = self._set_function()
        elif self.is_function_unset:
            response = self._unset_function()
        else:
            # Normal PUT. Onput Functions are applied here
            function_metadata = self._get_parent_container_metadata()
            self.req.headers.update(function_metadata)
            f_list = get_function_list_object(function_metadata, self.method)
            if f_list:
                self.logger.info('There are functions' +
                                 ' to execute: ' + str(f_list))
                self._setup_docker_gateway()
                f_data = self.docker_gateway.execute_function(f_list)
                response = self._process_function_data_req(f_data)
            else:
                response = self.req.get_response(self.app)

        return response

    @public
    def POST(self):
        """
        POST handler on Proxy
        """
        if self.is_function_set:
            response = self._set_function()
        elif self.is_function_unset:
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
        if self.conf['metadata_visibility']:
            for key in response.headers.keys():
                k = key.replace('Container', 'Object')
                if 'Sysmeta-Function' in k:
                    new_key = k.replace('X-Object-Sysmeta-', '')
                    response.headers[new_key] = response.headers[key]

        return response
