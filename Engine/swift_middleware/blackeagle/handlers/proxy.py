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
        self.request.headers['function-enabled'] = True
        self.memcache = cache_from_env(self.request.environ)

    def _parse_vaco(self):
        return self.request.split_path(3, 4, rest_with_last=True)

    def handle_request(self):
        if hasattr(self, self.request.method) and self.is_valid_request:
            try:
                handler = getattr(self, self.request.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                return HTTPMethodNotAllowed(request=self.request)
            return handler()
        else:
            return self.request.get_response(self.app)
            # return HTTPMethodNotAllowed(request=self.request)

    def _augment_empty_request(self):
        """
        Auxiliary function that sets the content-length header and the body
        of the request in such cases that the user doesn't send the metadata
        file when he assign a function to an object.
        """
        if 'Content-Length' not in self.request.headers:
            self.request.headers['Content-Length'] = 0
            self.request.body = ''

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
                    new_env = dict(self.request.environ)
                    auth_token = self.request.headers.get('X-Auth-Token')
                    sub_req = make_subrequest(new_env, 'PUT', path,
                                              headers={'X-Auth-Token': auth_token,
                                                       'Content-Length': 0},
                                              swift_source='function_middleware')
                    response = sub_req.get_response(self.app)
                    if response.is_success:
                        obj_list.append(pseudo_folder)
                    else:
                        raise ValueError("Error creating pseudo-folder")

    def _get_object_list(self, path):
        """
        Gets an object list of a specified path. The path may be '*', that means
        it returns all objects inside the container or a pseudo-folder.
        :param path: pseudo-folder path (ended with *), or '*'
        :return: list of objects
        """
        obj_list = list()

        dest_path = os.path.join('/', self.api_version, self.account, self.container)
        new_env = dict(self.request.environ)
        auth_token = self.request.headers.get('X-Auth-Token')

        if path == '*':
            # All objects inside a container hierarchy
            obj_list.append('')
        else:
            # All objects inside a pseudo-folder hierarchy
            obj_split = self.obj.rsplit('/', 1)
            pseudo_folder = obj_split[0] + '/'
            new_env['QUERY_STRING'] = 'prefix='+pseudo_folder

        sub_req = make_subrequest(new_env, 'GET', dest_path,
                                  headers={'X-Auth-Token': auth_token},
                                  swift_source='function_middleware')
        response = sub_req.get_response(self.app)
        for obj in response.body.split('\n'):
            if obj != '':
                obj_list.append(obj)

        self._augment_object_list(obj_list)

        return obj_list

    def _get_parent_vertigo_metadata(self):
        """
        Makes a HEAD to the parent pseudo-folder or container (7ms overhead)
        in order to get the function assignated metadata.
        :return: vertigo metadata dictionary
        """
        obj_split = self.obj.rsplit('/', 1)

        if len(obj_split) > 1:
            # object parent is pseudo-foldder
            psudo_folder = obj_split[0] + '/'
            f_key = 'X-Object-Sysmeta-Function-List'
            dest_path = os.path.join('/', self.api_version, self.account,
                                     self.container, psudo_folder)
        else:
            # object parent is container
            f_key = 'X-Container-Sysmeta-Function-List'
            dest_path = os.path.join('/', self.api_version, self.account,
                                     self.container)

        # We first try to get the function execution list from the memcache
        function_metadata = self.memcache.get("function_"+dest_path)

        if function_metadata:
            for key in function_metadata.keys():
                if key.replace('Container', 'Object').startswith('X-Object-Sysmeta-Function-'):
                    if key == f_key:
                        function = eval(function_metadata.pop(key))
                        function_metadata[key.replace('Container', 'Object')] = function
                    else:
                        function_metadata[key.replace('Container', 'Object')] = function_metadata.pop(key)
            return function_metadata

        # If the function execution list is not in memcache, we get it from Swift
        new_env = dict(self.request.environ)
        auth_token = self.request.headers.get('X-Auth-Token')
        sub_req = make_subrequest(new_env, 'HEAD', dest_path,
                                  headers={'X-Auth-Token': auth_token},
                                  swift_source='function_middleware')
        response = sub_req.get_response(self.app)

        function_metadata = dict()
        if response.is_success:
            for key in response.headers:
                if key.replace('Container', 'Object').startswith('X-Object-Sysmeta-Function-'):
                    # if key.replace('Container', 'Object').startswith('X-Object-Sysmeta-Function-Onput'):
                    #    continue
                    if key == f_key:
                        function = eval(response.headers[key])
                        function_metadata[key.replace('Container', 'Object')] = function
                    else:
                        function_metadata[key.replace('Container', 'Object')] = response.headers[key]

        if function_metadata:
            self.memcache.set("function_"+dest_path, function_metadata)
        else:
            vertigo_metadata = None

        return vertigo_metadata

    def _set_function(self):
        """
        Process both function assignation over an object or a group of objects
        """
        self.request.method = 'PUT'
        obj_list = list()
        _, function = self.get_function_assignation_data()
        self._verify_access(self.function_container, function)

        if '*' in self.obj:
            obj_list = self._get_object_list(self.obj)
        else:
            obj_list.append(self.obj)

        specific_md = self.request.body

        if self.obj == '*':
            # Save function information into container metadata
            trigger, function = self.get_function_assignation_data()
            set_function_container(self, trigger, function)

        for obj in obj_list:
            self.request.body = specific_md
            response = self._verify_access(self.container, obj)
            new_path = os.path.join('/', self.api_version, self.account, self.container, obj)
            self.request.environ['PATH_INFO'] = new_path
            self._augment_empty_request()

            response = self.request.get_response(self.app)
            if not response.is_success():
                # TODO: send back an error message
                break

        return response

    def _unset_function(self):
        """
        Unset a specified function from the trigger of an object or a group of
        objects.
        """
        self.request.method = 'PUT'
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
            new_path = os.path.join('/', self.api_version, self.account, self.container, obj)
            self.request.environ['PATH_INFO'] = new_path
            self._augment_empty_request()

            response = self.request.get_response(self.app)
            if not response.is_success():
                # TODO: send back an error message
                break

        return response

    def _handle_request_trough_middlebox(self, response):
        node = 'compute1'
        port = '8080'
        url = os.path.join('http://', node+':'+port, self.api_version, self.account)
        parsed, conn = http_connection(url)
        path = '%s/%s/%s' % (parsed.path, quote(self.container), quote(self.obj))
        self.request.headers['Middlebox'] = response.headers.pop('Middlebox')
        conn.request(self.method, path, '', self.request.headers)
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
        data = eval(self.request.headers['Middlebox'])
        self.request.headers['X-Backend-Storage-Policy-Index'] = data['policy']
        storage_node = data['storage_node']
        storage_port = data['storage_port']
        device = data['device']
        part = data['part']
        with Timeout(response_timeout):
            conn = http_connect(storage_node, storage_port, device, part,
                                'GET', path, headers=gen_headers(self.request.headers))
        with Timeout(response_timeout):
            resp = conn.getresponse()
        resp_headers = {}
        for header, value in resp.getheaders():
            resp_headers[header] = value.replace('"', '')
        response = Response(app_iter=DataIter(resp, 10), headers=resp_headers, request=self.request)

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
            response = self.request.get_response(self.app)
        # self.request = self.apply_function_on_pre_get()

        response = self.apply_function_on_post_get(response)

        if 'Middlebox' in response.headers:
            # The Object Storage node does not have enough resources, we need
            # to get the object through the Middlebox.
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
            # Normal PUT. Onput Functions are applied here, if any.
            function_metadata = self._get_parent_vertigo_metadata()
            self.request.headers.update(function_metadata)
            f_list = get_function_list_object(function_metadata, self.method)
            if f_list:
                self.logger.info('There are functions' +
                                 ' to execute: ' + str(f_list))
                self._setup_docker_gateway()
                f_data = self.docker_gateway.execute_function(f_list)
                response = self._process_function_data_req(f_data)
            else:
                response = self.request.get_response(self.app)

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
            response = self.request.get_response(self.app)

        return response

    @public
    def HEAD(self):
        """
        HEAD handler on Proxy
        """
        response = self.request.get_response(self.app)
        if self.conf['metadata_visibility']:
            for key in response.headers.keys():
                if key.replace('Container', 'Object').startswith('X-Object-Sysmeta-Function-'):
                    new_key = key.replace('Container', 'Object').replace('X-Object-Sysmeta-', '')
                    response.headers[new_key] = response.headers[key]

            if 'Function' in response.headers:
                function_dict = eval(response.headers['Function'])
                for trigger in function_dict.keys():
                    if not function_dict[trigger]:
                        del function_dict[trigger]
                response.headers['Function'] = function_dict

        return response
