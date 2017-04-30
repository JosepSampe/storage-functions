from blackeagle.common.utils import make_swift_request, \
    set_object_metadata, get_object_metadata
from blackeagle.gateways.docker.runtime import RuntimeSandbox, \
    FunctionInvocationProtocol
from shutil import copy2
import os
import ctypes
libc = ctypes.cdll.LoadLibrary('libc.so.6')


MC_MAIN_HEADER = "X-Object-Meta-Function-Main"
MC_DEP_HEADER = "X-Object-Meta-Function-Library-Dependency"


class DockerGateway():

    def __init__(self, request, response, conf, logger, account):
        self.req = request
        self.response = response
        self.conf = conf
        self.logger = logger
        self.account = account
        self.method = self.req.method.lower()
        self.scope = account[5:18]
        self.function_timeout = conf["function_timeout"]
        self.function_container = conf["function_container"]
        self.dep_container = conf["function_dependency"]
        self.execution_server = conf["execution_server"]
        self.workers = int(conf["workers"])

        self.fast = True
        # self.fast = True

        # Paths
        self.scope_dir = os.path.join(conf["main_dir"], self.scope)
        self.logger_path = os.path.join(self.scope_dir, conf["log_dir"])
        self.pipes_path = os.path.join(self.scope_dir, conf["pipes_dir"])
        thread = libc.syscall(186)
        tid = str(thread % self.workers)
        self.function_pipe_path = os.path.join(self.pipes_path,
                                               conf["function_pipe"]+"_"+tid)

    def execute_function(self, function_list):
        """
        Exeutes the function list.
         1. Starts the docker container (sandbox).
         3. Gets the functions metadata.
         4. Executes the function list.

        :param function_list: function list
        :returns: response from the function
        """
        if not self.fast:
            RuntimeSandbox(self.logger, self.conf, self.account).start()

        f_metadata = self._get_function_metadata(function_list)
        object_headers = self._get_object_headers()
        object_stream = self._get_object_stream()

        protocol = FunctionInvocationProtocol(object_stream,
                                              self.function_pipe_path,
                                              self.logger_path,
                                              dict(self.req.headers),
                                              object_headers,
                                              function_list,
                                              f_metadata,
                                              self.function_timeout,
                                              self.logger)
        return protocol.communicate()

    def _get_object_stream(self):
        if self.method == 'get':
            return self.response.app_iter
        if self.method == 'put':
            return self.req.environ['wsgi.input']

    def _get_object_headers(self):
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

    def _update_local_cache_from_swift(self, swift_container, obj_name):
        """
        Updates the local cache of functions and dependencies

        :param swift_container: container name
        :param obj_name: Name of the function or dependency
        """
        cache_target_path = os.path.join(self.scope_dir,
                                         self.conf["cache_dir"],
                                         swift_container)
        cache_target_obj = os.path.join(cache_target_path, obj_name)

        if not os.path.exists(cache_target_path):
            os.makedirs(cache_target_path, 0o777)

        resp = make_swift_request("GET", self.account,
                                  swift_container, obj_name)

        with open(cache_target_obj, 'w') as fn:
            fn.write(resp.body)

        set_object_metadata(cache_target_obj, resp.headers)

    def _is_avialable_in_cache(self, swift_container, obj_name):
        """
        checks whether the function or the dependency is in cache. If not,
        brings it from swift.

        :param swift_container: container name (function or dependency)
        :param object_name: Name of the function or dependency
        :returns : whether the object is available in cache
        """
        cached_target_obj = os.path.join(self.scope_dir,
                                         self.conf["cache_dir"],
                                         swift_container, obj_name)
        self.logger.info('Checking in cache: ' + swift_container +
                         '/' + obj_name)

        if not os.path.isfile(cached_target_obj):
            # If the objects is not in cache, brings it from Swift.
            # raise NameError(swift_container+'/'+object_name + ' not found in cache.')
            self.logger.info(swift_container + '/' + obj_name + ' not found in cache.')
            self._update_local_cache_from_swift(swift_container, obj_name)
        else:
            if not self.fast:
                self._update_local_cache_from_swift(swift_container, obj_name)  # DELETE! (Only for test purposes)
            self.logger.info(swift_container + '/' + obj_name + ' in cache.')

        return True

    def _update_from_cache(self, function_main, swift_container, obj_name):
        """
        Updates the tenant function folder from the local cache.

        :param function_main: main class of the function
        :param swift_container: container name (function or dependency)
        :param obj_name: Name of the function or dependency
        """
        # if enter to this method means that the objects exist in cache
        cached_target_obj = os.path.join(self.scope_dir,
                                         self.conf["cache_dir"],
                                         swift_container, obj_name)
        docker_target_dir = os.path.join(self.scope_dir,
                                         self.conf["java_runtime_dir"],
                                         function_main)
        docker_target_obj = os.path.join(docker_target_dir, obj_name)
        update_from_cache = False

        if not os.path.exists(docker_target_dir):
            os.makedirs(docker_target_dir, 0o777)
            update_from_cache = True
        elif not os.path.isfile(docker_target_obj):
            update_from_cache = True
        else:
            cached_obj_metadata = get_object_metadata(cached_target_obj)
            docker_obj_metadata = get_object_metadata(docker_target_obj)

            cached_obj_tstamp = float(cached_obj_metadata['X-Timestamp'])
            docker_obj_tstamp = float(docker_obj_metadata['X-Timestamp'])

            if cached_obj_tstamp > docker_obj_tstamp:
                update_from_cache = True

        if update_from_cache:
            self.logger.info('Going to update from cache: ' +
                             swift_container + '/' + obj_name)
            copy2(cached_target_obj, docker_target_obj)
            metadata = get_object_metadata(cached_target_obj)
            set_object_metadata(docker_target_obj, metadata)

    def _get_metadata(self, swift_container, obj_name):
        """
        Retrieves the swift metadata from the local cached object.

        :param swift_container: container name (function or dependency)
        :param obj_name: object name
        :returns: swift metadata dictionary
        """
        cached_target_obj = os.path.join(self.scope_dir,
                                         self.conf["cache_dir"],
                                         swift_container, obj_name)
        metadata = get_object_metadata(cached_target_obj)

        return metadata

    def _get_function_metadata(self, function_list):
        """
        Retrieves the function metadata from the list of functions.

        :param function_list: function list
        :returns: metadata dictionary
        """
        f_metadata = dict()

        for f_name in function_list:
            if self._is_avialable_in_cache(self.function_container, f_name):
                f_metadata[f_name] = self._get_metadata(self.function_container,
                                                        f_name)
                f_main = f_metadata[f_name][MC_MAIN_HEADER]
                self._update_from_cache(f_main, self.function_container, f_name)

                if f_metadata[f_name][MC_DEP_HEADER]:
                    dep_list = f_metadata[f_name][MC_DEP_HEADER].split(",")
                    for dep_name in dep_list:
                        if self._is_avialable_in_cache(self.dep_container,
                                                       dep_name):
                            self._update_from_cache(f_main,
                                                    self.dep_container,
                                                    dep_name)

        return f_metadata
