import os

TIMEOUT_HEADER = "X-Object-Meta-Function-Timeout"
MEMORY_HEADER = "X-Object-Meta-Function-Memory"


class Function(object):
    """
    Function main class.
    """

    def __init__(self, conf, scope, function):
        self.conf = conf
        self.scope = scope
        self.function_name = function.replace('.tar.gz', '')
        self.function_object = function
        
        # Paths
        self.scope_dir = os.path.join(conf["main_dir"], self.scope)
        self.logger_path = os.path.join(self.scope_dir, conf["log_dir"])
        self.workers_dir = conf["workers_dir"]

        self.main = ""
        self.log_path = os.path.join(self.logger_path, main)
        self.log_name = function_name.replace('tar.gz', 'log')
        self.full_log_path = os.path.join(self.log_path, self.log_name)
        self.function_name = function_name.replace('.tar.gz', '')

        if not os.path.exists(self.log_path):
            os.makedirs(self.log_path)

    def get_name(self):
        return self.function_name

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

    def _get_function_metadata(self, function_data):
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

    def open(self):
        self.logger_file = open(self.full_log_path, 'a')

    def get_logfd(self):
        return self.logger_file.fileno()

    def get_name(self):
        return self.function

    def get_dependencies(self):
        return self.dependencies

    def get_main(self):
        return self.main_class

    def get_size(self):
        statinfo = os.stat(self.full_path)
        return statinfo.st_size

    def close(self):
        self.logger_file.close()
