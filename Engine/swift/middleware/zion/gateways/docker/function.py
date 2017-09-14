from swift.common.wsgi import make_subrequest
from zion.common.utils import set_object_metadata, get_object_metadata
import tarfile
import os

TIMEOUT_HEADER = "X-Object-Meta-Function-Timeout"
MEMORY_HEADER = "X-Object-Meta-Function-Memory"
MAIN_HEADER = "X-Object-Meta-Function-Main"


class Function(object):
    """
    Function main class.
    """

    def __init__(self, be, scope, function_obj_name):
        self.be = be
        self.req = be.req
        self.conf = be.conf
        self.scope = scope
        self.function_obj_name = function_obj_name
        self.logger = be.logger
        self.function_name = function_obj_name.replace('.tar.gz', '')
        self.functions_container = self.conf['functions_container']
        # Dirs
        self.main_dir = self.conf["main_dir"]
        self.functions_dir = self.conf["functions_dir"]
        self.cache_dir = self.conf["cache_dir"]
        self.log_dir = self.conf["log_dir"]
        self.bin_dir = self.conf["bin_dir"]

        self._preparate_dirs()
        self._load_function()

    def _preparate_dirs(self):
        """
        Makes the required directories for managing the function.
        """
        self.logger.info('Preparing function directories')
        functions_path = os.path.join(self.main_dir, self.functions_dir)
        scope_path = os.path.join(functions_path, self.scope)
        self.cache_path = os.path.join(scope_path, self.cache_dir)
        self.log_path = os.path.join(scope_path, self.log_dir)
        self.bin_path = os.path.join(scope_path, self.bin_dir)

        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        if not os.path.exists(self.log_path):
            os.makedirs(self.log_path)

    def _load_function(self):
        """
        Loads the function.
        """
        self.logger.info('Loading function: '+self.function_obj_name)

        self.cached_function_obj = os.path.join(self.cache_path, self.function_obj_name)
        self.function_bin_path = os.path.join(self.bin_path, self.function_name)

        if not self._is_function_in_cache():
            self._update_local_cache_from_swift()
            self._extract_function()

        self._load_function_execution_information()

    def _is_function_in_cache(self):
        """
        Checks whether the function is in cache.

        :returns : whether the object is available in cache.
        """
        in_cache = False
        if os.path.isfile(self.cached_function_obj):
            self.logger.info(self.function_obj_name + ' found in cache.')
            in_cache = True
        else:
            self.logger.info(self.function_obj_name + ' not found in cache.')
            in_cache = False

        return in_cache

    def _update_local_cache_from_swift(self):
        """
        Updates the local cache of functions.
        """
        f_container = self.functions_container
        new_env = dict(self.be.req.environ)
        swift_path = os.path.join('/', self.be.api_version, self.be.account,
                                  f_container, self.function_obj_name)
        sub_req = make_subrequest(new_env, 'GET', swift_path,
                                  swift_source='function_middleware')
        resp = sub_req.get_response(self.be.app)

        with open(self.cached_function_obj, 'w') as fn:
            fn.write(resp.body)

        self.logger.info('Local cache updated: '+self.cached_function_obj)

        self.function_metadata = resp.headers
        set_object_metadata(self.cached_function_obj, resp.headers)

    def _extract_function(self):
        """
        Untars the function to the bin directory.
        """
        tar = tarfile.open(self.cached_function_obj, "r:gz")
        tar.extractall(path=self.function_bin_path)
        tar.close()

    def _load_function_execution_information(self):
        """
        Loads the memory needed and the timeout of the function.
        """
        function_metadata = get_object_metadata(self.cached_function_obj)

        if MEMORY_HEADER not in function_metadata or TIMEOUT_HEADER not in \
           function_metadata or MAIN_HEADER not in function_metadata:
            raise ValueError("Error Getting Function memory and timeout values")
        else:
            self.memory = int(function_metadata[MEMORY_HEADER])
            self.timeout = int(function_metadata[TIMEOUT_HEADER])
            self.main_class = function_metadata[MAIN_HEADER]

    def open_log(self):
        """
        Opens the log file where the function will log.
        """
        f_log_path = os.path.join(self.log_path, self.function_name)
        if not os.path.exists(f_log_path):
            os.makedirs(f_log_path)
        f_log_file = os.path.join(f_log_path, self.function_name+'.log')
        self.logger_file = open(f_log_file, 'a')

    def get_timeout(self):
        return self.timeout

    def get_main_class(self):
        return self.main_class

    def get_memory(self):
        return self.memory

    def get_logfd(self):
        return self.logger_file.fileno()

    def get_name(self):
        return self.function_name

    def get_obj_name(self):
        return self.function_obj_name

    def get_bin_path(self):
        return self.function_bin_path

    def close_log(self):
        """
        Closes the log file.
        """
        self.logger_file.close()
