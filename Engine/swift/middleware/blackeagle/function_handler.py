from swift.common.swob import wsgify
from swift.common.utils import get_logger
from blackeagle.handlers import ProxyHandler
from blackeagle.handlers import ObjectHandler
from blackeagle.handlers import ComputeHandler
from blackeagle.handlers.base import NotFunctionRequest


class FunctionHandlerMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.exec_server = conf.get('execution_server')
        self.logger = get_logger(conf, name=self.exec_server +
                                 "-server Blackeagle",
                                 log_route='function_handler')
        self.conf = conf
        self.handler_class = self._get_handler(self.exec_server)

    def _get_handler(self, exec_server):
        """
        Generate Handler class based on execution_server parameter

        :param exec_server: Where this storlet_middleware is running.
                            This should value should be 'proxy', 'object'
                            or 'compute'
        :raise ValueError: If exec_server is invalid
        """
        if exec_server == 'proxy':
            return ProxyHandler
        elif exec_server == 'object':
            return ObjectHandler
        elif exec_server == 'compute':
            return ComputeHandler
        else:
            raise ValueError('configuration error: execution_server must be '
                             'either proxy, object or compute but is %s' % exec_server)

    @wsgify
    def __call__(self, req):
        try:
            handler = self.handler_class(req, self.conf, self.app, self.logger)
            self.logger.debug('%s call in %s' % (req.method, req.path))

            return handler.handle_request()

        except NotFunctionRequest:
            self.logger.debug('No Blackeagle Request, bypassing middleware')
            return req.get_response(self.app)
        except Exception as exception:
            raise exception


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    conf = global_conf.copy()
    conf.update(local_conf)

    conf['devices'] = conf.get('devices', '/srv/node')
    conf['execution_server'] = conf.get('execution_server')
    conf['function_timeout'] = conf.get('function_timeout', 50)
    conf['function_pipe'] = conf.get('function_pipe', 'function_pipe')
    conf['docker_img_prefix'] = conf.get('docker_img_prefix', 'blackeagle')
    conf['function_visibility'] = conf.get('function_visibility', True)
    conf['main_dir'] = conf.get('main_dir', '/home/docker_device/blackeagle/scopes')
    conf['java_runtime_dir'] = conf.get('java_runtime_dir', 'runtime/java')

    conf['cache_dir'] = conf.get('cache_dir', 'cache')
    conf['log_dir'] = conf.get('log_dir', 'logs')
    conf['pipes_dir'] = conf.get('pipes_dir', 'pipes')

    conf['docker_repo'] = conf.get('docker_repo', '192.168.2.1:5001')
    conf['functions_container'] = conf.get('functions_container', 'functions')
    conf['workers'] = conf.get('workers', 1)

    conf['redis_host'] = conf.get('redis_host', 'localhost')
    conf['redis_port'] = int(conf.get('redis_port', 6379))
    conf['redis_db'] = int(conf.get('redis_db', 10))

    conf['default_function_timeout'] = int(conf.get('default_function_timeout', 10))
    conf['default_function_memory'] = int(conf.get('default_function_memory', 1024))
    conf['max_function_memory'] = int(conf.get('max_function_memory', 1024))

    conf['compute_nodes'] = conf.get('compute_nodes', '192.168.2.31:8080')

    def swift_functions(app):
        return FunctionHandlerMiddleware(app, conf)

    return swift_functions
