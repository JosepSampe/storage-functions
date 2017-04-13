from swift.common.swob import HTTPInternalServerError, HTTPException, wsgify
from swift.common.utils import get_logger
from ConfigParser import RawConfigParser
from blackeagle.handlers import ProxyHandler
from blackeagle.handlers import ObjectHandler
from blackeagle.handlers.base import NotFunctionRequest


class VertigoHandlerMiddleware(object):

    def __init__(self, app, conf, vertigo_conf):
        self.app = app
        self.exec_server = vertigo_conf.get('execution_server')
        self.logger = get_logger(conf, name=self.exec_server+"-server Blackeagle",
                                 log_route='function_handler')
        self.vertigo_conf = vertigo_conf
        self.handler_class = self._get_handler(self.exec_server)

    def _get_handler(self, exec_server):
        """
        Generate Handler class based on execution_server parameter

        :param exec_server: Where this storlet_middleware is running.
                            This should value shoud be 'proxy' or 'object'
        :raise ValueError: If exec_server is invalid
        """
        if exec_server == 'proxy' or exec_server == 'middlebox':
            return ProxyHandler
        elif exec_server == 'object':
            return ObjectHandler
        else:
            raise ValueError('configuration error: execution_server must be either proxy'
                             ', object or middlebox but is %s' % exec_server)

    @wsgify
    def __call__(self, req):
        try:
            handler = self.handler_class(req, self.vertigo_conf, self.app, self.logger)
            self.logger.debug('Function handler %s call in %s/%s/%s' %
                              (req.method, handler.account, handler.container, handler.obj))
        except HTTPException:
            raise
        except NotFunctionRequest:
            return req.get_response(self.app)

        try:
            return handler.handle_request()

        except HTTPException:
            self.logger.exception('Middleware execution failed')
            raise
        except Exception:
            self.logger.exception('Middleware execution failed')
            raise HTTPInternalServerError(body='Middleware execution failed')


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""

    conf = global_conf.copy()
    conf.update(local_conf)

    vertigo_conf = dict()
    vertigo_conf['devices'] = conf.get('devices', '/srv/node')
    vertigo_conf['execution_server'] = conf.get('execution_server')
    vertigo_conf['function_timeout'] = conf.get('function_timeout', 8)
    vertigo_conf['function_pipe'] = conf.get('f_pipe', 'function_pipe')
    vertigo_conf['docker_img_prefix'] = conf.get('docker_img_prefix', 'blackeagle')
    vertigo_conf['metadata_visibility'] = conf.get('metadata_visibility', True)
    vertigo_conf['main_dir'] = conf.get('function_dir', '/home/docker_device/blackeagle/scopes')
    vertigo_conf['java_runtime_dir'] = conf.get('cache_dir', 'runtime/java')
    vertigo_conf['python_runtime_dir'] = conf.get('cache_dir', 'runtime/python')
    vertigo_conf['cache_dir'] = conf.get('cache_dir', 'cache')
    vertigo_conf['log_dir'] = conf.get('cache_dir', 'logs')
    vertigo_conf['pipes_dir'] = conf.get('cache_dir', 'pipes')
    vertigo_conf['docker_repo'] = conf.get('docker_repo', '192.168.2.1:5001')
    vertigo_conf['function_container'] = conf.get('f_container', 'function')
    vertigo_conf['function_dependency'] = conf.get('f_dependency', 'dependency')
    vertigo_conf['workers'] = conf.get('workers', 1)
    vertigo_conf['use_storlets'] = conf.get('use_storlets', False)

    if vertigo_conf['use_storlets']:
        ''' Load storlet parameters '''
        configParser = RawConfigParser()
        configParser.read(conf.get('__file__'))
        storlet_parameters = configParser.items('filter:storlet_handler')
        for key, val in storlet_parameters:
            vertigo_conf[key] = val

        configParser = RawConfigParser()
        configParser.read(vertigo_conf['storlet_gateway_conf'])
        additional_items = configParser.items("DEFAULT")
        for key, val in additional_items:
            vertigo_conf[key] = val

        """ Load Storlets Gateway class """
        module_name = vertigo_conf['storlet_gateway_module']
        mo = module_name[:module_name.rfind(':')]
        cl = module_name[module_name.rfind(':') + 1:]
        module = __import__(mo, fromlist=[cl])
        the_class = getattr(module, cl)
        vertigo_conf["storlets_gateway_module"] = the_class

    def swift_vertigo(app):
        return VertigoHandlerMiddleware(app, global_conf, vertigo_conf)

    return swift_vertigo
