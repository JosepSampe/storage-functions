from swift.common.swob import Request, HTTPUnauthorized
from swift.common.utils import config_true_value
import json

try:
    from storlet_middleware.handlers.base import SwiftFileManager
except ImportError:
    pass

from blackeagle.common.utils import make_swift_request


class StorletGateway():

    def __init__(self, conf, logger, app, api_v, account, method):
        self.conf = conf
        self.logger = logger
        self.app = app
        self.api_version = api_v
        self.account = account
        self.scope = self.account[5:18]
        self.method = method
        self.server = self.conf['execution_server']

        self.storlet_name = None
        self.storlet_metadata = None
        self.gateway = None
        self.gateway_class = self.conf['storlets_gateway_module']
        self.sreq_class = self.gateway_class.request_class

        self.storlet_container = conf.get('storlet_container')
        self.storlet_dependency = conf.get('storlet_dependency')
        self.log_container = conf.get('storlet_logcontainer')
        self.client_conf_file = '/etc/swift/storlet-proxy-server.conf'

    def _setup_gateway(self):
        """
        Setup Storlet gateway instance
        """
        self.gateway = self.gateway_class(self.conf, self.logger, self.scope)

    def _get_storlet_data(self, storlet_data):
        storlet = storlet_data["storlet"]
        parameters = storlet_data["params"]
        server = storlet_data["server"]

        return storlet, parameters, server

    def _get_storlet_invocation_options(self, req):
        options = dict()

        filtered_key = ['X-Storlet-Range', 'X-Storlet-Generate-Log']

        for key in req.headers:
            prefix = 'X-Storlet-'
            if key.startswith(prefix) and key not in filtered_key:
                new_key = 'storlet_' + \
                    key[len(prefix):].lower().replace('-', '_')
                options[new_key] = req.headers.get(key)

        scope = self.account
        if scope.rfind(':') > 0:
            scope = scope[:scope.rfind(':')]

        options['scope'] = self.scope

        options['generate_log'] = \
            config_true_value(req.headers.get('X-Storlet-Generate-Log'))

        options['file_manager'] = \
            SwiftFileManager(self.account, self.storlet_container,
                             self.storlet_dependency, self.log_container,
                             self.client_conf_file, self.logger)

        return options

    def _augment_storlet_request(self, req):
        """
        Add to request the storlet parameters to be used in case the request
        is forwarded to the data node (GET case)
        :param params: paramegers to be augmented to request
        """
        for key, val in self.storlet_metadata.iteritems():
            req.headers['X-Storlet-' + key] = val

    def _parse_storlet_params(self, headers):
        """
        Parse storlet parameters from storlet/dependency object metadata
        :returns: dict of storlet parameters
        """
        params = dict()
        for key in headers:
            if key.startswith('X-Object-Meta-Storlet'):
                params[key[len('X-Object-Meta-Storlet-'):]] = headers[key]
        return params

    def _verify_access_to_storlet(self, storlet):
        """
        Verify access to the storlet object
        :params storlet: storlet name
        :return: is accessible
        :raises HTTPUnauthorized: If it fails to verify access
        """
        spath = '/'.join(['', self.api_version, self.account,
                          self.storlet_container, storlet])
        self.logger.debug('Verifying access to %s' % spath)

        resp = make_swift_request("HEAD", self.account,
                                  self.storlet_container,
                                  storlet)

        if not resp.is_success:
            return False

        self.storlet_name = storlet
        self.storlet_metadata = self._parse_storlet_params(resp.headers)
        for key in ['Content-Length', 'X-Timestamp']:
            self.storlet_metadata[key] = resp.headers[key]

        return True

    def _build_storlet_request(self, req_resp, params, data_iter):
        storlet_id = self.storlet_name

        new_env = dict(req_resp.environ)
        req = Request.blank(new_env['PATH_INFO'], new_env)

        req.environ['QUERY_STRING'] = params
        req.headers['X-Run-Storlet'] = self.storlet_name
        self._augment_storlet_request(req)
        options = self._get_storlet_invocation_options(req)

        if hasattr(data_iter, '_fp'):
            sreq = self.sreq_class(storlet_id, req.params, dict(),
                                   data_fd=data_iter._fp.fileno(),
                                   options=options)
        else:
            sreq = self.sreq_class(storlet_id, req.params, dict(),
                                   data_iter, options=options)

        return sreq

    def _call_gateway(self, req_resp, params, data_iter):
        sreq = self._build_storlet_request(req_resp, params, data_iter)
        sresp = self.gateway.invocation_flow(sreq)

        return sresp.data_iter

    def run(self, req_resp, storlet_list, data_iter):
        on_other_server = {}

        # Execute multiple Storlets, PIPELINE, if any.
        for key in sorted(storlet_list):
            storlet, params, server = self._get_storlet_data(storlet_list[key])

            if server == self.server:
                self._setup_gateway()
                self.logger.info('Blackeagle - Go to execute ' + storlet +
                                 ' storlet with parameters "' + params + '"')

                if not self._verify_access_to_storlet(storlet):
                    return HTTPUnauthorized('Blackeagle - Storlet ' + storlet +
                                            ': No permission')
                self._setup_gateway()
                data_iter = self._call_gateway(req_resp, params, data_iter)
            else:
                storlet_execution = {'storlet': storlet,
                                     'params': params,
                                     'server': server}
                launch_key = len(on_other_server.keys())
                on_other_server[launch_key] = storlet_execution

        if on_other_server:
            req_resp.headers['Storlet-List'] = json.dumps(on_other_server)

        if isinstance(req_resp, Request):
            req_resp.environ['wsgi.input'] = data_iter
        else:
            req_resp.app_iter = data_iter

        return req_resp
