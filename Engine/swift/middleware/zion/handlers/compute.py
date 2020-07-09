from zion.handlers import BaseHandler
from zion.handlers.base import NotFunctionRequest
from swift.common.utils import public
import time

class ComputeHandler(BaseHandler):

    def __init__(self, request, conf, app, logger, redis):
        super(ComputeHandler, self).__init__(
            request, conf, app, logger, redis)

    def _parse_vaco(self):
        return self.req.split_path(3, 4, rest_with_last=True)

    def _get_functions(self):
        return eval(self.req.headers.pop('functions_data'))

    def is_valid_request(self):
        return 'functions_data' in self.req.headers

    def handle_request(self):
        if hasattr(self, self.method) and self.is_valid_request():
            try:
                handler = getattr(self, self.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                raise NotFunctionRequest()
            return handler()
        else:
            raise NotFunctionRequest()

    @public
    def GET(self):
        """
        GET handler on Compute node
        """
        functions_data = self._get_functions()
        self.response = self.req.get_response(self.app)
        # self.response = Response(body="Test", headers=self.req.headers)
        t0 = time.time()
        self.apply_function_onget(functions_data)
        self.logger.info('------> TOAL ZION TIME: %0.6fs' % ((time.time()-t0)))

        return self.response

    @public
    def PUT(self):
        """
        PUT handler on Compute node
        """
        functions_data = self._get_functions()
        return self.apply_function_onput(functions_data)
