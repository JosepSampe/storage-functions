from zion.handlers import BaseHandler
from zion.handlers.base import NotFunctionRequest
from swift.common.utils import public


class ComputeHandler(BaseHandler):

    def __init__(self, request, conf, app, logger, redis):
        super(ComputeHandler, self).__init__(
            request, conf, app, logger, redis)

    def _parse_vaco(self):
        return self.req.split_path(3, 4, rest_with_last=True)

    def _get_functions(self):
        self.function_data = eval(self.req.headers.pop('function_data'))

    def is_valid_request(self):
        return 'function_data' in self.req.headers

    def handle_request(self):
        if hasattr(self, self.method) and self.is_valid_request():
            try:
                self._get_functions()
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
        self.response = self.req.get_response(self.app)
        # self.response = Response(body="Test", headers=self.req.headers)
        self.apply_function_onget()

        return self.response

    @public
    def PUT(self):
        """
        PUT handler on Compute node
        """
        return self.apply_function_onput()
