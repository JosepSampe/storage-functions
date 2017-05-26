from blackeagle.handlers import BaseHandler
from blackeagle.handlers.base import NotFunctionRequest

from swift.common.utils import public


class ComputeHandler(BaseHandler):

    def __init__(self, request, conf, app, logger):
        super(ComputeHandler, self).__init__(
            request, conf, app, logger)

    def _parse_vaco(self):
        return self.req.split_path(3, 4, rest_with_last=True)

    def _get_functions(self):
        self.function_data = eval(self.req.headers['function_data'])

    def handle_request(self):
        if hasattr(self, self.method):
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
        GET handler on Proxy
        """
        response = self.req.get_response(self.app)
        response = self.apply_function_on_get(response)

        return response

    @public
    def PUT(self):
        """
        PUT handler on Proxy
        """
        self.apply_function_on_put()
        response = self.req.get_response(self.app)

        return response
