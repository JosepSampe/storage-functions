from swift.common.swob import HTTPMethodNotAllowed, Response
from swift.common.utils import public
from blackeagle.handlers import BaseHandler
from blackeagle.common.utils import set_function_object
from blackeagle.common.utils import unset_function_object


class ObjectHandler(BaseHandler):

    def __init__(self, request, conf, app, logger):
        super(ObjectHandler, self).__init__(
            request, conf, app, logger)

    def _parse_vaco(self):
        self.device, self.part, acc, cont, obj = self.req.split_path(
                                5, 5, rest_with_last=True)
        return ('v1', acc, cont, obj)

    def handle_request(self):
        if hasattr(self, self.req.method) and self.is_valid_request:
            try:
                handler = getattr(self, self.req.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                return HTTPMethodNotAllowed(request=self.req)
            return handler()
        else:
            return self.req.get_response(self.app)
            # return HTTPMethodNotAllowed(request=self.req)

    def _generate_middlebox_response(self):
        data = dict()
        data['storage_node'] = self.req.environ['SERVER_NAME']
        data['storage_port'] = self.req.environ['SERVER_PORT']
        data['policy'] = self.req.headers['X-Backend-Storage-Policy-Index']
        data['device'] = self.device
        data['part'] = self.part
        response = Response(body='', headers={'Middlebox': data},
                            request=self.req)

        return response

    @public
    def GET(self):
        """
        GET handler on Object
        """

        r = self._generate_middlebox_response()
        print r.headers['Middlebox']

        available_compute_resources = True

        if not self.is_middlebox_request and not available_compute_resources:
            resp = self._generate_middlebox_response()
        else:
            resp = self.req.get_response(self.app)
            if not self.is_middlebox_request and not self.is_slo_object(resp):
                resp = self.apply_function_on_post_get(resp)

        return resp
