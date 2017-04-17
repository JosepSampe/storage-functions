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
        self.device, self.part, acc, cont, obj = self.request.split_path(
            5, 5, rest_with_last=True)
        return ('v1', acc, cont, obj)

    def handle_request(self):
        if hasattr(self, self.request.method) and self.is_valid_request:
            try:
                handler = getattr(self, self.request.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                return HTTPMethodNotAllowed(request=self.request)
            return handler()
        else:
            return self.request.get_response(self.app)
            # return HTTPMethodNotAllowed(request=self.request)

    def _generate_middlebox_response(self):
        data = dict()
        data['storage_node'] = self.request.environ['SERVER_NAME']
        data['storage_port'] = self.request.environ['SERVER_PORT']
        data['policy'] = self.request.headers['X-Backend-Storage-Policy-Index']
        data['device'] = self.device
        data['part'] = self.part
        response = Response(body='', headers={'Middlebox': data}, request=self.request)

        return response

    @public
    def GET(self):
        """
        GET handler on Object
        """
        available_compute_resources = False

        if not self.is_middlebox_request and not available_compute_resources:
            response = self._generate_middlebox_response()
        else:
            response = self.request.get_response(self.app)
            if not self.is_middlebox_request:
                response = self.apply_function_on_get(response)

        return response

    @public
    def PUT(self):
        """
        PUT handler on Object
        """
        if self.is_function_set:
            trigger, function = self.get_function_assignation_data()

            try:
                set_function_object(self, trigger, function)
                msg = 'Function "' + function + \
                      '" correctly assigned to the "' + trigger + '" trigger.\n'
            except ValueError as e:
                msg = e.args[0]
            self.logger.info(msg)

            response = Response(body=msg, headers={'etag': ''},
                                request=self.request)

        elif self.is_function_unset:
            trigger, function = self.get_function_deletion_data()

            try:
                unset_function_object(self, trigger, function)
                msg = 'Function "' + function +\
                      '" correctly removed from the "' + trigger + '" trigger.\n'
            except ValueError as e:
                msg = e.args[0]

            response = Response(body=msg, headers={'etag': ''},
                                request=self.request)

        else:
            response = self.request.get_response(self.app)

        return response
