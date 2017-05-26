from eventlet import Timeout

from swift.common.utils import get_logger
from swift.common.constraints import check_utf8
from swift.common.swob import HTTPBadRequest, HTTPForbidden, \
    HTTPMethodNotAllowed, HTTPNotFound, HTTPPreconditionFailed, \
    HTTPServerError, HTTPException, Request, HTTPOk


class MiddleBoxApp(object):
    """WSGI application for the Middlebox server."""

    def __init__(self, conf, logger=None):
        if conf is None:
            conf = {}
        if logger is None:
            self.logger = get_logger(conf, log_route='middlebox-server')
        else:
            self.logger = logger

    def __call__(self, env, start_response):
        """
        WSGI entry point.
        Wraps env in swob.Request object and passes it down.

        :param env: WSGI environment dictionary
        :param start_response: WSGI callable
        """
        try:
            req = self.update_request(Request(env))
            return self.handle_request(req)(env, start_response)
        except UnicodeError:
            err = HTTPPreconditionFailed(
                request=req, body='Invalid UTF8 or contains NULL')
            return err(env, start_response)
        except (Exception, Timeout):
            start_response('500 Server Error',
                           [('Content-Type', 'text/plain')])
            return ['Internal server error.\n']

    def update_request(self, req):
        if 'x-storage-token' in req.headers and \
                'x-auth-token' not in req.headers:
            req.headers['x-auth-token'] = req.headers['x-storage-token']
        return req

    def handle_request(self, req):
        """
        Entry point for server.
        Should return a WSGI-style callable (such as swob.Response).

        :param req: swob.Request object
        """
        try:
            self.logger.set_statsd_prefix('middlebox-server')
            print(req.path)
            if req.content_length and req.content_length < 0:
                self.logger.increment('errors')
                return HTTPBadRequest(request=req,
                                      body='Invalid Content-Length')

            try:
                if not check_utf8(req.path_info):
                    self.logger.increment('errors')
                    return HTTPPreconditionFailed(
                        request=req, body='Invalid UTF8 or contains NULL')
            except UnicodeError:
                self.logger.increment('errors')
                return HTTPPreconditionFailed(
                    request=req, body='Invalid UTF8 or contains NULL')

            return HTTPOk(body='Good!', request=req)

        except HTTPException as error_response:
            return error_response
        except (Exception, Timeout):
            self.logger.exception('ERROR Unhandled exception in request')
            return HTTPServerError(request=req)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI middlebox apps."""
    conf = global_conf.copy()
    conf.update(local_conf)
    app = MiddleBoxApp(conf)
    return app
