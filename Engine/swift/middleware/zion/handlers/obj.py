from zion.handlers import ComputeHandler


class ObjectHandler(ComputeHandler):

    def __init__(self, request, conf, app, logger, redis):
        super(ObjectHandler, self).__init__(
            request, conf, app, logger, redis)

    def _parse_vaco(self):
        _, _, acc, cont, obj = self.req.split_path(
            5, 5, rest_with_last=True)
        return ('v1', acc, cont, obj)
