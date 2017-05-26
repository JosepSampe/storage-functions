import os
import sys
from optparse import OptionParser


def parse_options():
    parser = OptionParser(usage="%prog CONFIG [options]")
    parser.add_option("-v", "--verbose", default=False, action="store_true",
                      help="log to console")
    options, args = parser.parse_args()

    if not args:
        parser.print_usage()
        print("Error: missing config path argument")
        sys.exit(1)
    config = os.path.abspath(args.pop(0))
    if not os.path.exists(config):
        parser.print_usage()
        print("Error: unable to locate %s" % config)
        sys.exit(1)

    extra_args = []
    # if any named options appear in remaining args, set the option to True
    for arg in args:
        if arg in options.__dict__:
            setattr(options, arg, True)
        else:
            extra_args.append(arg)

    options = vars(options)
    if extra_args:
        options['extra_args'] = extra_args
    return config, options
