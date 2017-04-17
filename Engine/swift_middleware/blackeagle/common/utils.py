from swift.common.internal_client import InternalClient
from swift.common.exceptions import DiskFileXattrNotSupported, DiskFileNoSpace
from swift.common.exceptions import DiskFileNotExist
from swift.obj.diskfile import get_data_dir as df_data_dir, _get_filename
from swift.common.request_helpers import get_name_and_placement
from swift.common.utils import storage_directory, hash_path, cache_from_env
from swift.common.wsgi import make_subrequest
from eventlet import Timeout
import select
import xattr
import logging
import pickle
import errno
import os


PICKLE_PROTOCOL = 2

SYSMETA_OBJ_HEADER = 'X-Object-Sysmeta-Function-'
FUNCTION_HEADER_OBJ = SYSMETA_OBJ_HEADER + 'List'

SYSMETA_CONTAINER_HEADER = 'X-Container-Sysmeta-Function-'
FUNCTION_HEADER_CONTAINER = SYSMETA_CONTAINER_HEADER + 'List'

SWIFT_METADATA_KEY = 'user.swift.metadata'

LOCAL_PROXY = '/etc/swift/local-proxy-server.conf'
DEFAULT_MD_STRING = {'onget': None,
                     'onput': None,
                     'ondelete': None}


def read_metadata(fd, md_key=None):
    """
    Helper function to read the pickled metadata from an object file.

    :param fd: file descriptor or filename to load the metadata from
    :param md_key: metadata key to be read from object file
    :returns: dictionary of metadata
    """
    meta_key = SWIFT_METADATA_KEY

    metadata = ''
    key = 0
    try:
        while True:
            metadata += xattr.getxattr(fd, '%s%s' % (meta_key,
                                                     (key or '')))
            key += 1
    except (IOError, OSError) as e:
        if metadata == '':
            return False
        for err in 'ENOTSUP', 'EOPNOTSUPP':
            if hasattr(errno, err) and e.errno == getattr(errno, err):
                msg = "Filesystem at %s does not support xattr" % \
                      _get_filename(fd)
                logging.exception(msg)
                raise DiskFileXattrNotSupported(e)
        if e.errno == errno.ENOENT:
            raise DiskFileNotExist()
    return pickle.loads(metadata)


def write_metadata(fd, metadata, xattr_size=65536, md_key=None):
    """
    Helper function to write pickled metadata for an object file.

    :param fd: file descriptor or filename to write the metadata
    :param md_key: metadata key to be write to object file
    :param metadata: metadata to write
    """
    meta_key = SWIFT_METADATA_KEY

    metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
    key = 0
    while metastr:
        try:
            xattr.setxattr(fd, '%s%s' % (meta_key, key or ''),
                           metastr[:xattr_size])
            metastr = metastr[xattr_size:]
            key += 1
        except IOError as e:
            for err in 'ENOTSUP', 'EOPNOTSUPP':
                if hasattr(errno, err) and e.errno == getattr(errno, err):
                    msg = "Filesystem at %s does not support xattr" % \
                          _get_filename(fd)
                    logging.exception(msg)
                    raise DiskFileXattrNotSupported(e)
            if e.errno in (errno.ENOSPC, errno.EDQUOT):
                msg = "No space left on device for %s" % _get_filename(fd)
                logging.exception(msg)
                raise DiskFileNoSpace()
            raise


def get_object_metadata(data_file):
    """
    Retrieves the swift metadata of a specified data file

    :param data_file: full path of the data file
    :returns: dictionary with all swift metadata
    """
    fd = open_data_file(data_file)
    metadata = read_metadata(fd, SWIFT_METADATA_KEY)
    close_data_file(fd)

    return metadata


def get_container_metadata(ctx, container):
    new_env = dict(ctx.request.environ)
    auth_token = ctx.request.headers.get('X-Auth-Token')
    sub_req = make_subrequest(new_env, 'HEAD', container,
                              headers={'X-Auth-Token': auth_token},
                              swift_source='function_middleware')
    response = sub_req.get_response(ctx.app)
    return response.headers


def set_object_metadata(data_file, metadata):
    """
    Sets the swift metadata to the specified data_file

    :param data_file: full path of the data file
    """
    fd = open_data_file(data_file)
    write_metadata(fd, metadata, md_key=SWIFT_METADATA_KEY)
    close_data_file(fd)


def set_container_metadata(ctx, metadata):
    """
    Sets the swift metadata to the container

    :param metadata: metadata dictionary
    """
    memcache = cache_from_env(ctx.request.environ)
    dest_path = os.path.join('/', ctx.api_version, ctx.account, ctx.container)
    for key in metadata.keys():
        if not key.startswith(SYSMETA_CONTAINER_HEADER):
            del metadata[key]
    # We store the Function metadata in the memcached server (only 10 minutes)
    memcache.set("function_"+dest_path, metadata, time=600)
    new_env = dict(ctx.request.environ)
    auth_token = ctx.request.headers.get('X-Auth-Token')
    metadata.update({'X-Auth-Token': auth_token})
    sub_req = make_subrequest(new_env, 'POST', dest_path,
                              headers=metadata,
                              swift_source='function_middleware')
    sub_req.get_response(ctx.app)


def make_swift_request(op, account, container=None, obj=None):
    """
    Makes a swift request via a local proxy (cost expensive)

    :param op: opertation (PUT, GET, DELETE, HEAD)
    :param account: swift account
    :param container: swift container
    :param obj: swift object
    :returns: swift.common.swob.Response instance
    """
    iclient = InternalClient(LOCAL_PROXY, 'function_middleware', 1)
    path = iclient.make_path(account, container, obj)
    resp = iclient.make_request(op, path, {'PATH_INFO': path}, [200])

    return resp


def verify_access(ctx, path):
    """
    Verifies access to the specified object in swift

    :param ctx: ProxyHandler instance
    :param path: swift path of the object to check
    :returns: headers of the object whether exists
    """
    ctx.logger.debug('Verifying access to %s' % path)

    new_env = dict(ctx.request.environ)
    if 'HTTP_TRANSFER_ENCODING' in new_env.keys():
        del new_env['HTTP_TRANSFER_ENCODING']

    for key in DEFAULT_MD_STRING.keys():
        env_key = 'HTTP_X_FUNCTION_' + key.upper()
        if env_key in new_env.keys():
            del new_env[env_key]

    auth_token = ctx.request.headers.get('X-Auth-Token')
    sub_req = make_subrequest(
        new_env, 'HEAD', path,
        headers={'X-Auth-Token': auth_token},
        swift_source='function_middleware')

    return sub_req.get_response(ctx.app)


def get_data_dir(ctx):
    """
    Gets the data directory full path

    :param ctx: ObjectHandler instance
    :returns: the data directory path
    """
    devices = ctx.conf.get('devices')
    device, partition, account, container, obj, policy = \
        get_name_and_placement(ctx.request, 5, 5, True)
    name_hash = hash_path(account, container, obj)
    device_path = os.path.join(devices, device)
    storage_dir = storage_directory(df_data_dir(policy), partition, name_hash)
    data_dir = os.path.join(device_path, storage_dir)

    return data_dir


def get_data_file(ctx):
    """
    Gets the data file full path

    :param ctx: ObjectHandler instance
    :returns: the data file path
    """
    data_dir = get_data_dir(ctx)
    files = os.listdir(data_dir)

    for swift_file in files:
        if swift_file.endswith(".data"):
            return os.path.join(data_dir, swift_file)


def open_data_file(data_file):
    """
    Open a data file

    :param data_file: full path of the data file
    :returns: a file descriptor of the open data file
    """
    fd = os.open(data_file, os.O_RDONLY)
    return fd


def close_data_file(fd):
    """
    Close a file descriptor

    :param fd: file descriptor
    """
    os.close(fd)


def set_function_container(ctx, trigger, function):
    """
    Sets a function to the specified container in the main request,
    and stores the metadata file

    :param ctx: ObjectHandler instance
    :param trigger: trigger name
    :param function: function name
    :raises ValueError: If it fails
    """
    container = os.path.join('/', ctx.api_version, ctx.account, ctx.container)

    # 1st: set function name to list
    metadata = get_container_metadata(ctx, container)
    try:
        function_dict = get_function_dict_container(metadata)
    except:
        raise ValueError('ERROR: There was an error getting trigger'
                         ' dictionary from the object.\n')

    if not function_dict:
        function_dict = DEFAULT_MD_STRING
    if not function_dict[trigger]:
        function_dict[trigger] = list()
    if function not in function_dict[trigger]:
        function_dict[trigger].append(function)

    # 2nd: Get function specific metadata
    specific_md = ctx.request.body.rstrip()

    # 3rd: Assign all metadata to the container
    try:
        metadata[FUNCTION_HEADER_CONTAINER] = function_dict
        sysmeta_key = (SYSMETA_CONTAINER_HEADER + trigger + '-' + function).title()
        if specific_md:
            metadata[sysmeta_key] = specific_md
        else:
            if sysmeta_key in metadata:
                del metadata[sysmeta_key]
        set_container_metadata(ctx, metadata)
    except:
        raise ValueError('ERROR: There was an error setting trigger'
                         ' dictionary from the object.\n')


def unset_function_from_container(ctx, trigger, function):
    """
    Unsets a function to the specified object in the main request

    :param ctx: ObjectHandler instance
    :param trigger: trigger name
    :param function: function name
    :raises ValueError: If it fails
    """
    ctx.logger.debug('Going to unset "' + function +
                     '" function from "' + trigger + '" trigger')

    container = os.path.join('/', ctx.api_version, ctx.account, ctx.container)
    metadata = get_container_metadata(ctx, container)
    try:
        function_dict = get_function_dict_container(metadata)
    except:
        raise ValueError('ERROR: There was an error getting trigger'
                         ' metadata from the object.\n')

    try:
        if trigger == "function" and function == "all":
            for key in metadata.keys():
                if key.startswith(SYSMETA_CONTAINER_HEADER):
                    del metadata[key]
        else:
            if metadata[FUNCTION_HEADER_CONTAINER]:
                if isinstance(metadata[FUNCTION_HEADER_CONTAINER], dict):
                    function_dict = metadata[FUNCTION_HEADER_CONTAINER]
                else:
                    function_dict = eval(metadata[FUNCTION_HEADER_CONTAINER])

                if function == 'all':
                    function_list = function_dict[trigger]
                    function_dict[trigger] = None
                    for mc_k in function_list:
                        sysmeta_key = (SYSMETA_CONTAINER_HEADER + trigger + '-' + mc_k).title()
                        if sysmeta_key in metadata:
                            metadata[sysmeta_key] = ''
                elif function in function_dict[trigger]:
                    function_dict[trigger].remove(function)
                    sysmeta_key = (SYSMETA_CONTAINER_HEADER + trigger + '-' + function).title()
                    if sysmeta_key in metadata:
                        metadata[sysmeta_key] = ''
                else:
                    raise

                metadata[FUNCTION_HEADER_CONTAINER] = function_dict
                metadata = clean_function_dict_container(metadata)
            else:
                raise

        set_container_metadata(ctx, metadata)
    except:
        raise ValueError('Error: Function "' + function + '" not'
                         ' assigned to the "' + trigger + '" trigger.\n')


def set_function_object(ctx, trigger, function):
    """
    Sets a function to the specified object in the main request,
    and stores the metadata file

    :param ctx: ObjectHandler instance
    :param trigger: trigger name
    :param function: function name
    :raises ValueError: If it fails
    """

    # 1st: set function name to list
    try:
        function_dict = get_function_dict_object(ctx)
    except:
        raise ValueError('ERROR: There was an error getting trigger'
                         ' dictionary from the object.\n')

    if not function_dict:
        function_dict = DEFAULT_MD_STRING
    if not function_dict[trigger]:
        function_dict[trigger] = list()
    if function not in function_dict[trigger]:
        function_dict[trigger].append(function)

    for tger in function_dict.keys():
        if function_dict[tger] == None:
            del function_dict[tger]

    # 2nd: Set function specific metadata
    specific_md = ctx.request.body.rstrip()

    # 3rd: Assign all metadata to the object
    try:
        data_file = get_data_file(ctx)
        metadata = get_object_metadata(data_file)
        metadata[FUNCTION_HEADER_OBJ] = function_dict
        sysmeta_key = (SYSMETA_OBJ_HEADER + trigger + '-' + function).title()
        if specific_md:
            metadata[sysmeta_key] = specific_md
        else:
            if sysmeta_key in metadata:
                del metadata[sysmeta_key]

        set_object_metadata(data_file, metadata)
    except Exception as e:
        print e
        raise ValueError('ERROR: There was an error setting trigger'
                         ' dictionary from the object.\n')


def unset_function_object(ctx, trigger, function):
    """
    Unsets a function to the specified object in the main request

    :param ctx: ObjectHandler instance
    :param trigger: trigger name
    :param function: function name
    :raises ValueError: If it fails
    """
    ctx.logger.debug('Going to unset "' + function +
                     '" function from "' + trigger + '" trigger')

    try:
        data_file = get_data_file(ctx)
        metadata = get_object_metadata(data_file)
    except:
        raise ValueError('ERROR: There was an error getting trigger'
                         ' metadata from the object.\n')

    try:
        if trigger == "function" and function == "all":
            for key in metadata.keys():
                if key.startswith(SYSMETA_OBJ_HEADER):
                    del metadata[key]
        else:
            if metadata[FUNCTION_HEADER_OBJ]:
                if isinstance(metadata[FUNCTION_HEADER_OBJ], dict):
                    function_dict = metadata[FUNCTION_HEADER_OBJ]
                else:
                    function_dict = eval(metadata[FUNCTION_HEADER_OBJ])
                if function == 'all':
                    function_list = function_dict[trigger]
                    function_dict[trigger] = None
                    for mc_k in function_list:
                        sysmeta_key = (SYSMETA_OBJ_HEADER + trigger + '-' + mc_k).title()
                        if sysmeta_key in metadata:
                            del metadata[sysmeta_key]
                elif function in function_dict[trigger]:
                    function_dict[trigger].remove(function)
                    sysmeta_key = (SYSMETA_OBJ_HEADER + trigger + '-' + function).title()
                    if sysmeta_key in metadata:
                        del metadata[sysmeta_key]
                else:
                    raise
                metadata[FUNCTION_HEADER_OBJ] = function_dict
                metadata = clean_function_dict_object(metadata)
            else:
                raise
        set_object_metadata(data_file, metadata)
    except:
        raise ValueError('Error: Function "' + function + '" not'
                         ' assigned to the "' + trigger + '" trigger.\n')

    data_dir = get_data_dir(ctx)
    ctx.logger.debug('Object path: ' + data_dir)


def clean_function_dict_object(function_metadata):
    """
    Auxiliary function that cleans the function dictionary, deleting
    empty lists for each trigger, and deleting all dictionary whether all
    values are None.

    :param function_metadata: function dictionary
    :returns function_metadata: function dictionary
    """
    for trigger in function_metadata[FUNCTION_HEADER_OBJ].keys():
        if not function_metadata[FUNCTION_HEADER_OBJ][trigger]:
            function_metadata[FUNCTION_HEADER_OBJ][trigger] = None

    if all(value is None for value in function_metadata[FUNCTION_HEADER_OBJ].values()):
        del function_metadata[FUNCTION_HEADER_OBJ]

    return function_metadata


def clean_function_dict_container(function_metadata):
    """
    Auxiliary function that cleans the function dictionary, deleting
    empty lists for each trigger, and deleting all dictionary whether all
    values are None.

    :param function_metadata: function dictionary
    :returns function_metadata: function dictionary
    """
    mc_dict = eval(function_metadata[FUNCTION_HEADER_CONTAINER])
    for trigger in mc_dict.keys():
        if not mc_dict[trigger]:
            mc_dict[trigger] = None

    if all(value is None for value in mc_dict.values()):
        function_metadata[FUNCTION_HEADER_CONTAINER] = ''

    return function_metadata


def get_function_dict_object(ctx):
    """
    Gets the list of associated functions to the requested object.
    This method retrieves a dictionary with all triggers and all
    functions associated to each trigger.

    :param ctx: ObjectHandler instance
    :returns: function dictionary
    """
    data_file = get_data_file(ctx)
    metadata = get_object_metadata(data_file)

    if FUNCTION_HEADER_OBJ in metadata:
        if isinstance(metadata[FUNCTION_HEADER_OBJ], dict):
            return metadata[FUNCTION_HEADER_OBJ]
        else:
            return eval(metadata[FUNCTION_HEADER_OBJ])
    else:
        return None


def get_function_dict_container(function_metadata):
    """
    Gets the list of associated functions to the requested container.
    This method retrieves a dictionary with all triggers and all
    functions associated to each trigger.

    :param function_metadata: response headers of the object
    :returns: function dictionary
    """
    if FUNCTION_HEADER_CONTAINER in function_metadata:
        if isinstance(function_metadata[FUNCTION_HEADER_CONTAINER], dict):
            return function_metadata[FUNCTION_HEADER_CONTAINER]
        else:
            return eval(function_metadata[FUNCTION_HEADER_CONTAINER])
    else:
        return None


def get_function_list_object(function_metadata, method):
    """
    Gets the list of associated functions to the requested object.
    This method gets the functions dictionary from the object headers,
    and filter the content to return only a list of names of functions
    associated to the type of request (put, get, delete)

    :param function_metadata: response headers of the object
    :param method: current method
    :returns: function list associated to the type of the request
    """
    if not function_metadata:
        return None

    if function_metadata[FUNCTION_HEADER_OBJ]:
        if isinstance(function_metadata[FUNCTION_HEADER_OBJ], dict):
            function_dict = function_metadata[FUNCTION_HEADER_OBJ]
        else:
            function_dict = eval(function_metadata[FUNCTION_HEADER_OBJ])
        function_list = function_dict["on" + method]
    else:
        function_list = None

    return function_list


class DataFdIter(object):
    def __init__(self, fd):
        self.closed = False
        self.fd = fd
        self.timeout = 10
        self.buf = b''

    def __iter__(self):
        return self

    def read_with_timeout(self, size):
        try:
            with Timeout(self.timeout):
                chunk = os.read(self.fd, size)
        except Timeout:
            if self.cancel_func:
                self.cancel_func()
            self.close()
            raise
        except Exception:
            self.close()
            raise
        return chunk

    def next(self, size=64 * 1024):
        if len(self.buf) < size:
            r, _, _ = select.select([self.fd], [], [], self.timeout)
            if len(r) == 0:
                self.close()

            if self.fd in r:
                self.buf += self.read_with_timeout(size - len(self.buf))
                if self.buf == b'':
                    raise StopIteration('Stopped iterator ex')
            else:
                raise StopIteration('Stopped iterator ex')

        if len(self.buf) > size:
            data = self.buf[:size]
            self.buf = self.buf[size:]
        else:
            data = self.buf
            self.buf = b''
        return data

    def _close_check(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')

    def read(self, size=64 * 1024):
        self._close_check()
        return self.next(size)

    def readline(self, size=-1):
        self._close_check()

        # read data into self.buf if there is not enough data
        while b'\n' not in self.buf and \
              (size < 0 or len(self.buf) < size):
            if size < 0:
                chunk = self.read()
            else:
                chunk = self.read(size - len(self.buf))
            if not chunk:
                break
            self.buf += chunk

        # Retrieve one line from buf
        data, sep, rest = self.buf.partition(b'\n')
        data += sep
        self.buf = rest

        # cut out size from retrieved line
        if size >= 0 and len(data) > size:
            self.buf = data[size:] + self.buf
            data = data[:size]

        return data

    def readlines(self, sizehint=-1):
        self._close_check()
        lines = []
        try:
            while True:
                line = self.readline(sizehint)
                if not line:
                    break
                lines.append(line)
                if sizehint >= 0:
                    sizehint -= len(line)
                    if sizehint <= 0:
                        break
        except StopIteration:
            pass
        return lines

    def close(self):
        if self.closed:
            return
        os.close(self.fd)
        self.closed = True

    def __del__(self):
        self.close()


class DataIter(object):
    def __init__(self, resp, timeout):
        self.closed = False
        self.resp = resp
        self.timeout = timeout
        self.buf = b''

    def __iter__(self):
        return self

    def read_with_timeout(self, size):
        try:
            with Timeout(self.timeout):
                chunk = self.resp.read(size)
        except Timeout:
            self.close()
            raise
        except Exception:
            self.close()
            raise

        return chunk

    def next(self, size=64 * 1024):
        if len(self.buf) < size:
            self.buf += self.read_with_timeout(size - len(self.buf))
            if self.buf == b'':
                self.close()
                raise StopIteration('Stopped iterator ex')

        if len(self.buf) > size:
            data = self.buf[:size]
            self.buf = self.buf[size:]
        else:
            data = self.buf
            self.buf = b''
        return data

    def _close_check(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')

    def read(self, size=64 * 1024):
        self._close_check()
        return self.next(size)

    def close(self):
        if self.closed:
            return
        self.resp.close()
        self.closed = True

    def __del__(self):
        self.close()
