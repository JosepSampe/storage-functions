from swift.common.exceptions import DiskFileXattrNotSupported, \
    DiskFileNoSpace, DiskFileNotExist
from eventlet import Timeout
import xattr
import select
import logging
import pickle
import errno
import os

PICKLE_PROTOCOL = 2
SWIFT_METADATA_KEY = 'user.swift.metadata'


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
    print('999999999999999999999')
    try:
        while True:
            metadata += xattr.getxattr(fd, '%s%s' % (meta_key,
                                                     (key or '')))
            print(metadata)
            key += 1
    except (IOError, OSError) as e:
        print(e)
        print('888888888888888')
        if metadata == '':
            return False
        for err in 'ENOTSUP', 'EOPNOTSUPP':
            if hasattr(errno, err) and e.errno == getattr(errno, err):
                msg = "Filesystem at %s does not support xattr" % \
                      get_filename(fd)
                logging.exception(msg)
                raise DiskFileXattrNotSupported(e)
        if e.errno == errno.ENOENT:
            raise DiskFileNotExist()
    print(metadata)
    print('///////')
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
                          get_filename(fd)
                    logging.exception(msg)
                    raise DiskFileXattrNotSupported(e)
            if e.errno in (errno.ENOSPC, errno.EDQUOT):
                msg = "No space left on device for %s" % get_filename(fd)
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
    print('******')
    metadata = read_metadata(fd, SWIFT_METADATA_KEY)
    print('-++*-++-++-')
    close_data_file(fd)

    return metadata


def set_object_metadata(data_file, metadata):
    """
    Sets the swift metadata to the specified data_file

    :param data_file: full path of the data file
    """
    fd = open_data_file(data_file)
    write_metadata(fd, metadata, md_key=SWIFT_METADATA_KEY)
    close_data_file(fd)


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


def get_filename(fd):
    """
    Helper function to get to file name from a file descriptor or filename.

    :param fd: file descriptor or filename.

    :returns: the filename.
    """
    if hasattr(fd, 'name'):
        # fd object
        return fd.name

    # fd is a filename
    return fd


class DataFdIter(object):
    def __init__(self, fd):
        self.closed = False
        self.data_fd = fd
        self.timeout = 10
        self.buf = b''

    def __iter__(self):
        return self

    def read_with_timeout(self, size):
        try:
            with Timeout(self.timeout):
                chunk = os.read(self.data_fd, size)
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
            r, _, _ = select.select([self.data_fd], [], [], self.timeout)
            if len(r) == 0:
                self.close()

            if self.data_fd in r:
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
        os.close(self.data_fd)
        self.closed = True

    def __del__(self):
        self.close()
