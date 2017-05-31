from blackeagle.gateways.docker.bus import Bus
from blackeagle.gateways.docker.datagram import Datagram
from eventlet.timeout import Timeout
import select
import eventlet
import json
import os
import sys

eventlet.monkey_patch()


class Protocol(object):

    def __init__(self, worker, object_stream, object_metadata,
                 request_headers, function_parameters, be):
        self.worker = worker
        self.object_stream = object_stream
        self.object_metadata = object_metadata
        self.request_headers = request_headers
        self.function_parameters = function_parameters
        self.function_timeout = self.worker.function.get_timeout()
        self.logger = be.logger
        self.function_name = self.worker.function.get_name()

        # remote side file descriptors and their metadata lists
        # to be sent as part of invocation
        self.fds = list()
        self.fdmd = list()

        # local side file descriptors
        self.command_read_fd = None  # local
        self.command_write_fd = None  # remote
        self.output_data_read_fd = None  # local
        self.output_data_write_fd = None  # remote

        self.input_data_read_fd = None  # Data from the object - remote
        self.input_data_write_fd = None  # Data from the object - local
        self.internal_pipe = False

    def _add_output_object_stream(self):
        self.output_data_read_fd, self.output_data_write_fd = os.pipe()
        self.fds.append(self.output_data_write_fd)
        md = dict()
        md['type'] = "OUTPUT_FD"
        self.fdmd.append(md)

    def _add_output_command_stream(self):
        """
        Adds the output fd to send commands from the function
        """
        self.command_read_fd, self.command_write_fd = os.pipe()
        self.fds.append(self.command_write_fd)
        md = dict()
        md['type'] = "COMMAND_FD"
        self.fdmd.append(md)

    def _add_input_object_stream(self):
        # Actual object from swift passed to function
        if hasattr(self.object_stream, '_fp'):
            self.input_data_read_fd = self.object_stream._fp.fileno()
        else:
            self.internal_pipe = True
            self.input_data_read_fd, self.input_data_write_fd = os.pipe()

        self.fds.append(self.input_data_read_fd)

        if "X-Service-Catalog" in self.request_headers:
            del self.request_headers['X-Service-Catalog']

        if "Cookie" in self.request_headers:
            del self.request_headers['Cookie']

        metadata = {'request_headers': self.request_headers,
                    'object_metadata': self.object_metadata,
                    'parameters': self.function_parameters}

        md = dict()
        md['type'] = "INPUT_FD"
        md['data'] = json.dumps(metadata)
        self.fdmd.append(md)

    def _prepare_invocation_fds(self):
        self._add_output_object_stream()
        self._add_output_command_stream()
        self._add_input_object_stream()

    def _close_local_side_descriptors(self):
        if self.output_data_read_fd:
            os.close(self.output_data_read_fd)
            self.output_data_read_fd = None

    def _close_remote_side_descriptors(self):
        if self.output_data_write_fd:
            os.close(self.output_data_write_fd)
        if self.command_write_fd:
            os.close(self.command_write_fd)
        if self.internal_pipe and self.input_data_read_fd:
            os.close(self.input_data_read_fd)

    def _invoke(self):
        dtg = Datagram()
        dtg.set_files(self.fds)
        dtg.set_metadata(self.fdmd)
        dtg.set_command(1)
        # Send datagram to function worker
        channel = self.worker.get_channel()
        rc = Bus.send(channel, dtg)
        if (rc < 0):
            raise Exception("Failed to send data to function")

    def _wait_for_read_with_timeout(self, fd):
        r, _, _ = select.select([fd], [], [], self.function_timeout)
        if len(r) == 0:
            raise Timeout('Timeout while waiting for Function output')
        if fd in r:
            return

    def _send_data_to_function(self):
        if self.internal_pipe:
            eventlet.spawn_n(self._write_input_data,
                             self.input_data_write_fd,
                             self.object_stream)

    def _write_input_data(self, w_fd, data_iter):
        try:
            writer = os.fdopen(w_fd, 'w')
            for chunk in data_iter:
                with Timeout(self.function_timeout):
                    writer.write(chunk)
            writer.close()
        except Exception:
            self.logger.exception('Unexpected error at writing input data')

    def _byteify(self, data):
        if isinstance(data, dict):
            return {self._byteify(key): self._byteify(value)
                    for key, value in data.iteritems()}
        elif isinstance(data, list):
            return [self._byteify(element) for element in data]
        elif isinstance(data, unicode):
            return data.encode('utf-8')
        else:
            return data

    def _read_response(self):
        f_resp = dict()

        try:
            self._wait_for_read_with_timeout(self.command_read_fd)
            flat_json = os.read(self.command_read_fd, 12)

            if flat_json:
                f_resp = self._byteify(json.loads(flat_json))
            else:
                raise ValueError('No response from function')
        except:
            e = sys.exc_info()[1]
            f_resp['cmd'] = 'RE'  # Request Error
            f_resp['message'] = ('Error running ' + self.function_name +
                                 ' function: ' + str(e))

        # TODO: read extra data from pipe
        out_data = dict()
        command = f_resp['cmd']

        if command == 'DR':
            # Data Read
            self._send_data_to_function()
            out_data = self._read_response()

        if command == 'DW':
            # Data Write
            out_data['command'] = command
            out_data['fd'] = self.output_data_read_fd

        if command == 'RE':
            # Request Error
            out_data['command'] = command
            out_data['message'] = f_resp['message']

        if command == 'RR':
            # Request Rewire
            out_data['command'] = command
            out_data['object_id'] = f_resp['object_id']

        if command == 'RC':
            # Request Continue
            out_data['command'] = command

        if 'object_metadata' in f_resp:
            out_data['object_metadata'] = f_resp['object_metadata']
        if 'request_headers' in f_resp:
            out_data['request_headers'] = f_resp['request_headers']
        if 'response_headers' in f_resp:
            out_data['response_headers'] = f_resp['response_headers']

        if out_data['command'] != 'DW':
            self._close_local_side_descriptors()

        return out_data

    def comunicate(self):
        self._prepare_invocation_fds()

        try:
            self._invoke()
        except Exception as e:
            raise e
        finally:
            self._close_remote_side_descriptors()

        out_data = self._read_response()
        os.close(self.command_read_fd)

        return out_data
