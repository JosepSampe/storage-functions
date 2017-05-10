from blackeagle.gateways.docker.bus import Bus
from blackeagle.gateways.docker.datagram import Datagram
from eventlet.timeout import Timeout
import select
import eventlet
import json
import os
import subprocess
import time
import cmd
import sys

FUNCTION_FD_INPUT_OBJECT = 0
FUNCTION_FD_OUTPUT_OBJECT = 1
FUNCTION_FD_OUTPUT_COMMAND = 2
FUNCTION_FD_LOGGER = 4

F_MAIN_HEADER = "X-Object-Meta-Function-Main"
F_DEP_HEADER = "X-Object-Meta-Function-Library-Dependency"

eventlet.monkey_patch()


class RuntimeSandbox(object):
    """
    The RunTimeSandbox represents a re-usable per scope sandbox.
    """

    def __init__(self, logger, conf, account):
        self.scope = account[5:18]
        self.conf = conf
        self.logger = logger
        self.docker_img_prefix = conf['docker_img_prefix']
        self.docker_repo = conf['docker_repo']
        self.workers = conf['workers']

    def _is_started(self, container_name):
        """
        Auxiliary function that checks whether the container is started.

        :param docker_container_name : name of the container
        :returns: whether exists
        """
        cmd = ("docker ps | grep -v 'grep' | grep '" +
               container_name + "' | awk '{print $1}'")
        docker_id = os.popen(cmd).read()

        if not docker_id:
            return False

        return True

    def _is_stopped(self, container_name):
        """
        Auxiliary function that checks whether the container is started.

        :param docker_container_name : name of the container
        :returns: whether exists
        """
        cmd = ("docker ps -f 'status=exited' | grep -v 'grep' | grep '" +
               container_name + "' | awk '{print $1}'")
        docker_id = os.popen(cmd).read()

        if not docker_id:
            return False

        return True

    def _delete(self, container_name):
        cmd = ("docker rm -f " + container_name)
        os.popen(cmd)

    def start(self):
        """
        Starts the docker container.
        """
        container_name = '%s_%s' % (self.docker_img_prefix, self.scope)

        if self._is_stopped(container_name):
            self._delete(container_name)

        if not self._is_started(container_name):
            docker_image_name = '%s/%s' % (self.docker_repo, self.scope)

            local_functions_dir = self.conf["main_dir"] + "/" + self.scope

            java_runtime_local = local_functions_dir + "/" + self.conf["java_runtime_dir"]
            java_runtime_docker = "/home/swift"
            java_runtime_mount = '%s:%s' % (java_runtime_local, java_runtime_docker)

            pipes_local = local_functions_dir + "/" + self.conf["pipes_dir"]
            pipes_docker = "/mnt/channels"
            pipes_mount = '%s:%s' % (pipes_local, pipes_docker)

            cmd = 'docker run --name ' + container_name + \
                  ' -d -v /dev/log:/dev/log -v ' + pipes_mount + \
                  ' -v ' + java_runtime_mount + ' -i -t ' + docker_image_name + \
                  ' debug "/home/swift/start_daemon.sh ' + self.workers + '"'

            self.logger.info(cmd)

            self.logger.info('Starting container ' + container_name + ' ...')

            p = subprocess.call(cmd, shell=True)

            if p == 0:
                time.sleep(1)
                self.logger.info('Container "' + container_name + '" started')
        else:
            self.logger.info('Container "' + container_name + '" is already started')


class Function(object):
    """
    Function main class.
    """

    def __init__(self, logger_path, name, main, dependencies):
        self.log_path = os.path.join(logger_path, main)
        self.log_name = name.replace('jar', 'log')
        self.full_log_path = os.path.join(self.log_path, self.log_name)
        self.function = name
        self.main_class = main
        self.dependencies = dependencies

        if not os.path.exists(self.log_path):
            os.makedirs(self.log_path)

    def open(self):
        self.logger_file = open(self.full_log_path, 'a')

    def get_logfd(self):
        return self.logger_file.fileno()

    def get_name(self):
        return self.function

    def get_dependencies(self):
        return self.dependencies

    def get_main(self):
        return self.main_class

    def get_size(self):
        statinfo = os.stat(self.full_path)
        return statinfo.st_size

    def close(self):
        self.logger_file.close()


class FunctionInvocationProtocol(object):

    def __init__(self, input_stream, f_pipe_path, f_logger_path, req_headers,
                 object_headers, f_list, f_metadata, timeout, logger):
        self.input_stream = input_stream
        self.logger = logger
        self.f_pipe_path = f_pipe_path
        self.f_logger_path = f_logger_path
        self.timeout = timeout
        self.req_md = req_headers
        self.object_md = object_headers
        self.function_list = f_list  # Ordered function execution list
        self.f_md = f_metadata  # Function metadata
        self.functions = list()  # Function object list

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

    def _add_input_object_stream(self):
        # Actual object from swift passed to function
        if hasattr(self.input_stream, '_fp'):
            self.input_data_read_fd = self.input_stream._fp.fileno()
        else:
            self.internal_pipe = True
            self.input_data_read_fd, self.input_data_write_fd = os.pipe()

        self.fds.append(self.input_data_read_fd)

        if "X-Service-Catalog" in self.req_md:
            del self.req_md['X-Service-Catalog']

        if "Cookie" in self.req_md:
            del self.req_md['Cookie']

        headers = {'req_md': self.req_md, 'object_md': self.object_md}

        md = dict()
        md['type'] = FUNCTION_FD_INPUT_OBJECT
        md['json_md'] = json.dumps(headers)
        self.fdmd.append(md)

    def _add_output_object_stream(self):
        self.output_data_read_fd, self.output_data_write_fd = os.pipe()
        self.fds.append(self.output_data_write_fd)
        md = dict()
        md['type'] = FUNCTION_FD_OUTPUT_OBJECT
        self.fdmd.append(md)

    def _add_output_command_stream(self):
        """
        Adds the output fd to send commands from the function
        """
        self.command_read_fd, self.command_write_fd = os.pipe()
        self.fds.append(self.command_write_fd)
        md = dict()
        md['type'] = FUNCTION_FD_OUTPUT_COMMAND
        self.fdmd.append(md)

    def _add_function_data(self):
        for f in self.functions:
            self.fds.append(f.get_logfd())
            md = dict()
            md['type'] = FUNCTION_FD_LOGGER
            md['function'] = f.get_name()
            md['main'] = f.get_main()
            md['dependencies'] = f.get_dependencies()
            self.fdmd.append(md)

    def _prepare_invocation_descriptors(self):
        self._add_input_object_stream()
        self._add_output_object_stream()
        self._add_output_command_stream()
        self._add_function_data()

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
        # dtg.set_exec_params(prms)
        dtg.set_command(1)

        # Send datagram to container daemon
        rc = Bus.send(self.f_pipe_path, dtg)
        if (rc < 0):
            raise Exception("Failed to send execute command")

    def _wait_for_read_with_timeout(self, fd):
        r, _, _ = select.select([fd], [], [], self.timeout)
        if len(r) == 0:
            raise Timeout('Timeout while waiting for Function output')
        if fd in r:
            return

    def _send_data_to_function(self):
        if self.internal_pipe:
            eventlet.spawn_n(self._write_input_data,
                             self.input_data_write_fd,
                             self.input_stream)

    def _write_input_data(self, w_fd, data_iter):
        try:
            writer = os.fdopen(w_fd, 'w')
            for chunk in data_iter:
                with Timeout(self.timeout):
                    writer.write(chunk)
            writer.close()
        except Exception:
            self.logger.exception('Unexpected error at writing input data')

    def byteify(self, data):
        if isinstance(data, dict):
            return {self.byteify(key): self.byteify(value)
                    for key, value in data.iteritems()}
        elif isinstance(data, list):
            return [self.byteify(element) for element in data]
        elif isinstance(data, unicode):
            return data.encode('utf-8')
        else:
            return data

    def _read_response(self):
        f_resp = dict()
        for f_name in self.function_list:
            try:
                self._wait_for_read_with_timeout(self.command_read_fd)
                flat_json = os.read(self.command_read_fd, 12)

                if flat_json:
                    f_resp[f_name] = self.byteify(json.loads(flat_json))
                else:
                    raise ValueError('No response from function')
            except:
                # TODO: handle timeout or no response exception
                e = sys.exc_info()[1]
                f_resp[f_name] = dict()
                f_resp[f_name]['cmd'] = 'RE'  # Request Error
                f_resp[f_name]['message'] = ('Error running ' + f_name +
                                             ': No response from function.')

        # TODO: read extra data from pipe
        out_data = dict()
        for f_name in self.function_list:
            command = f_resp[f_name]['cmd']

            if command == 'DR':
                # Data Read
                self._send_data_to_function()
                out_data = self._read_response()
                break
            if command == 'DW':
                # Data Write
                out_data['command'] = command
                out_data['fd'] = self.output_data_read_fd
            if command == 'RE':
                # Request Error
                out_data['command'] = command
                out_data['message'] = f_resp[f_name]['message']
                break
            if command == 'RR':
                # Request Rewire
                out_data['command'] = command
                out_data['object_id'] = f_resp[f_name]['object_id']
                break
            if command == 'RS':
                # Request Storlet
                out_data['command'] = command
                if 'list' not in out_data:
                    out_data['list'] = dict()
                for k in sorted(f_resp[f_name]['list']):
                    new_key = len(out_data['list'])
                    out_data['list'][new_key] = f_resp[f_name]['list'][k]
                break
            if command == 'RC':
                out_data['command'] = command

            if 'object_metadata' in f_resp[f_name]:
                out_data['object_metadata'] = f_resp[f_name]['object_metadata']
            if 'request_headers' in f_resp[f_name]:
                out_data['request_headers'] = f_resp[f_name]['request_headers']
            if 'response_headers' in f_resp[f_name]:
                out_data['response_headers'] = f_resp[f_name]['response_headers']

        if out_data['command'] != 'DW':
            self._close_local_side_descriptors()

        return out_data

    def communicate(self):
        for function_name in self.function_list:
            function = Function(self.f_logger_path,
                                function_name,
                                self.f_md[function_name][F_MAIN_HEADER],
                                self.f_md[function_name][F_DEP_HEADER])
            self.functions.append(function)

        for function in self.functions:
            function.open()

        self._prepare_invocation_descriptors()

        try:
            self._invoke()
        except Exception as e:
            raise e
        finally:
            self._close_remote_side_descriptors()
            for function in self.functions:
                function.close()

        out_data = self._read_response()
        os.close(self.command_read_fd)

        return out_data
