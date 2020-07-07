from zion.gateways.docker.datagram import Datagram
from ctypes import c_char_p
from ctypes import c_int
from ctypes import POINTER
import ctypes


class Bus(object):
    '''@summary: This class wraps low level C-API for Bus functionality
              to be used with Python
    '''
    BUS_SO_NAME = '/opt/zion/runtime/java/lib/bus.so'

    def __init__(self):
        '''@summary:             CTOR
                              Setup argument types mappings.
        '''
        # load the C-library
        self.bus_back_ = ctypes.CDLL(Bus.BUS_SO_NAME)

        # create Bus
        self.bus_back_.bus_create.argtypes = [c_char_p]
        self.bus_back_.bus_create.restype = c_int

        # listen to Bus
        self.bus_back_.bus_listen.argtypes = [c_int]
        self.bus_back_.bus_listen.restype = c_int

        # send message
        self.bus_back_.bus_send_msg.argtypes = [c_char_p,
                                                POINTER(c_int),
                                                c_int,
                                                c_char_p,
                                                c_int,
                                                c_char_p,
                                                c_int]
        self.bus_back_.bus_send_msg.restype = c_int

        # receive message
        self.bus_back_.bus_recv_msg.argtypes = [c_int,
                                                POINTER(POINTER(c_int)),
                                                POINTER(c_int),
                                                POINTER(c_char_p),
                                                POINTER(c_int),
                                                POINTER(c_char_p),
                                                POINTER(c_int)]
        self.bus_back_.bus_recv_msg.restype = c_int

    @staticmethod
    def start_logger(str_log_level='DEBUG', container_id=None):
        '''@summary:             Start logger.
        @param str_log_level: The level of verbosity in log records.
                              Default value - 'DEBUG'.
        @type  str_log_level: string
        @rtype:               void
        '''
        # load the C-library
        bus_back_ = ctypes.CDLL(Bus.BUS_SO_NAME)

        bus_back_.bus_start_logger.argtypes = [c_char_p, c_char_p]
        bus_back_.bus_start_logger(str_log_level, container_id)

    @staticmethod
    def stop_logger():
        '''@summary: Stop logger.
        @rtype:   void
        '''
        # load the C-library
        bus_back_ = ctypes.CDLL(Bus.BUS_SO_NAME)
        bus_back_.bus_stop_logger()

    def create(self, bus_name):
        '''@summary:         Instantiate an Bus. A wrapper for C function.
        @param bus_name: Path to domain socket "file".
        @type  bus_name: string
        @return:          Handler to the opened Bus.
        @rtype:           integer
        '''
        return self.bus_back_.bus_create(bus_name)

    def listen(self, bus_handler):
        '''@summary:            Listen to the Bus.
                             Suspend the executing thread.
        @param bus_handler: Handler to Bus to listen.
        @type  bus_handler: integer
        @return:             Status, whether Bus is listened successfully.
        @rtype:              integer
        '''
        return self.bus_back_.bus_listen(bus_handler)

    def receive(self, bus_handler):
        '''@summary:            Read the data from Bus.
                             Create a datagram.
        @param bus_handler: Handler to Bus to read data from.
        @type  bus_handler: integer
        @return:             An object with the obtained data. Null-able.
        @rtype:              Datagram
        '''
        ph_files = POINTER(c_int)()
        pp_metadata = (c_char_p)()
        pp_params = (c_char_p)()
        pn_files = (c_int)()
        pn_metadata = (c_int)()
        pn_params = (c_int)()

        # Invoke C function
        n_status = self.bus_back_.bus_recv_msg(bus_handler,
                                               ph_files,
                                               pn_files,
                                               pp_metadata,
                                               pn_metadata,
                                               pp_params,
                                               pn_params)
        result_dtg = None
        if 0 <= n_status:
            # The invocation was successful.
            # De-serialize the data

            # Aggregate file descriptors
            n_files = pn_files.value
            h_files = []
            for i in range(n_files):
                h_files.append(ph_files[i])

            # Extract Python strings
            n_metadata = pn_metadata.value
            str_metadata = pp_metadata.value
            n_params = pn_params.value
            str_params = pp_params.value

            # Trim the junk out
            if 0 < n_metadata:
                str_metadata = str_metadata[0:n_metadata]
            str_params = str_params[0:n_params]

            # Construct actual result datagram
            result_dtg = Datagram()
            result_dtg.from_raw_data(h_files,
                                     str_metadata,
                                     str_params)
        return result_dtg

    @staticmethod
    def send(bus_name, datagram):
        '''@summary:         Send the datagram through Bus.
                          Serialize dictionaries into JSON strings.
        @param bus_name:  Path to domain socket "file".
        @type  bus_name:  string
        @param datagram:  The object to send
        @type  datagram:  Datagram
        @return:          Status of the operation
        @rtype:           integer
        '''

        # Serialize the datagram into JSON strings and C integer array
        str_json_params = datagram.get_params_and_cmd_as_json()
        p_params = c_char_p(str_json_params.encode('utf-8'))
        n_params = c_int(len(str_json_params))

        n_files = c_int(0)
        h_files = None
        n_metadata = c_int(0)
        p_metadata = None

        if datagram.get_num_files() > 0:
            str_json_metadata = datagram.get_files_metadata_as_json()
            p_metadata = c_char_p(str_json_metadata.encode('utf-8'))
            n_metadata = c_int(len(str_json_metadata))

            n_fds = datagram.get_num_files()
            n_files = c_int(n_fds)

            file_fds = datagram.get_files()
            h_files = (c_int * n_fds)()

            for i in range(n_fds):
                h_files[i] = file_fds[i]

        # Invoke C function
        bus = Bus()
        n_status = bus.bus_back_.bus_send_msg(bus_name.encode('utf-8'),
                                              h_files,
                                              n_files,
                                              p_metadata,
                                              n_metadata,
                                              p_params,
                                              n_params)
        return n_status
