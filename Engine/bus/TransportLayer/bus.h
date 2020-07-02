#ifndef schannel_h
#define schannel_h

/*======================= API declarations =================================*/

/*----------------------------------------------------------------------------
 * start/halt logging
 */
extern void bus_start_logger( const char* str_log_level,  const char* container_id);
extern void bus_stop_logger(  void );

/*----------------------------------------------------------------------------
 * sbus_create
 * create an instance of SBus object
 * returns -1 on error, SBus handler on success
 */
extern int bus_create( const char* str_bus_path );

/*----------------------------------------------------------------------------
 * sbus_listen
 * Suspend the
 * returns -1 on error, 0 on success
 */
extern int bus_listen( int n_bus_handle );

/*----------------------------------------------------------------------------
 * sbus_recv_msg
 * reads the data, allocates memory for the necessary buffers
 * See sbus_send_msg for the extended description of arguments
 *
 * Caller shall clean up the buffers to avoid memory leaks
 */
extern int bus_recv_msg( int    n_bus_handler,
                         int**  pp_files,
                         int*   pn_files,
                         char** pstr_files_metadata,
                         int*   pn_files_metadata_len,
                         char** pstr_msg_data,
                         int*   pn_msg_len );

/*----------------------------------------------------------------------------
 * sbus_send_msg
 * sends the message in predefined format
 *
 * arguments:
 * str_sbus_path        - SBus path
 * p_files              - array of actual system file descriptors, and...
 * n_files              - its length
 * str_files_metadata   - JSON-encoded string with the metadata
 *                        for the previous array, and
 * n_files_metadata_len - string length
 * str_msg_data         - the message, JSON-encoded string
 * n_msg_len            - length of the above
 */
extern int bus_send_msg( const char* str_bus_path,
                         const int*  p_files,
                         int         n_files,
                         const char* str_files_metadata,
                         int         n_files_metadata_len,
                         const char* str_msg_data,
                         int         n_msg_len );

#endif
