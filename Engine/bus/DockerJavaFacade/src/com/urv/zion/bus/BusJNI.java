package com.urv.zion.bus;

/*----------------------------------------------------------------------------
 * JNI wrapper for low-level C API
 * 
 * Just declarations here.
 * See BusJNI.c for the implementation
 * */
public class BusJNI 
{
	static 
	{
		System.loadLibrary("jbus");
	}

	public native void startLogger(   final String         strLogLevel, final String contId );
	public native void stopLogger();
	public native int createBus(     final String         strBusName  );
	public native int listenBus(     int                  nBus        );
	public native int sendRawMessage( final String         strBusName,
                                      final BusRawMessage Msg         );
	public native BusRawMessage receiveRawMessage( int    nBus        );
}
