package com.urv.zion.bus;

import java.io.IOException;

/*----------------------------------------------------------------------------
 * Backend
 * 
 * This class wraps and transfers calls to the JNI implementation 
 * */
public class BusBackend 
{
	/*------------------------------------------------------------------------
	 * JNI layer delegate, common to every instance of BusBackend
	 * */
	private static BusJNI BusJNIObj_  = new BusJNI();
	
	/*------------------------------------------------------------------------
	 * Enumerating logging levels
	 * The values are suitable to syslog constants
	 * */
	public static enum eLogLevel
	{
		BUS_LOG_DEBUG,
		BUS_LOG_INFO,
		BUS_LOG_WARNING,
		BUS_LOG_CRITICAL,
		BUS_LOG_OFF
	};
		
	/*------------------------------------------------------------------------
	 * Initiate logging with the required detail level 
	 * */
	public void startLogger( eLogLevel eLogLevel, String contId )
	{
		String strLogLevel = null;
		switch( eLogLevel )
		{
		case BUS_LOG_DEBUG:
			strLogLevel = "DEBUG";
			break;
		case BUS_LOG_INFO:
			strLogLevel = "INFO";
			break;
		case BUS_LOG_WARNING:
			strLogLevel = "WARNING";
			break;
		case BUS_LOG_CRITICAL:
			strLogLevel = "CRITICAL";
			break;
		case BUS_LOG_OFF:
			strLogLevel = "OFF";
			break;
		default:
			strLogLevel = "WARNINIG";
			break;
		}
		BusJNIObj_.startLogger(strLogLevel, contId);
	}
	
	/*------------------------------------------------------------------------
	 * Stop logging 
	 * */
	public void stopLogger()
	{
		BusJNIObj_.stopLogger();
	}
	
	/*------------------------------------------------------------------------
	 * Create the bus. 
	 * */
	public BusHandler createBus( final String strBusName ) 
			                                                throws IOException
	{
		int nBus = BusJNIObj_.createBus( strBusName );
		if( 0 > nBus )
			throw new IOException( "Unable to create Bus - " + strBusName );
		return new BusHandler( nBus );
	}
	
	/*------------------------------------------------------------------------
	 * Wait and listen to the bus.
	 * The executing thread is suspended until some data arrives. 
	 * */
	public boolean listenBus( final BusHandler hBus ) 
			                                                throws IOException
	{
		int nStatus = BusJNIObj_.listenBus( hBus.getFD() );
		if( 0 > nStatus )
			throw new IOException( "Unable to listen to Bus" );
		return true;
	}
	
	/*------------------------------------------------------------------------
	 * Take the message and send it.
	 * */
	public int sendRawMessage( final String 		strBusName, 
			                   final BusRawMessage Msg ) 
			                		                       throws IOException
	{
		int nStatus = BusJNIObj_.sendRawMessage(strBusName, Msg );
		if( 0 > nStatus )
			throw new IOException( "Unable to send message" );
		return nStatus;
	}
	
	/*------------------------------------------------------------------------
	 * Read some actual raw data from the bus
	 * */
	public BusRawMessage receiveRawMessage( final BusHandler hBus )
	                                                        throws IOException
	{
		BusRawMessage Msg = BusJNIObj_.receiveRawMessage( hBus.getFD() );
		if( null == Msg )
			throw new IOException( "Unable to retrieve a message" );
		return Msg;
	}
	
}
