package com.urv.zion.bus;

import java.io.IOException;
import com.urv.zion.bus.BusBackend.eLogLevel;

/*----------------------------------------------------------------------------
 * Bus
 * 
 * The front end Java class for Bus functionality.
 * */
public class Bus 
{
    private BusHandler hServerSideBus_;
    private BusBackend BusBack_;
    
    /*------------------------------------------------------------------------
     * CTOR
     * 
     * Instantiate the BusBackend object. Start logging
     * */
    public Bus( final String contId ) throws IOException
    {
        BusBack_ = new BusBackend();
        BusBack_.startLogger( eLogLevel.BUS_LOG_DEBUG, contId );
    }

    /*------------------------------------------------------------------------
     * create
     * 
     * Initialize the server side Bus
     * */
    public void create( final String strPath ) throws IOException 
    {
        hServerSideBus_ = BusBack_.createBus( strPath );
    }

    /*------------------------------------------------------------------------
     * listen
     * 
     * Listen to the Bus. Suspend the executing thread
     * */
    public void listen() throws IOException 
    {
        BusBack_.listenBus(hServerSideBus_);
    }

    /*------------------------------------------------------------------------
     * receive
     * */
    public BusDatagram receive() throws IOException 
    {
        BusRawMessage Msg = BusBack_.receiveRawMessage( hServerSideBus_ );
        BusDatagram Dtg = new BusDatagram( Msg );
        return Dtg;
    }
    
    /*------------------------------------------------------------------------
     * send
     * */
    public void send( final String       strBusPath,
                      final BusDatagram Dtg         ) throws IOException 
    {
        
        BusRawMessage Msg = Dtg.toRawMessage();
        BusBack_.sendRawMessage(strBusPath, Msg);
    }

    /*------------------------------------------------------------------------
     * DTOR
     * 
     * Stop logging
     * */
    public void finalize()
    {
        BusBack_.stopLogger();
    }
}
