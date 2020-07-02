package com.urv.zion.bus;

/*----------------------------------------------------------------------------
 * This class encapsulates OS level file descriptor used 
 * in Transport Layer APIs. 
 * */

public class BusHandler 
{
	private int nFD_;

	/*------------------------------------------------------------------------
	 * CTOR
	 * No default value
	 * */
	public BusHandler( int nFD )
	{
		nFD_ = nFD;
	}

	/*------------------------------------------------------------------------
	 * Getter
	 * */
	public int getFD()
	{
		return nFD_;
	}
	
	/*------------------------------------------------------------------------
     * Validity
     * */
    public boolean isValid()
    {
        return (0 <= getFD());
    }

}
