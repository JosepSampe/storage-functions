package com.urv.zion.bus;

import java.io.FileDescriptor;

/*----------------------------------------------------------------------------
 * RawMessage
 * 
 * This class aggregates the data which is sent through Bus.
 * No logic is implemented here. 
 * */

public class BusRawMessage 
{
	/*------------------------------------------------------------------------
	 * Data Fields
	 * */
	
	// Array of open file descriptors (FDs)
	private FileDescriptor[] hFiles_;
	
	// JSON-encoded string describing the FDs
	private String strMetadata_;
	
	// JSON-encoded string with additional information 
	// for micro-controller execution 
	private String strParams_;

	/*------------------------------------------------------------------------
	 * Default CTOR
	 * */
	public BusRawMessage()
	{
		hFiles_      = null;
		strMetadata_ = null;
		strParams_   = null;
	}

	/*------------------------------------------------------------------------
	 * Setters/getters
	 * */
	public FileDescriptor[] getFiles()
	{
		return hFiles_;
	}

	public void setFiles( FileDescriptor[] hFiles )
	{
		this.hFiles_ = hFiles;
	}

	public String getMetadata()
	{
		return strMetadata_;
	}

	public void setMetadata( String strMetadata )
	{
		this.strMetadata_ = strMetadata;
	}

	public String getParams()
	{
		return strParams_;
	}

	public void setParams( String strParams ) 
	{
		this.strParams_ = strParams;
	}
}
