package com.urv.blackeagle.runtime.api;

import org.slf4j.Logger;
import java.util.Map;


public class Api {
	
	private Logger logger_;
	public Swift swift;


	public Api(Map<String, String> reqMd, Logger localLog) 
	{	
		String projectId = reqMd.get("X-Project-Id");
		String token = reqMd.get("X-Auth-Token");
		
		logger_ = localLog;
		swift = new Swift(token, projectId, logger_);
		logger_.trace("Full API created");
	}
	
}