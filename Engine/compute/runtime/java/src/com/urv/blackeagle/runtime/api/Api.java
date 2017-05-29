package com.urv.blackeagle.runtime.api;

import org.slf4j.Logger;
import java.util.Map;


public class Api {
	
	private Logger logger_;
	public Swift swift;


	public Api(Map<String, String> request_headers, Logger localLog) 
	{	
		String projectId = request_headers.get("X-Project-Id");
		String token = null;
		if (request_headers.containsKey("X-Auth-Token"))
			token = request_headers.get("X-Auth-Token");
		
		logger_ = localLog;
		swift = new Swift(token, projectId, logger_);
		logger_.info("Full API created");
	}
	
	public void close(){
		swift.close();
	}
}