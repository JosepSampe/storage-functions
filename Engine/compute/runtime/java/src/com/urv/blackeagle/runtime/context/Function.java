package com.urv.blackeagle.runtime.context;

import java.util.Map;
import org.slf4j.Logger;

public class Function {
	public Map<String, String> parameters;
	private Logger logger_;

	public Function(Map<String, String> params, Logger logger) {
		parameters = params;
		logger_ = logger;
		
		logger_.trace("CTX Function created");
	}

}