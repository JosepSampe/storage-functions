package com.urv.blackeagle.runtime.context;

import java.io.FileDescriptor;
import org.slf4j.Logger;

import com.urv.blackeagle.runtime.api.Swift;

import java.util.Map;


public class Context {
	
	private Logger logger_;	
	public Log log;
	public Response response;
	public Request request;
	public Object object;
	public Function function;
	public Storlet storlet;


	public Context(FileDescriptor inputStreamFd, FileDescriptor outputStreamFd, String functionName, 
				   Map<String, String> functionParameters, FileDescriptor logFd, FileDescriptor commandFd, 
				   Map<String, String> objectMd, Map<String, String> reqMd, Logger localLog, Swift swift) 
	{	
		String currentObject = reqMd.get("X-Container")+"/"+reqMd.get("X-Object");
		
		logger_ = localLog;
		log = new Log(logFd, logger_);
		storlet = new Storlet(commandFd, logger_);
		function = new Function(functionParameters, logger_);
		response = new Response(logger_);
		request = new Request(commandFd, reqMd, response, logger_);
		object = new Object(inputStreamFd, outputStreamFd, commandFd, objectMd, currentObject, request, response, swift, logger_);
		
		request.setObjectCtx(object);

		logger_.trace("Full Context created");
	}
	
}