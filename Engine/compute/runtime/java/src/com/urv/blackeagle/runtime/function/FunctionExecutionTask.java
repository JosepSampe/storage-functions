package com.urv.blackeagle.runtime.function;

import com.ibm.storlet.sbus.SBusDatagram;
import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;

import java.io.FileDescriptor;
import java.util.HashMap;
import java.util.Map;


public class FunctionExecutionTask implements Runnable {
	private Logger logger_ = null;
	private SBusDatagram dtg = null;

	/*------------------------------------------------------------------------
	 * CTOR
	 * */
	public FunctionExecutionTask(SBusDatagram dtg, Logger logger) {
		this.logger_ = logger;
		this.dtg = dtg;
		
		logger_.trace("Function execution task created");
	}

	/*------------------------------------------------------------------------
	 * run
	 * 
	 * Actual function invocation
	 * */
	@SuppressWarnings("unchecked")
	public void run() {
		
		int nFiles = dtg.getNFiles();

		FileDescriptor inputStreamFd = null;
		FileDescriptor outputStreamFd = null;
		FileDescriptor commandFd = null;
		FileDescriptor logStream  = null;
		
		Map<String, String> object_md = null;
		Map<String, String> req_md = null;
		
		String functionName, functionMainClass, functionDependencies = null;
		Api api = null;
		Context ctx = null;
		Function f = null;
		
		HashMap<String, String>[] filesMD = dtg.getFilesMetadata();
		logger_.trace("Got " + nFiles + " fds");

		for (int i = 0; i < nFiles; ++i) {	
			String strFDtype = filesMD[i].get("type");
			
			if (strFDtype.equals("SBUS_FD_INPUT_OBJECT")) {
				inputStreamFd = dtg.getFiles()[i];
				JSONObject jsonMetadata;
				try {
					jsonMetadata = (JSONObject)new JSONParser().parse(filesMD[i].get("json_md"));
					object_md = (Map<String, String>) jsonMetadata.get("object_md");
					req_md = (Map<String, String>) jsonMetadata.get("req_md");
				} catch (ParseException e) {
					e.printStackTrace();
				}
				jsonMetadata = null;
				logger_.trace("Got object input stream and request metadata");
				
			} else if (strFDtype.equals("SBUS_FD_OUTPUT_OBJECT")){
				outputStreamFd = dtg.getFiles()[i];
				logger_.trace("Got object output stream");
	
			} else if (strFDtype.equals("SBUS_FD_OUTPUT_OBJECT_METADATA")) {
				commandFd = dtg.getFiles()[i];
				logger_.trace("Got Function command stream");
				
			} else if (strFDtype.equals("SBUS_FD_LOGGER")){
				logStream = dtg.getFiles()[i];
				functionName = filesMD[i].get("function");
				functionMainClass = filesMD[i].get("main");
				functionDependencies = filesMD[i].get("dependencies");
				logger_.trace("Got "+functionName);
				
				api = new Api(req_md, logger_);
				ctx = new Context(inputStreamFd, outputStreamFd, functionName, logStream, commandFd, object_md, req_md, logger_, api.swift);
				f = new Function(functionName, functionMainClass, functionDependencies, logger_);

				logger_.trace("Function '"+functionName+"' loaded");
				IFunction function = f.getFunction();
				logger_.trace("START: Going to execute '"+functionName+"' function");
				function.invoke(ctx, api);
				ctx.request.forward();
				ctx.object.stream.close();
				ctx.object.metadata.flush();
				api.swift.close();
				logger_.trace("END: Function '"+functionName+"' executed");
				
				f = null;
				api = null;
				ctx = null;
				function = null;
				filesMD = null;
			}
		}
	}
}