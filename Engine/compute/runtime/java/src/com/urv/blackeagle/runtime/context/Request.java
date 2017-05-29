package com.urv.blackeagle.runtime.context;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.json.simple.JSONObject;
import org.slf4j.Logger;


public class Request {
	private FileOutputStream command;
	private JSONObject outMetadata = new JSONObject();
	private Object object;
	private Response response;
	private Logger logger_;
	public Headers headers;
	public boolean command_sent = false;

	public Request(FileDescriptor commandFd, Map<String, String> requestHeaders, Response resp, Logger logger) {
		command = new FileOutputStream(commandFd);
		headers = new Headers(requestHeaders);
		response = resp;
		logger_ = logger;

		logger_.trace("CTX Request created");
	}
	
	public void setObjectCtx(Object obj){
		object = obj;
	}
	
	@SuppressWarnings("unchecked")
	public void forward(){
		if (!this.command_sent){
			this.command_sent = true;
			logger_.info("Sending command: CONTINUE");
			outMetadata.put("cmd","RC");
			this.sendDataToSwift();
	
			outMetadata.clear();
			// TODO: is it necessary to offload object metadata here?
			if (object.metadata.isModified())
				outMetadata.put("object_metadata", object.metadata.getAll());
			if (response.headers.isModified())
				outMetadata.put("response_headers",response.headers.getAll());
			if (this.headers.isModified())
				outMetadata.put("request_headers", headers);
			this.sendDataToSwift();
		}
	}
	
	@SuppressWarnings("unchecked")
	public void cancel(String message){	
		logger_.trace("Sending command: CANCEL");
		outMetadata.put("cmd", "RE");
		outMetadata.put("message", message);
		this.sendDataToSwift();
	}
	
	@SuppressWarnings("unchecked")
	public void rewire(String object_id){
		logger_.trace("Sending command: REWIRE");
		outMetadata.put("cmd", "RR");
		outMetadata.put("object_id", object_id);
		this.sendDataToSwift();
	}
	
	private void sendDataToSwift() {
		try {
			command.write(outMetadata.toString().getBytes());
			command.flush();
		} catch (IOException e) {
			logger_.trace("Error sending command on ApiRequest");
		}
	}
	
	public class Headers { 
		private boolean dirtyBit = false;
		private Map<String, String> headers;
		
		public Headers(Map<String, String> requestHeaders){
			this.headers = requestHeaders;
		}
		 
		public String get(String key){
			return this.headers.get(key);
		}
		
		public Map<String, String> getAll(){
			return this.headers;
		}

		public void set(String key, String value){
			this.headers.put(key, value);
			dirtyBit = true;
		}

		public void del(String key){
			this.headers.remove(key);
			dirtyBit = true;
		}
		
		public boolean isModified(){
			return this.dirtyBit;
		}
	}
}