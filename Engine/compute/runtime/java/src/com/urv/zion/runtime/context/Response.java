package com.urv.zion.runtime.context;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;


public class Response {
	private Logger logger_;
	public Headers headers;

	public Response(Logger logger) {

		headers = new Headers();
		logger_ = logger;

		logger_.trace("CTX Response created");
	}
	
	public class Headers { 
		private boolean dirtyBit = false;
		private Map<String, String> headers = new HashMap<String, String>();
		 
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