package com.urv.blackeagle.runtime.context;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import org.json.simple.JSONObject;
import org.slf4j.Logger;


public class Storlet {
	private FileOutputStream command;
	private Logger logger_;
	private Integer index;
	private JSONObject outMetadata = new JSONObject();
	private JSONObject storletList = new JSONObject();
	
	public Storlet(FileDescriptor commandFd, Logger logger) {
		command = new FileOutputStream(commandFd);
		logger_ = logger;
		index = 0;
		logger_.trace("CTX Storlet created");
	}

	@SuppressWarnings("unchecked") 
	public void set(String storlet, String parameters){
		JSONObject storletPack = new JSONObject();
		storletPack.put("storlet",storlet);
		storletPack.put("params",parameters);
		storletPack.put("server","object");		
		storletList.put(index,storletPack);
		index = index+1;
	}
	
	@SuppressWarnings("unchecked") 
	public void run() {
		try {
			if (storletList.isEmpty()){
				outMetadata.put("command","CONTINUE");
			} else {
				outMetadata.put("command","STORLET");
				outMetadata.put("list",storletList);
			}
			command.write(outMetadata.toString().getBytes());
			this.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	private void flush() {
		try {
			command.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}