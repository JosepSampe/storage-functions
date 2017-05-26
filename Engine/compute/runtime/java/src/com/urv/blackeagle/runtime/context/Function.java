package com.urv.blackeagle.runtime.context;

import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;

import com.urv.blackeagle.runtime.api.Swift;


public class Function {
	private String name;
	private String method;
	private Logger logger_;
	private String object;
	private Swift swift;
	
	public JSONObject metadata = new JSONObject();

	public Function(Map<String, String> objectMetadata, String mcName, String currentObject, 
							  String requestMethod, Swift apiSwift, Logger logger) {
		name = mcName;
		method = requestMethod;
		logger_ = logger;
		object = currentObject;
		swift = apiSwift;
		
		String metadataKey = "x-object-sysmeta-function-on"+method+"-"+name;
		for (String key: objectMetadata.keySet()){
			if (key.toLowerCase().equals(metadataKey)){
				String functionMetadata = objectMetadata.get(key);				
				if (functionMetadata != null) {
					try{
						metadata = (JSONObject) new JSONParser().parse(functionMetadata);
					} catch (ParseException e) {
						logger_.trace("Error parsing function metadata: "+e.getMessage());
					}
				}
			}
		}
		
		logger_.trace("CTX Function created");
	}
	
	public void updateMetadata(){
		logger_.trace("Updating function metadata");
		swift.setFunction(object, name, method, metadata.toString());
	}
}