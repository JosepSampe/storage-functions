package com.urv.blackeagle.function.reducer;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;
import com.urv.blackeagle.runtime.function.IFunction;

public class Handler implements IFunction {
	
	/***
	 * function entry method. 
	 */
	public void invoke(Context ctx, Api api) {

		ctx.log.emit("Init Reducer Function");

		String data;
		String manifest = "";
		
		while((data = ctx.object.stream.read()) != null) {
			manifest = manifest + data;
		}
		
		try {			
			JSONArray jsonManifest = (JSONArray) new JSONParser().parse(manifest);
			for(int i = 0; i < jsonManifest.size(); i++){
				 JSONObject jsonObj = (JSONObject) jsonManifest.get(i);
				 String obj = (String) jsonObj.get("name");
				 ctx.log.emit(obj); 
			}

		} catch (ParseException e) {
			ctx.log.emit("Reducer function - raised IOException: " + e.getMessage());
		}


		ctx.object.stream.write("Processing...\n");
		ctx.log.emit("Ended Reducer Function");

	}
	
}
