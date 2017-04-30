package com.urv.blackeagle.function.reducer;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.Response;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;


import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;
import com.urv.blackeagle.runtime.function.IFunction;

public class Handler implements IFunction {
	
	/***
	 * function entry method. 
	 */
	public void invoke(Context ctx, Api api) {

		ctx.log.emit("Init Reducer Function");
		
		AsyncHttpClient asyncHttpClient = new DefaultAsyncHttpClient();
		ArrayList<Future<Response>> futures = new ArrayList<>();
		String token = ctx.request.headers.get("X-Auth-Token");
		String data;
		String url;
		String manifest = "";

		// 1. Load the manifest file
		ctx.log.emit("Loading manifest.");
		while((data = ctx.object.stream.read()) != null) {
			manifest = manifest + data;
		}
		
		// 2. Convert manifest to json object, and request the resources
		ctx.log.emit("Parsing manifest and requesting object chunks.");
		try {			
			JSONArray jsonManifest = (JSONArray) new JSONParser().parse(manifest);
			for(int i = 0; i < jsonManifest.size(); i++){
				JSONObject jsonObj = (JSONObject) jsonManifest.get(i);
				String obj = (String) jsonObj.get("name");
				url = "http://192.168.2.1:8080/v1/AUTH_bd34c4073b65426894545b36f0d8dcce" + obj;
				ctx.log.emit(url);
				futures.add(asyncHttpClient.prepareGet(url).addHeader("X-Auth-Token", token).execute());	 
			}
		} catch (ParseException e) {
			ctx.log.emit("Reducer function - raised IOException: " + e.getMessage());
		}

		// 3. Wait for all responses
		ctx.log.emit("Collecting http responses. Blocking.");
		ArrayList<Response> responses = futures.stream().map(f -> {
            try {
                return f.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return null;
            }
        }).collect(Collectors.toCollection(ArrayList::new));
        
        
        // 4. Operate over responses
        ctx.log.emit("Responses collected. Resuming Function.");
        int i = 1;
        int total = 0;
        String result = null;
        for (Response r : responses) {
        	ctx.log.emit(String.format("\nResponse %d:", i++));
            if (r != null){
            	result = r.getResponseBody();
            	total +=  Integer.parseInt(result);
            	ctx.log.emit(r.getResponseBody());
            }
            else ctx.log.emit("Error: null request");
        }
		
        // 5. Close async client
        try {
            asyncHttpClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
		
		 
		ctx.object.stream.write("Total GET requests in the ubuntu-one trace sample: "+total+"\n");
		ctx.log.emit("Ended Reducer Function");

	}
	
}
