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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
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
        
        
		Map<String, Integer> userDict = new HashMap<>();
		
        // 4. Operate over responses
        ctx.log.emit("Responses collected. Resuming Function.");
        int i = 1;
        String result = null;
        for (Response r : responses) {
        	ctx.log.emit(String.format("\nResponse %d:", i++));
            if (r != null){
            	try {
            		result = r.getResponseBody();
            		JSONObject jsonResult = (JSONObject) new JSONParser().parse(result);
            		 
            		ctx.log.emit("Response recived, parsing");
            		for (Object key : jsonResult.keySet()) {
            		        String userId = (String)key;
            		        int value = Integer.parseInt(jsonResult.get(userId).toString());
            		        if (userDict.containsKey(userId)){
            					userDict.put(userId, userDict.get(userId) + value);
            				} else {
            					userDict.put(userId, value);
            				}
            		 }
				} catch (ParseException e) {
					 ctx.log.emit("Error parsing the response");
				}	
            }
            else ctx.log.emit("Error: null request");
        }
		
        // 5. Close async client
        try {
            asyncHttpClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        Map<String, Integer> topTen = sortByValue(userDict);
        
		ctx.object.stream.write(topTen.toString());

		ctx.log.emit("Ended Reducer Function");

	}
	
	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
	    return map.entrySet()
	              .stream()
	              .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
	              .limit(10)
	              .collect(Collectors.toMap(
	                Map.Entry::getKey, 
	                Map.Entry::getValue, 
	                (e1, e2) -> e1, 
	                LinkedHashMap::new
	              ));
	}
	
}
