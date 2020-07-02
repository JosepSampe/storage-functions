package com.urv.zion.function.prefetching;

import java.util.Arrays;
import java.util.List;
import com.urv.zion.runtime.api.Api;
import com.urv.zion.runtime.context.Context;
import com.urv.zion.runtime.function.IFunction;

public class Handler implements IFunction {
	
	/***
	 * Function entry method. 
	 */
	public void invoke(Context ctx, Api api) {
		ctx.log.emit("Init Prefetching Function");

		String resources = ctx.object.metadata.get("Resources");
		String container = ctx.request.headers.get("X-Container");
		String currentLocation = ctx.request.headers.get("X-Current-Location");
				
		if (resources != null){
			List<String> staticResources = Arrays.asList(resources.split(","));
			String link = "";
			
			for (String resource : staticResources){
				link = link + "<"+currentLocation+"/"+resource+">;rel=preload,";
				ctx.log.emit("<"+currentLocation+"/"+resource+">;rel=preload;");
				
			}
			ctx.response.headers.set("Link", link);
			/*
			ctx.request.forward();
			
			for (String resource : staticResources){
				api.swift.prefetch(container+"/"+resource);
			}*/
		}

		ctx.log.emit("Ended Prefetching Function");
	}
}