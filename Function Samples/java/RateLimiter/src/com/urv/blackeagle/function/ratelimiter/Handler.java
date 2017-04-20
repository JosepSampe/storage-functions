package com.urv.blackeagle.function.ratelimiter;

import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;
import com.urv.blackeagle.runtime.function.IFunction;
import com.google.common.util.concurrent.RateLimiter;

public class Handler implements IFunction {
	
    final double ONE_MB = 1024*1024;
    final RateLimiter limiter = RateLimiter.create(10*ONE_MB);
    
	/***
	 * function entry method. 
	 */
	public void invoke(Context ctx, Api api) {
		String data;
		
		while((data = ctx.object.stream.read()) != null) {
			limiter.acquire(data.length());
			ctx.object.stream.write(data);
		}
	}
	
}
