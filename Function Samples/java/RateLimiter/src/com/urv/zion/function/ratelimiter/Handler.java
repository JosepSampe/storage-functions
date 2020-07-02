package com.urv.zion.function.ratelimiter;

import com.urv.zion.runtime.api.Api;
import com.urv.zion.runtime.context.Context;
import com.urv.zion.runtime.function.IFunction;
import com.google.common.util.concurrent.RateLimiter;

public class Handler implements IFunction {
	
    final double ONE_MB = 1024*1024;
    final RateLimiter limiter = RateLimiter.create(30*ONE_MB);
    
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
