package com.urv.blackeagle.function.limiter;

import java.text.SimpleDateFormat;
import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;
import com.urv.blackeagle.runtime.function.IFunction;

public class Handler implements IFunction {
	
	/***
	 * function entry method. 
	 */
	public void invoke(Context ctx, Api api) {
		java.util.Date date = new java.util.Date();
		SimpleDateFormat formater = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zz");
		String strDate = formater.format(date);

		Long accessed = ctx.object.metadata.incr("Accessed");
		ctx.object.metadata.set("Last-Access", strDate);

		if (accessed > 10000){
			ctx.request.cancel("Error: maximum reads reached.");
		} else {
			ctx.request.forward();
		}
	}	
}
