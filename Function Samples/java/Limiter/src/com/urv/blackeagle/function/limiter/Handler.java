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
		
		String max_reads = ctx.function.parameters.get("max_reads");
		
		java.util.Date date = new java.util.Date();
		SimpleDateFormat formater = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zz");
		String strDate = formater.format(date);

		Long accessed = ctx.object.metadata.incr("Accessed");
		ctx.object.metadata.set("Last-Access", strDate);

		if (accessed > Integer.parseInt(max_reads)){
			ctx.request.cancel("Error: maximum reads reached.");
		} else {
			ctx.request.forward();
		}
	}	
}
