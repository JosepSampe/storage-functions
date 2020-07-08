package com.urv.zion.function.counter;

import java.text.SimpleDateFormat;
import com.urv.zion.runtime.api.Api;
import com.urv.zion.runtime.context.Context;
import com.urv.zion.runtime.function.IFunction;

public class Handler implements IFunction {
	
	/***
	 * function entry method. 
	 */
	public void invoke(Context ctx, Api api) {
		
		ctx.log.emit("Init access-counter Function");
		
		java.util.Date date = new java.util.Date();
		SimpleDateFormat formater = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zz");
		String strDate = formater.format(date);

		ctx.object.metadata.incr("Accessed");
		ctx.object.metadata.set("Last-Access", strDate);

		ctx.request.forward();

		ctx.log.emit("Ended access-counter Function");
	}	
}
