package com.urv.blackeagle.function.noopdataiterator;

import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;
import com.urv.blackeagle.runtime.function.IFunction;

public class Handler implements IFunction {
	
	/***
	 * function entry method. 
	 */
	public void invoke(Context ctx, Api api) {

		ctx.log.emit("Init Noop Data Iterator Function");

		String data;
		
		while((data = ctx.object.stream.read()) != null) {
			ctx.object.stream.write(data);
		}

		ctx.log.emit("Ended Noop Data Iterator Function");

	}
	
}
