package com.urv.zion.function.none;

import com.urv.zion.runtime.api.Api;
import com.urv.zion.runtime.context.Context;
import com.urv.zion.runtime.function.IFunction;

public class Handler implements IFunction {
	
	/***
	 * function entry method. 
	 */
	public void invoke(Context ctx, Api api) {
		ctx.request.forward();
	}
	
}
