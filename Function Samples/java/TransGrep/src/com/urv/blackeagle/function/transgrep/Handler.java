package com.urv.blackeagle.function.transgrep;

import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;
import com.urv.blackeagle.runtime.function.IFunction;

public class Handler implements IFunction {
	
	/***
	 * Function entry method. 
	 */
	public void invoke(Context ctx, Api api) {
		ctx.log.emit("Init TransGrep Function");

		ctx.storlet.set("transcoder-1.0.jar", null);
		ctx.storlet.set("grep-1.0.jar","regexp=*^a*");
		ctx.storlet.run();

		ctx.log.emit("Ended TransGrep Function");
	}
	
}
