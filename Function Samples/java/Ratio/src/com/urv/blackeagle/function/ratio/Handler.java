package com.urv.blackeagle.function.ratio;

import java.util.Arrays;
import java.util.List;
import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;
import com.urv.blackeagle.runtime.function.IFunction;

public class Handler implements IFunction {
	
	/***
	 * Function entry method. 
	 */
	public void invoke(Context ctx, Api api) {
		ctx.logger.emitLog("Init Access Ratio Calculator Function");

		ctx.request.forward(); // Return request to the user; the rest of code will be executed asynchronously
		
		Integer ts = (int) (System.currentTimeMillis() / 1000);
		String accesses = ctx.object.metadata.get("Accesses");
		Integer time = 10;  // 10 minutes
		Integer threshold = 5000;  // 5000 requests
		Integer from = ts-(time*60);
		Integer to = ts+(time*60);
		Integer count = 0;
		Integer intTstamp;
		double ratio;
			
		if (accesses == null){
			ctx.object.metadata.set("Accesses", String.valueOf(ts));
			ratio = 1/threshold;
		}else{
			accesses = accesses+","+String.valueOf(ts);
			//api.object.metadata.set("Accesses", accesses);

			List<String> accessesList = Arrays.asList(accesses.split(","));
			
			for (String tstamp : accessesList){
					intTstamp = Integer.parseInt(tstamp);
					if (intTstamp > from && intTstamp < to)
						count+=1;
			}
			ratio = (double) count/threshold;
		}
		
		if (ratio > 1)
			ctx.object.move("data_2/new_file.sh");
		
		ctx.logger.emitLog("Current Ratio: "+Double.toString(ratio));
		ctx.logger.emitLog("Ended Access Ratio Calculator Function");
	}
}