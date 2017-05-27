package com.urv.blackeagle.function.cbac;

import java.util.HashMap;

import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;
import com.urv.blackeagle.runtime.function.IFunction;

public class Handler implements IFunction {
	
	private  HashMap<String, Integer> header_;
	
    public Handler()
    {
        this.header_ = new HashMap<>();
        loadHeader();
    }
	
    private void loadHeader()
    {
        String header[] = {
            "ssn", "age", "workclass", "fnlwgt", "education", "education-num", "marital-status", 
            "occupation", "relationship", "race", "sex", "capital-gain", "capital-loss", 
            "hours-per-week", "native-country", "prediction"
        };
        
        for(int i = 0; i < header.length; i++)
        	this.header_.put(header[i], i);

    }
	
	private void filterData(Context ctx, String allowed_cols){
		String in_line, out_line = "";
		String[] split_line;
		String[] select = allowed_cols.split(",");
		
		while((in_line = ctx.object.stream.readLine()) != null) {
			
			if (select[0].equals("*")){
				ctx.object.stream.write(in_line);
            } else { 
				split_line = in_line.split(",");
				if (split_line.length < header_.size()) continue;
	                for(int l = 0; l < select.length; l++){
	                	if (l==0){
	                		out_line = split_line[header_.get(select[l])];
	                	} else {
	                		out_line = out_line+","+split_line[header_.get(select[l])];
	                	}
	                }
	                ctx.object.stream.write(out_line+"\n");   
            }
		}		
	}
	
	/***
	 * Function entry method. 
	 */
	public void invoke(Context ctx, Api api) {
		
		ctx.log.emit("Init CBAC Function");
		
		String requetRoles = ctx.request.headers.get("X-Roles");

		String role = ctx.function.parameters.get("role").toString();
		String allowed_cols = ctx.function.parameters.get("allowed_cols").toString();
		
		ctx.log.emit("User roles: "+requetRoles);
		ctx.log.emit("Role: "+role+", Allowed columns: "+allowed_cols); 
		
		if (requetRoles.toLowerCase().contains(role)){
			ctx.log.emit("--> Allowed request");
			filterData(ctx, allowed_cols);
		} else {
			ctx.log.emit("--> Unallowed request");
			ctx.request.cancel("ERROR: User not allowed");	
		}

		ctx.log.emit("Ended CBAC Function");
		
	}

}
