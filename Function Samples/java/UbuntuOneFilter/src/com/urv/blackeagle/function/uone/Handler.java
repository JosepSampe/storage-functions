package com.urv.blackeagle.function.uone;

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
                "t", "addr", "caps", "client_metadata", "current_gen", "ext", "failed", "free_bytes", "from_gen", "hash", 
                "level", "logfile_id", "method", "mime", "msg", "node_id", "nodes", "pid", "req_id", "req_t", 
                "root", "server", "shared_by", "shared_to", "shares", "sid", "size", "time", "tstamp", "type", 
                "udfs", "user_id", "user", "vol_id"
         };
        
        for(int i = 0; i < header.length; i++)
        	this.header_.put(header[i], i);

    }
	
	/***
	 * Function entry method. 
	 */
	public void invoke(Context ctx, Api api) {
		
		ctx.log.emit("Init Ubuntu one Trace Filter Function");
		
		String sql = "select count(*) where req_t='GetContentResponse'";
		
		String in_line;
		String[] split_line;
		Integer total = 0;
		
		ctx.log.emit("Init Ubuntu one Trace Filter Function");
		
		while((in_line = ctx.object.stream.readLine()) != null) {
			split_line = in_line.split(",");
			if (split_line.length < header_.size()) continue;
			if(split_line[header_.get("req_t")].equals("GetContentResponse")){
            	total += 1;
            }
		}
		
		ctx.object.stream.write(Integer.toString(total));
		
		ctx.log.emit("Total lines: "+Integer.toString(total));
		ctx.log.emit("Ended Ubuntu one Trace Filter Function");
	}

}
