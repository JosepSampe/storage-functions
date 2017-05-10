package com.urv.blackeagle.function.uone;

import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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
		
		String sql = "select user_id, count(*) where (req_t='GetContentResponse' or req_t='PutContentResponse') and msg='Request done' group by user_id";
		
		String in_line;
		String[] split_line;
		Integer total = 0;
		Map<String, Integer> userDict = new HashMap<>();
		JSONObject outData = new JSONObject();
		
		ctx.log.emit("Init Ubuntu one Trace Filter Function");
		
		while((in_line = ctx.object.stream.readLine()) != null) {
			split_line = in_line.split(",");
			if (split_line.length < header_.size()) continue;
			if((split_line[header_.get("req_t")].equals("GetContentResponse") || 
					split_line[header_.get("req_t")].equals("PutContentResponse")) &&
					split_line[header_.get("msg")].equals("Request done")){
            	total += 1;
				String userId = split_line[header_.get("user_id")];
				if (userDict.containsKey(userId)){
					userDict.put(userId, userDict.get(userId) + 1);
				} else {
					userDict.put(userId, 1);
				}
            }
		}
		
		//for (Map.Entry<String, Integer> entry : userDict.entrySet())
		//	outData.put(entry.getKey(), entry.getValue());
		
		try {
			outData = (JSONObject) new JSONParser().parse(JSONObject.toJSONString(userDict));
		} catch (ParseException e) {
			ctx.log.emit("Error while converting MAP to a JSON object");
		}

		ctx.object.stream.write(outData.toString());
		
		ctx.log.emit("Total lines: "+Integer.toString(total));
		ctx.log.emit("Ended Ubuntu one Trace Filter Function");
	}

}
