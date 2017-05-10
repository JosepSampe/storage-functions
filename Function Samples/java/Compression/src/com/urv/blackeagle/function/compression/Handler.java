package com.urv.blackeagle.function.compression;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;
import com.urv.blackeagle.runtime.function.IFunction;

public class Handler implements IFunction {
	
	/***
	 * function entry method. 
	 */
	public void invoke(Context ctx, Api api) {

		ctx.log.emit("Init Compression Function");

		GZIPOutputStream tocompress;
		byte[] buffer;
		
		try {
			tocompress = new GZIPOutputStream(ctx.object.stream.getOutputStream());			
			while((buffer = ctx.object.stream.readBytes()) != null) {
				tocompress.write(buffer);
			}
		} catch (IOException e) {
			ctx.log.emit("Error compressing object");
		}

		ctx.log.emit("Ended Compression Function");

	}
	
}
