package com.urv.zion.function.compression;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import com.urv.zion.runtime.api.Api;
import com.urv.zion.runtime.context.Context;
import com.urv.zion.runtime.function.IFunction;

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
			tocompress.close();
		} catch (IOException e) {
			ctx.log.emit("Error compressing object");
		}

		ctx.log.emit("Ended Compression Function");

	}
	
}
