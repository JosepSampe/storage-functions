package com.urv.zion.function.imageresizer;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import com.urv.zion.runtime.api.Api;
import com.urv.zion.runtime.context.Context;
import com.urv.zion.runtime.function.IFunction;

public class Handler implements IFunction {

	/***
	 * function entry method. 
	 */
	public void invoke(Context ctx, Api api) {

		ctx.log.emit("Init Image Resizer Function");

		byte[] data;
		
		try {
			String uuid = UUID.randomUUID().toString();
			String tmp_file = "/tmp/"+uuid+".jpg";

			FileOutputStream image = new FileOutputStream(tmp_file);
			while((data = ctx.object.stream.readBytes()) != null) {
				image.write(data);
			}
			image.close();

			Process p = new ProcessBuilder("sh", "-c", "mogrify -resize 50% "+tmp_file).start();
			p.waitFor();

			Path path = FileSystems.getDefault().getPath(tmp_file);
	        byte [] fileData = Files.readAllBytes(path);
	        
	        ctx.object.stream.writeBytes(fileData);
	        
	        Files.delete(path);
			
		} catch (FileNotFoundException e) {
			ctx.log.emit("Error creating image file in /tmp folder");
		}catch (IOException e) {
			ctx.log.emit("Error writing image to output stream");
		}catch (InterruptedException e) {
			ctx.log.emit("Error resizing image");
		}

		ctx.log.emit("Ended Image Resizer Function");

	}
	
}
