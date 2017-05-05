package com.urv.blackeagle.function.blurfaces;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.opencv.core.Mat;
import org.opencv.core.MatOfRect;
import org.opencv.core.Rect;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;

import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;
import com.urv.blackeagle.runtime.function.IFunction;

public class Handler implements IFunction {
	
	/***
	 * function entry method. 
	 */
	public void invoke(Context ctx, Api api) {

		ctx.log.emit("Init Blurfaces Function");

		byte[] data;
		
		try {
			String uuid = UUID.randomUUID().toString();
			String tmp_file = "/tmp/"+uuid+".jpg";
			String classsifier = "haarcascade_frontalface_alt.xml";

			ctx.log.emit("Extracting classifier to /tmp folder");
			URL inputUrl = getClass().getResource(classsifier);
			File dest = new File("/tmp/"+classsifier);
			if (!dest.exists()) FileUtils.copyURLToFile(inputUrl, dest);

			ctx.log.emit("Storing image in /tmp folder");
			FileOutputStream image = new FileOutputStream(tmp_file);
			while((data = ctx.object.stream.readBytes()) != null) {
				image.write(data);
			}
			image.close();
			
			ctx.log.emit("Loading classifier");
			CascadeClassifier faceDetector = new CascadeClassifier("/tmp/"+classsifier);
			ctx.log.emit(faceDetector.toString());
			
			ctx.log.emit("Loading image");
		    Mat img = Imgcodecs.imread(tmp_file);
		    MatOfRect faceDetections = new MatOfRect();
		    ctx.log.emit(img.toString());
		    
		    faceDetector.detectMultiScale(img, faceDetections);
		    ctx.log.emit("Detected faces: " + faceDetections.toArray().length);

		    for (Rect rect : faceDetections.toArray()) {
		    	Mat mask = img.submat(rect);
		        Imgproc.GaussianBlur(mask, mask, new Size(25, 25), 0);
		    }

	        ctx.log.emit("Writing blurred image to: "+tmp_file);
	        Imgcodecs.imwrite(tmp_file, img);
	        
	        Path path = FileSystems.getDefault().getPath(tmp_file);
	        byte [] fileData = Files.readAllBytes(path);
	        ctx.object.stream.writeBytes(fileData);
	        Files.delete(path);
			
		} catch (FileNotFoundException e) {
			ctx.log.emit("Error creating image file in /tmp folder");
		}catch (IOException e) {
			ctx.log.emit("Error writing image to output stream");
		}

		ctx.log.emit("Ended Blurfaces Function");

	}
	
}
