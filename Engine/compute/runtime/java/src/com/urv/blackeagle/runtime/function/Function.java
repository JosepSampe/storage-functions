package com.urv.blackeagle.runtime.function;

import org.slf4j.Logger;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.io.File;
import java.util.List;

public class Function {

	private Logger logger_;
	private String strName_;
	private String mainClass_;
	private IFunction function_;
	
	private IFunction loadFunction() {
		IFunction function = null;

		try {	
			List<File> searchPath = Files.walk(Paths.get("/opt/zion/function"))
					                    .filter(Files::isRegularFile)
					                    .map(Path::toFile)
					                    .collect(Collectors.toList());
			
			logger_.info(searchPath.toString());
			
			URL[] classLoaderUrls = new URL[searchPath.size()];
			int index = 0;
			for (File jar : searchPath)
				classLoaderUrls[index++] = jar.toURI().toURL();			
			
			ClassLoader cl = new URLClassLoader(classLoaderUrls);			
			Class<?> c = Class.forName(mainClass_, true, cl);
			
			function = (IFunction) c.newInstance();
			logger_.info("Function loaded: "+strName_);
		} catch (Exception e) {
			logger_.error(strName_ + ": Failed to load handler class."
					+ " Class path is: "
					+ System.getProperty("java.class.path"));
			logger_.error(strName_ + ": " + e.getMessage());			
		}
		return function;
	}

	public Function(String name, String main, Logger logger) {
		this.strName_ = name;	
		this.mainClass_ = main;
		this.logger_ = logger;
		
		this.function_= loadFunction();
	}
	
	public IFunction getFunction() {
		return this.function_;
	}

	public String getName() {
		return this.strName_;
	}

}
