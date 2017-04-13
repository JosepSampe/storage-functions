package com.urv.blackeagle.runtime.function;

import org.slf4j.Logger;
import java.util.List;
import java.util.Arrays;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;


public class Function {

	private Logger logger_;
	private String strName_;
	private String strMainClass_;
	private String strDependencies_;
	private IFunction function_;

	private IFunction loadFunction() {
		IFunction function = null;
		List<String> dependencies = Arrays.asList(strDependencies_.split(","));
		URL[] searchPath = new URL[dependencies.size()+1];
		Integer index = 0;	

		try {	
			//logger_.info("/home/swift/"+strHandlerClassName_+"/"+strHandlerName_);
			
			searchPath[index++] = new File("/home/swift/"+strMainClass_+"/"+strName_).toURI().toURL();
			for (String dependency : dependencies) {
				//logger_.info("/home/swift/"+strHandlerClassName_+"/"+dependency);
				searchPath[index++] = new File("/home/swift/"+strMainClass_+"/"+dependency).toURI().toURL();
			}
			
			ClassLoader cl = new URLClassLoader(searchPath);			
			Class<?> c = Class.forName(strMainClass_, true, cl);

			function = (IFunction) c.newInstance();
			//logger_.info("Function loaded: "+strName_+" with dependencies: "+strDependencies_);

		} catch (Exception e) {
			logger_.error(strName_ + ": Failed to load handler class "
					+ strMainClass_ + " class path is "
					+ System.getProperty("java.class.path"));
			logger_.error(strName_ + ": " + e.getStackTrace().toString());			
		}
		return function;
	}

	public Function(String name, String mainClass, String dependencies, Logger logger) {
		this.logger_ = logger;
		this.strName_ = name;
		this.strMainClass_ = mainClass;
		this.strDependencies_ = dependencies;
		
		this.function_ = loadFunction();
	}
	
	public IFunction getFunction() {
		return this.function_;
	}

}
