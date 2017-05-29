package com.urv.blackeagle.runtime.function;

import org.slf4j.Logger;


public class Function {

	private Logger logger_;
	private String strName_;
	private String mainClass_;
	private IFunction function_;
	
	private IFunction loadFunction() {
		IFunction function = null;

		try {	
			Class<?> c = ClassLoader.getSystemClassLoader().loadClass(mainClass_);
			function = (IFunction) c.newInstance();
			logger_.info("Function loaded: "+strName_);
		} catch (Exception e) {
			logger_.error(strName_ + ": Failed to load handler class "
					+ " class path is "
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
