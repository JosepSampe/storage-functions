package com.urv.zion.runtime.context;

import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;

public class Log {
	private Logger logger_;
	private FileOutputStream functionLog;

	public Log(FileOutputStream fLog, Logger logger) {
		functionLog = fLog;
		logger_ = logger;
		
		logger_.trace("CTX Function Log created");
	}

	public void emit(String message) {
		message = message+"\n";
		try {
			functionLog.write(message.getBytes());
		} catch (IOException e) {

		}

	}

	public void flush() {
		try {
			functionLog.flush();
		} catch (IOException e) {
		}
	}

}