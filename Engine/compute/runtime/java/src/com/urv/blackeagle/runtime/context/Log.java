package com.urv.blackeagle.runtime.context;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;

public class Log {
	private Logger logger_;
	private FileOutputStream stream;

	public Log(FileDescriptor fd, Logger logger) {
		stream = new FileOutputStream(fd);
		logger_ = logger;
		
		logger_.trace("CTX Function Log created");
	}

	public void emit(String message) {
		message = message+"\n";
		try {
			stream.write(message.getBytes());
		} catch (IOException e) {

		}

	}

	public void flush() {
		try {
			stream.flush();
		} catch (IOException e) {
		}
	}

}