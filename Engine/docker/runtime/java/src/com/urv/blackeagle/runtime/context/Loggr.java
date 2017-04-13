package com.urv.blackeagle.runtime.context;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;

public class Loggr {
	private Logger logger_;
	private FileOutputStream stream;

	public Loggr(FileDescriptor fd, Logger logger) {
		stream = new FileOutputStream(fd);
		logger_ = logger;
		
		logger_.trace("CTX Logger created");
	}

	public void emitLog(String message) {
		message = message + "\n";
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