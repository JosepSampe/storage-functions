package com.urv.blackeagle.runtime.context;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.json.simple.JSONObject;
import org.slf4j.Logger;

import com.urv.blackeagle.runtime.api.Swift;


public class Object {
	private String object;
	private Swift swift;
	private Logger logger_;	
	private Request request;
	private Response response;
	public Metadata metadata;
	public Stream stream;
	public String timestamp;
	public String etag;
	public String lastModified;
	public String contentLength;
	public String backendTimestamp;
	public String contentType;
	private FileOutputStream command;
		
	public Object(FileDescriptor inputStreamFd, FileDescriptor outputStreamFd, FileDescriptor commandFd,
			      Map<String, String> objectMetadata, String currentObject, Request req, Response resp, 
			      Swift apiSwift, Logger logger) {
		
		stream = new Stream(inputStreamFd, outputStreamFd);
		command = new FileOutputStream(commandFd);
		object = currentObject;
		request = req;
		response = resp;
		swift = apiSwift;		
		logger_ = logger;
		objectMetadata.entrySet().removeIf(entry -> entry.getKey().startsWith("X-Object-Sysmeta-Vertigo"));

		metadata = new Metadata(objectMetadata);
		
		timestamp = objectMetadata.get("X-Timestamp");
		etag = objectMetadata.get("Etag");
		lastModified = objectMetadata.get("Last-Modified");
		contentLength = objectMetadata.get("Content-Length");
		backendTimestamp = objectMetadata.get("X-Backend-Timestamp");
		contentType = objectMetadata.get("Content-Type");
		
		logger_.trace("CTX Object created");
	}
				
	public void copy(String dest){
		swift.copy(object, dest);
	}
	
	public void move(String dest){
		swift.move(object, dest);
	}
	
	public void delete(){
		swift.delete(object);
	}
	
	public class Stream {
		protected InputStream inputStream;
		protected OutputStream outputStream;
		private BufferedReader br;
		private BufferedWriter bw;
		boolean dataRead = false, dataWrite = false;
		private JSONObject outMetadata = new JSONObject();
		
		public Stream(FileDescriptor inputStreamFd, FileDescriptor outputStreamFd){
			inputStream = ((InputStream) (new FileInputStream(inputStreamFd)));
			outputStream = ((OutputStream) (new FileOutputStream(outputStreamFd)));
			
			try {
				br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
				bw = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				logger_.trace("Error: Unsuported encoding URF-8");
			}
		}

		public String read(){
			String chunk = this.read(65535);
			return chunk;
		}

		public String read(int bytes){
			int len = 0;
			char[] cbuf = new char[bytes];
			try {
				if (dataRead == false){
					dataRead = true;
					this.sendReadCommand();
				}
				len = br.read(cbuf);
			} catch (IOException e) {
				logger_.trace("Error while reading input data from the object");
				len = -1;
			}
			if (len == -1){
				try {
					br.close();
					inputStream.close();
					logger_.trace("Closed input stream");
				} catch (IOException e) {
					logger_.trace("Error closing buffered reader (input)");
				}
				return null;
			}
			return String.valueOf(cbuf);
		}
		
		public String readLine(){
			String line = null;
			try {
				if (dataRead == false){
					dataRead = true;
					this.sendReadCommand();
				}
				line = br.readLine();
			} catch (IOException e) {
				logger_.trace("Error while reading lines");
			}
			return line;
		}	
		
		public void write(String data){
			try {
				if (dataWrite == false){
					dataWrite = true;
					this.sendWriteCommand();
				}
				bw.write(data);
			} catch (IOException e) {
				logger_.trace("Error while writing out data");
			}
		}
		
		public void close(){
			try {
				br.close();
				inputStream.close();
				bw.close();
				outputStream.close();
				logger_.trace("Closed input/output streams");
			} catch (IOException e) {
				logger_.trace("Error closing buffered writer (output)");
			}
		}
		
		@SuppressWarnings("unchecked")
		public void sendReadCommand() {
			outMetadata.put("command","DATA_READ");
			this.sendCommand();

		}
		
		@SuppressWarnings("unchecked")
		public void sendWriteCommand() {
			outMetadata.put("command","DATA_WRITE");
			if (metadata.isModified())
				outMetadata.put("object_metadata", metadata.getAll());
			if (response.headers.isModified())
				outMetadata.put("response_headers",response.headers.getAll());
			if (request.headers.isModified())
				outMetadata.put("request_headers", request.headers.getAll());
			this.sendCommand();
		}
		
		public void sendCommand() {
			try {
				command.write(outMetadata.toString().getBytes());
				command.flush();
			} catch (IOException e) {
				logger_.trace("Error sending DATA READ/WRITE command");
			}
		}
		
	}

	public class Metadata { 
		private boolean dirtyBit = false;
		private Map<String, String> metadata;
		
		public Metadata(Map<String, String> objMd){
			metadata = objMd;
		}
		 
		public String get(String key){
			return metadata.get("X-Object-Meta-"+key);
		}
		
		public Map<String, String> getAll(){
			return metadata;
		}

		public void set(String key, String value){
			this.metadata.put("X-Object-Meta-"+key, value);
			swift.metadata.set(object, key, value);
			dirtyBit = true;
		}

		public Long incr(String key){
			Long newValue = swift.metadata.incr(object, key);
			this.metadata.put("X-Object-Meta-"+key, String.valueOf(newValue));
			dirtyBit = true;
			return newValue;
		}
		
		public Long incrBy(String key, int value){
			Long newValue = swift.metadata.incrBy(object, key, value);
			this.metadata.put("X-Object-Meta-"+key, String.valueOf(newValue));
			dirtyBit = true;
			return newValue;
		}
		
		public Long decr(String key){
			Long newValue = swift.metadata.decr(object, key);
			this.metadata.put("X-Object-Meta-"+key, String.valueOf(newValue));
			dirtyBit = true;
			return newValue;
		}
		
		public Long decrBy(String key, Integer value){
			Long newValue = swift.metadata.decrBy(object, key, value);
			this.metadata.put("X-Object-Meta-"+key, String.valueOf(newValue));
			dirtyBit = true;
			return newValue;
		}

		public void del(String key){
			swift.metadata.del(object, key);
			this.metadata.remove("X-Object-Meta-"+key);
			dirtyBit = true;
		}
		
		public boolean isModified(){
			return this.dirtyBit;
		}
		
		public void flush(){
			if (dirtyBit)
				swift.metadata.flush(object);
		}
		
	}
}