package com.urv.blackeagle.runtime.api;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import redis.clients.jedis.Jedis;
import org.slf4j.Logger;


public class Swift {
	private Logger logger_;	
	private String token;
	private String storageUrl;
	private String tenantId;
	private Jedis redis;
	public Metadata metadata;
	
	private List<String> unnecessaryHeaders = Arrays.asList(null, "Connection", "X-Trans-Id", "Date");
	Map<String, Map<String, String>> objectMetadata = new HashMap<String, Map<String, String>>();
	private boolean dirtyBit = false;

	public Swift(Jedis r, Properties prop, String strToken, String projectId, Logger logger) {
		token = strToken;
		tenantId = projectId;
		logger_ = logger;
		redis = r;

		String swift_host = prop.getProperty("host_ip");
		String swift_port = prop.getProperty("swift_port");
		storageUrl =  "http://"+swift_host+swift_port+"/v1/AUTH_"+projectId+"/";
		metadata = new Metadata();

		logger_.info("API Swift created");
	}
	
	public void close(){
		//redis.close();		
	}

	public class Metadata { 
		
		private void getAll(String source){
			logger_.info("Getting object metadata from Swift");

			String objectID = tenantId+"/"+source;
			Set<String> keys = redis.keys(objectID);

			if (keys.size() == 0){
				HttpURLConnection conn = newConnection(source);
				try {
					conn.setRequestMethod("HEAD");
				} catch (ProtocolException e) {
					logger_.error("API Swift: Bad Protocol");
				}
				sendRequest(conn);
				Map<String, List<String>> headers = conn.getHeaderFields();		
				Map<String, String> metadata = new HashMap<String, String>();
				for (Entry<String, List<String>> entry : headers.entrySet()) {
					String key = entry.getKey();
					String value = entry.getValue().get(0);
					if (!unnecessaryHeaders.contains(key) && !key.startsWith("Vertigo")){
						metadata.put(key.toLowerCase(), value);
					}
					
				}
				redis.hmset(objectID, metadata);
				objectMetadata.put(objectID, metadata);
				
			} else {
				for (String key: keys){
					Map<String, String> values = redis.hgetAll(key);
					objectMetadata.put(objectID, values);
				}
			}

		}	
		 
		public String get(String source, String key){
			String objectID = tenantId+"/"+source;
			String value = redis.hget(objectID, key.toLowerCase());
			if (value == null){
				getAll(source);
				Map<String, String> metadata;
				metadata = objectMetadata.get(objectID);
				value = metadata.get(key.toLowerCase());
			}
			return value;
		}
				
		public void set(String source, String key, String value){
			String objectID = tenantId+"/"+source;
			String redisKey = "x-object-meta-"+key.toLowerCase();
			redis.hset(objectID,redisKey,value);
		}
		
		public Long incr(String source, String key){
			String objectID = tenantId+"/"+source;
			String redisKey = "x-object-meta-"+key.toLowerCase();
			Long newValue = redis.hincrBy(objectID,redisKey,1);
			return newValue;
		}
		
		public Long incrBy(String source, String key, int value){
			String objectID = tenantId+"/"+source;
			String redisKey = "x-object-meta-"+key.toLowerCase();
			Long newValue = redis.hincrBy(objectID, redisKey, value);
			return newValue;
		}
		
		public Long decr(String source, String key){
			String objectID = tenantId+"/"+source;
			String redisKey = "x-object-meta-"+key.toLowerCase();
			Long newValue = redis.hincrBy(objectID,redisKey,-1);
			return newValue;
		}
		
		public Long decrBy(String source, String key, int value){
			String objectID = tenantId+"/"+source;
			String redisKey = "x-object-meta-"+key.toLowerCase();
			Long newValue = redis.hincrBy(objectID, redisKey, -value);
			return newValue;
		}

		public void del(String source, String key){
			String objectID = tenantId+"/"+source;
			String redisKey = "x-object-meta-"+key.toLowerCase();
			redis.hdel(objectID, redisKey);
		}
		
		public void flush(String source){
			logger_.info("Going to offload object metadata to Swift");
			String objectID = tenantId+"/"+source;
			Map<String, String> redisData = redis.hgetAll(objectID);
			HttpURLConnection conn = newConnection(source);
			redisData.forEach((k,v)->conn.setRequestProperty(k, v));
			try {
				conn.setRequestMethod("POST");
			} catch (ProtocolException e) {
				logger_.error("API Swift: Bad Protocol");
			}
			sendRequest(conn);
		}
		
	}

	public void setFunction(String source, String functionName, String method, String metadata){
		HttpURLConnection conn = newConnection(source);
		conn.setRequestProperty("X-Function-on"+method, functionName);
		try {
			conn.setRequestMethod("POST");
		} catch (ProtocolException e) {
			logger_.error("API Swift: Bad Protocol");
		}
		sendFunctionMetadata(conn, metadata);
	}	
	
	private int sendFunctionMetadata(HttpURLConnection conn, String metadata){
		OutputStream os;
		int status = 404;
		try {
			conn.connect();
			os = conn.getOutputStream();
			os.write(metadata.getBytes());
			os.close();
			status = conn.getResponseCode();
		} catch (IOException e) {
			logger_.error("API Swift: Error setting function metadata");
		}
		conn.disconnect();	
		return status;
	}
	
	public void copy(String source, String dest){
		if (!source.equals(dest)){
			HttpURLConnection conn = newConnection(dest);
			conn.setFixedLengthStreamingMode(0);
			conn.setRequestProperty("X-Copy-From", source);
			try {
				conn.setRequestMethod("PUT");
			} catch (ProtocolException e) {
				logger_.error("API Swift: Bad Protocol");
			}
			sendRequest(conn);
			logger_.info("Copying "+source+" object to "+dest);
		}
	}
	
	public void move(String source, String dest){
		if (!source.equals(dest)){
			HttpURLConnection conn = newConnection(source);
			conn.setFixedLengthStreamingMode(0);
			conn.setRequestProperty("X-Link-To", dest);
			try {
				conn.setRequestMethod("PUT");
			} catch (ProtocolException e) {
				logger_.trace("API Swift: Bad Protocol");
			}
			sendRequest(conn);
			logger_.info("Moving "+source+" object to "+dest);
		}
	}
		
	public void prefetch(String source){
		HttpURLConnection conn = newConnection(source);
		conn.setRequestProperty("X-Object-Prefetch","True");
		try {
			conn.setRequestMethod("POST");
		} catch (ProtocolException e) {
			logger_.error("API Swift: Bad Protocol");
		}
		sendRequest(conn);
	}
	
	/*
	 * Public method for deleting an object
	 */
	public void delete(String source){
		HttpURLConnection conn = newConnection(source);
		try {
			conn.setRequestMethod("DELETE");
		} catch (ProtocolException e) {
			logger_.error("API Swift: Bad Protocol");
		}
		sendRequest(conn);
	}

	/*
	 * Public method for getting an object without headers 
	 */
	public HttpURLConnection get(String source){
		HttpURLConnection conn = newConnection(source);

		return getObject(conn);
	}
	
	/*
	 * Public method for getting an object with headers 
	 */
	public HttpURLConnection get(String source, Map<String, String> headers){
		HttpURLConnection conn = newConnection(source);
		
		for (Map.Entry<String, String> entry : headers.entrySet()){
			conn.setRequestProperty(entry.getKey(), entry.getValue());
		}
		
		return getObject(conn);
	}
	
	private HttpURLConnection getObject(HttpURLConnection conn){
		conn.setDoOutput(false);
		conn.setDoInput(true);
		
		try {
			conn.setRequestMethod("GET");
		} catch (ProtocolException pe) {
			logger_.error("API Swift: Bad Protocol");
		} 
		return conn;	
	}

	private HttpURLConnection newConnection(String source){
		String storageUri = storageUrl+source;
		URL url = null;
		HttpURLConnection conn = null;
		try {
			url = new URL(storageUri);
			conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			if (token != null) conn.setRequestProperty("X-Auth-Token", token);
			conn.setRequestProperty("User-Agent", "function_java_runtime");
		} catch (MalformedURLException e) {
			logger_.error("API Swift: Malformated URL");
		} catch (IOException e) {
			logger_.error("API Swift: Error opening connection");
		}
		return conn;	
	}
	
	private int sendRequest(HttpURLConnection conn){
		int status = 404;
		try {
			conn.connect();
			status = conn.getResponseCode();
		} catch (IOException e) {
			logger_.error("API Swift: Error getting response");
		}
		conn.disconnect();
		return status;
	}
	
	private String MD5(String key) {
        MessageDigest m = null;
		try {
			m = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			logger_.error("API Swift: Hash Algorith error");
		}
        m.update(key.getBytes(),0,key.length());
        String hash = new BigInteger(1,m.digest()).toString(16);
        while (hash.length() < 32)
        	hash = "0"+hash;
        return hash;
	}
	
}