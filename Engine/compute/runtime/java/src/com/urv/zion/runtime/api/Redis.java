package com.urv.zion.runtime.api;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;
import redis.clients.jedis.Jedis;
import org.slf4j.Logger;


public class Redis {
	private Logger logger_;
	private Jedis redis = null;

	
	public Redis(Jedis r, Properties prop, String projectId, Logger logger){
		logger_ = logger;
		redis = r;
		logger_.trace("Api Redis created");
	}

	public Jedis getClient(){
		return redis;
	}	
}