package com.urv.blackeagle.runtime.daemon;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import redis.clients.jedis.Jedis;
import com.ibm.storlet.sbus.*;
import com.urv.blackeagle.runtime.function.Function;
import com.urv.blackeagle.runtime.function.FunctionExecutionTask;
import java.util.HashMap;
import java.util.Properties;


/*----------------------------------------------------------------------------
 * DockerDaemon - Java Runtime
 *  
 * */
public class DockerDaemon {

	private static ch.qos.logback.classic.Logger logger_;
	private static SBus bus_;
	private static Function function_ = null;
	private static FileOutputStream functionLog_;
	
	private static String configFile = "/opt/zion/config/worker.config";
	private static Properties prop_;
	private static String host;
	private static int redisPort;
	private static int redisDatabase;
	private static Jedis redis_ = null;

	/*------------------------------------------------------------------------
	 * initLog
	 * */
	private static boolean initLog(final String strLogLevel) {
		Level newLevel = Level.toLevel(strLogLevel);
		boolean bStatus = true;
		try {
			logger_ = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("DockerDaemon");
			logger_.setLevel(newLevel);
			logger_.info("Logger Started");
		} catch (Exception e) {
			System.err.println("got exception " + e);
			bStatus = false;
		}
		return bStatus;
	}

	/*------------------------------------------------------------------------
	 * main
	 * 
	 * Entry point.
	 * args[1] - path to Bus
	 * args[2] - log level
	 * 
	 * */
	public static void main(String[] args) throws Exception {
		initialize(args);
		mainLoop();
	}

	/*------------------------------------------------------------------------
	 * initialize
	 * 
	 * Initialize the resources
	 * */
	private static void initialize(String[] args) throws Exception {
		String strBusPath = args[0];
		String strLogLevel = args[1];
		String strContId = args[3];
		
		System.out.println("Initializing Docker Daemon");
		
		if (initLog(strLogLevel) == false)
			return;

		logger_.trace("Instanciating Bus");
		System.out.println("Instanciating Bus");
		bus_ = new SBus(strContId);
		
		try{
			logger_.trace("Loading configuration file "+configFile);
			prop_ = new Properties();
			InputStream is = new FileInputStream(configFile);
			prop_.load(is);
			
			host = prop_.getProperty("host_ip");
			redisPort = Integer.parseInt(prop_.getProperty("redis_port"));
			redisDatabase = Integer.parseInt(prop_.getProperty("redis_db"));
			
		} catch (IOException e) {
			logger_.error("Failed to load the configuration file: "+configFile);
			return;
		}

		
		redis_ = new Jedis(host,redisPort);
		redis_.select(redisDatabase);

		try {
			logger_.trace("Initialising Swift bus: "+strBusPath);
			System.out.println("Initialising Swift bus: "+strBusPath);
			bus_.create(strBusPath);
		} catch (IOException e) {
			logger_.error("Failed to create Swift Bus");
			return;
		}
	}

	/*------------------------------------------------------------------------
	 * mainLoop
	 * 
	 * The main loop - listen, receive, execute till the HALT command. 
	 * */
	private static void mainLoop() throws Exception {
		boolean doContinue = true;
		while (doContinue) {
			// Wait for incoming commands
			try {
				logger_.trace("listening on Bus");
				bus_.listen();
				logger_.trace("Bus listen() returned");
			} catch (IOException e) {
				logger_.error("Failed to listen on Bus. Exiting");
				doContinue = false;
				break;
			}

			logger_.trace("Calling receive");
			SBusDatagram dtg = null;
			try {
				dtg = bus_.receive();
				logger_.trace("Receive returned");
			} catch (IOException e) {
				logger_.error("Failed to receive data on Bus");
				doContinue = false;
				break;

			}
			
			logger_.trace("Going to process recived datagram");
			processDatagram(dtg);
		}
	}
	
	/*------------------------------------------------------------------------
	 * processDatagram
	 * 
	 * Process the recived datagram
	 * */
	private static void processDatagram(SBusDatagram dtg){
		int command = dtg.getNFiles();

		/*
		 *  Start Function
		 */
		if (command == 1){
			if (function_ == null){
				logger_.trace("Got Function startup command");
				String functionName, mainClass;
				HashMap<String, String>[] metadata = dtg.getFilesMetadata();
				FileDescriptor logFd  = null;
				
				logFd = dtg.getFiles()[0];
				functionLog_ = new FileOutputStream(logFd);
				functionName = metadata[0].get("function");
				mainClass = metadata[0].get("main_class");
				logger_.trace("Got "+functionName+" Function");
				function_ = new Function(functionName, mainClass, logger_);
			}
		}
		
		/*
		 * Process Request
		 */
		if (command == 3){
			logger_.trace("Got Function invocation request");
			FunctionExecutionTask functionTask = new FunctionExecutionTask(dtg, prop_, redis_, function_, functionLog_, logger_);
			new Thread(functionTask).start(); 	
		}
	}
}
