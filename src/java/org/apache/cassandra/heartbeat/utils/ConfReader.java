package org.apache.cassandra.heartbeat.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfReader {
	private static final Logger logger = LoggerFactory.getLogger(ConfReader.class);
	public static Properties configuration;
	public static Properties tranProperties;
	private boolean heartbeatEnable = false;
	private String ksName = "";
	private boolean logEnabled = false;
	public static final ConfReader instance = new ConfReader();

	private ConfReader() {
		configuration = new Properties();
		String confStr = "conf" + File.separator + "key-value-store.conf";
		try {
			configuration.load(new FileInputStream(new File(confStr)));
			heartbeatEnable = Boolean.valueOf(configuration.getProperty("heartbeat.enable"));
			logEnabled = Boolean.valueOf(configuration.getProperty("log.enable"));
		} catch (IOException e) {
			logger.error("Failed to load configuration " + confStr);
			e.printStackTrace();
		}
	}

	public int getHeartbeatInterval() {
		String value = configuration.getProperty("heartbeat.interval");
		try {
			return Integer.valueOf(value).intValue();
		} catch (NumberFormatException e) {
			return 50;
		}
	}

	public String getKeySpaceName() {
		if(ksName.isEmpty())
			ksName = configuration.getProperty("keyspace.name");
		return ksName;
	}

	public String getColumnFamilyName() {
		return configuration.getProperty("columnfamily.name");
	}
	
	public boolean enableWriteReadLocalQuorum() {
		return Boolean.valueOf(configuration.getProperty("enable.write-read-local-quorum"));
	}
	
	public boolean heartbeatEnable() {
		return heartbeatEnable;
	}
	
	public boolean isLogEnabled() {
		return logEnabled;
	}
}
