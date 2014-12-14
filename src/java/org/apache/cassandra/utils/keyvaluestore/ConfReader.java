package org.apache.cassandra.utils.keyvaluestore;

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
	public static final ConfReader instance = new ConfReader();

	private ConfReader() {
		configuration = new Properties();
		String confStr = "conf" + File.separator + "key-value-store.conf";
		try {
			configuration.load(new FileInputStream(new File(confStr)));
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
		return configuration.getProperty("keyspace.name");
	}

	public String getColumnFamilyName() {
		return configuration.getProperty("columnfamily.name");
	}
	
	public boolean enableWriteReadLocalQuorum() {
		return Boolean.valueOf(configuration.getProperty("enable.write-read-local-quorum"));
	}
}
