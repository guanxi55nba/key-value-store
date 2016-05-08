package org.apache.cassandra.heartbeat.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfReader
{
    private static final Logger logger = LoggerFactory.getLogger(ConfReader.class);
    public static Properties configuration;
    public static Properties tranProperties;
    public boolean heartbeatEnable = false;
    public String ksName = "";
    public boolean logEnabled = false;
    public int heartbeatInternval = 10;
    public boolean quorumEnabled = false;
    private static final ConfReader instance = new ConfReader();
    private static final String USER_DIR = "user.dir";
    private static final String PARENT_FOLDER = "conf";
    private static final String CONF_FILE_NAME = "key-value-store.conf";

    private ConfReader()
    {
        configuration = new Properties();
        StringBuilder sb = new StringBuilder();
        sb.append(System.getProperty(USER_DIR));
        sb.append(File.separator);
        sb.append(PARENT_FOLDER);
        sb.append(File.separator);
        sb.append(CONF_FILE_NAME);
		String confStr = sb.toString();
        try
        {
            configuration.load(new FileInputStream(new File(confStr)));
            heartbeatEnable = Boolean.valueOf(configuration.getProperty("heartbeat.enable"));
            logEnabled = Boolean.valueOf(configuration.getProperty("log.enable")) && heartbeatEnable;
            heartbeatInternval = Integer.valueOf(configuration.getProperty("heartbeat.interval"));
            quorumEnabled = Boolean.valueOf(configuration.getProperty("enable.write-read-local-quorum"));
        }
        catch (IOException e)
        {
            logger.error("Failed to load configuration " + confStr);
            e.printStackTrace();
        }
    }

    public static int getHeartbeatInterval()
    {
        return instance.heartbeatInternval;
    }

    public static boolean enableWriteReadLocalQuorum()
    {
        return instance.quorumEnabled;
    }

    public static boolean heartbeatEnable()
    {
        return instance.heartbeatEnable;
    }

    public static boolean isLogEnabled()
    {
        return instance.logEnabled;
    }
}
