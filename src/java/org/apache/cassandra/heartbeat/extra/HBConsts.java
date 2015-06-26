package org.apache.cassandra.heartbeat.extra;

import org.apache.cassandra.heartbeat.utils.ConfReader;

public class HBConsts {
	public static final String VERSON_NO = "vn";
	public static final String SOURCE = "src";
	public static final String KEY_ID = "id";
	public static final String VERSION_WRITE_TIME = "writetime(" + VERSON_NO + ")";
	public static final String COORDINATOR = "C";
	
	//Not used
	public static final String VALID_TO = "vt";
	public static final String CF_NAME = ConfReader.instance.getColumnFamilyName();
}
