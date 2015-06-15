package org.apache.cassandra.heartbeat.extra;

/**
 * Present the version of one update { version: timestamp }
 * 
 * @author XiGuan
 * 
 */
public class Version {
	long localVersion;
	long timestamp;

	public Version(long version, long timestamp) {
		this.localVersion = version;
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getLocalVersion() {
		return localVersion;
	}
}
