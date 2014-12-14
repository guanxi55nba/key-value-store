package org.apache.cassandra.heartbeat;

import java.nio.ByteBuffer;

public class KeyMetaData {
	private String m_ksName;
	private String m_cfName;
	private ByteBuffer m_key;
	public KeyMetaData(String inKsName, String inCfName, ByteBuffer inKey) {
		super();
		m_ksName = inKsName;
		m_cfName = inCfName;
		m_key = inKey;
	}
	public String getKsName() {
		return m_ksName;
	}
	public String getCfName() {
		return m_cfName;
	}
	public ByteBuffer getKey() {
		return m_key;
	}
}
