package org.apache.cassandra.heartbeat;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.UntypedResultSet.Row;

public class KeyMetaData {
	private String m_ksName;
	private String m_cfName;
	private Row m_row;
	private ByteBuffer m_key;
	public KeyMetaData(String inKsName, String inCfName, ByteBuffer inKey, Row inRow) {
		super();
		m_ksName = inKsName;
		m_cfName = inCfName;
		m_key = inKey;
		m_row = inRow;
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
	
	public Row getRow() {
		return m_row;
	}
}
