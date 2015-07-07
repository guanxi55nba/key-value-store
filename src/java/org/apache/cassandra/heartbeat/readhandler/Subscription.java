package org.apache.cassandra.heartbeat.readhandler;

import org.apache.cassandra.service.pager.Pageable;

/**
 * Read Subscription: { Pagable, readTs, lockObject }
 * 
 * @author XiGuan
 * 
 */
public class Subscription implements Comparable<Subscription> {
	Pageable m_pageable;
	Long m_timestamp;
	byte[] m_lockObject;
	public final long m_version;

	public Subscription(Pageable inPageable, long inTimestamp, byte[] lockObject) {
		m_pageable = inPageable;
		m_timestamp = inTimestamp;
		m_lockObject = lockObject;
		m_version = -1;
	}
	
    public Subscription(Pageable inPageable, long inTimestamp, byte[] lockObject, long inVersion)
    {
        m_pageable = inPageable;
        m_timestamp = inTimestamp;
        m_lockObject = lockObject;
        m_version = inVersion;
    }

	@Override
	public int compareTo(Subscription inTs) {
		return m_timestamp.compareTo(inTs.getTimestamp());
	}

	public Pageable getPageable() {
		return m_pageable;
	}

	public void setPageable(Pageable inPageable) {
		m_pageable = inPageable;
	}

	public Long getTimestamp() {
		return m_timestamp;
	}

	public void setTimestamp(Long inTimestamp) {
		m_timestamp = inTimestamp;
	}

	public byte[] getLockObject() {
		return m_lockObject;
	}

	public void setLockObject(byte[] inLockObject) {
		m_lockObject = inLockObject;
	}
	
	
}
