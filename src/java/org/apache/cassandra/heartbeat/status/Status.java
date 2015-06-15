package org.apache.cassandra.heartbeat.status;

import java.util.Map;
import java.util.TreeMap;

/**
 * Status map structure: { vn-to-ts: { vn1: ts1, vn2: ts2 }, updateTs: ts }
 * 
 * @author XiGuan
 * 
 */
public class Status {

	private long m_updateTs;
	private TreeMap<Long, Long> m_vnToTs;

	public Status(long inUpdateTs, TreeMap<Long, Long> inVnToTs) {
		m_updateTs = inUpdateTs;
		m_vnToTs = inVnToTs;
		if (m_vnToTs == null) {
			m_vnToTs = new TreeMap<Long, Long>();
		}
	}

	public Status() {
		m_updateTs = -1;
		m_vnToTs = new TreeMap<Long, Long>();
	}

	public void updateVnTsData(long inVersionNo, long inTimestamp) {
		m_vnToTs.put(inVersionNo, inTimestamp);
	}

	public void setUpdateTs(long inUpdateTs) {
		m_updateTs = inUpdateTs;
	}

	public long getUpdateTs() {
		return m_updateTs;
	}

	public void updateVnTsData(Map<Long, Long> inMap) {
		m_vnToTs.putAll(inMap);
	}

	public Long removeEntry(Long inVersion) {
		return m_vnToTs.remove(inVersion);
	}

	public TreeMap<Long, Long> getVersionTsMap() {
		return m_vnToTs;
	}
}
