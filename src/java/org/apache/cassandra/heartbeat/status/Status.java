package org.apache.cassandra.heartbeat.status;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Status map structure: { vn-to-ts: { vn1: ts1, vn2: ts2 }, updateTs: ts }
 * 
 * @author XiGuan
 * 
 */
public class Status
{
    private volatile long m_updateTs;
    private ConcurrentSkipListMap<Long, Long> m_currentVnToTs;
    private ConcurrentSkipListMap<Long, Long> m_removedVnToTs = new ConcurrentSkipListMap<Long, Long>();
    byte[] m_lockObj = new byte[0];

    public Status()
    {
        m_updateTs = -1;
        m_currentVnToTs = new ConcurrentSkipListMap<Long, Long>();
    }

    public void addVnTsData(long inVersionNo, long inTimestamp)
    {
        synchronized (m_lockObj)
        {
            if (!m_removedVnToTs.containsKey(inVersionNo))
            {
                m_currentVnToTs.put(inVersionNo, inTimestamp);
            }
        }
    }
    
    public void addVnTsData(Map<Long, Long> inMap, long inTs)
    {
        synchronized (m_lockObj)
        {
            for (Map.Entry<Long, Long> entry : inMap.entrySet())
            {
                if (!m_removedVnToTs.containsKey(entry.getKey()))
                {
                    m_currentVnToTs.put(entry.getKey(), entry.getValue());
                }
            }
            setUpdateTs(inTs);
        }
    }
    
    public Long removeEntry(Long inVersion, Long inTs)
    {
        boolean removed;
        synchronized (m_lockObj)
        {
            m_removedVnToTs.put(inVersion, inTs);
            removed = m_currentVnToTs.remove(inVersion, inTs);
        }
        long ts = removed ? inTs : System.currentTimeMillis();
        return ts;
    }

    public void setUpdateTs(long inUpdateTs)
    {
        if (inUpdateTs > m_updateTs)
            m_updateTs = inUpdateTs;
    }

    public long getUpdateTs()
    {
        return m_updateTs;
    }

    public ConcurrentSkipListMap<Long, Long> getVnToTsMap()
    {
        return m_currentVnToTs;
    }
}
