package org.apache.cassandra.heartbeat.status;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Status map structure: { vn-to-ts: { vn1: ts1, vn2: ts2 }, updateTs: ts }
 * 
 * @author XiGuan
 * 
 */
public class Status
{
    private ConcurrentSkipListMap<Long, Long> m_currentVnToTs;
    private ConcurrentSkipListMap<Long, Long> m_removedVnToTs = new ConcurrentSkipListMap<Long, Long>();
    byte[] m_lockObj = new byte[0];
    private volatile boolean flag = true;

    public Status()
    {
        m_currentVnToTs = new ConcurrentSkipListMap<Long, Long>();
    }

    public void addVnTsData(long inVersionNo, long inTimestamp)
    {
        if (!m_removedVnToTs.containsKey(inVersionNo)&&flag)
        {
            m_currentVnToTs.put(inVersionNo, inTimestamp);
        }
    }
    
    public void addVnTsData(Map<Long, Long> inMap)
    {
        for (Map.Entry<Long, Long> entry : inMap.entrySet())
        {
            if (flag && m_removedVnToTs.get(entry.getKey()) != null )
            {
                m_currentVnToTs.put(entry.getKey(), entry.getValue());
            }
        }
    }
    
    public long removeEntry(Long inVersion, Long inTs)
    {
        boolean removed;
        m_removedVnToTs.put(inVersion, inTs);
        flag = true;
        removed = m_currentVnToTs.remove(inVersion, inTs);
        long ts = removed ? inTs : System.currentTimeMillis();
        return ts;
    }

    public ConcurrentSkipListMap<Long, Long> getVnToTsMap()
    {
        return m_currentVnToTs;
    }
}
