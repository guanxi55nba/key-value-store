package org.apache.cassandra.heartbeat.readhandler;

import java.util.Map;
import java.util.Set;

import org.apache.cassandra.heartbeat.status.ARResult;
import org.apache.cassandra.heartbeat.status.KeyResult;
import org.apache.cassandra.heartbeat.status.StatusMap;
import org.apache.cassandra.service.pager.Pageable;

import com.datastax.shaded.netty.util.internal.ConcurrentHashMap;
import com.google.common.collect.Sets;

/**
 * Read Subscription: { Pagable, readTs, lockObject }
 * 
 * @author XiGuan
 * 
 */
public class Subscription
{
    public final long m_version;
    private String m_ksName;
    private String m_key;
    private long m_timestamp;
    private ConcurrentHashMap<String, KeyResult> m_statusMap; // src, key result
    private volatile boolean m_flag = true;
    private Set<byte[]> m_lockSet;
    private volatile boolean m_lockIsClear;

    public Subscription(String ksName, String key, long inTs)
    {
        m_ksName = ksName;
        m_key = key;
        m_version = inTs;
        m_statusMap = new ConcurrentHashMap<String, KeyResult>();
        m_lockSet = Sets.newConcurrentHashSet();
    }

    public void add(byte[] lockObj, Pageable pg, ARResult inResult)
    {
        if (!m_lockIsClear)
        {
            m_lockSet.add(lockObj);
            for (Map.Entry<String, KeyResult> entry : inResult.getBlockMap().entrySet())
            {
                KeyResult result = m_statusMap.putIfAbsent(entry.getKey(), entry.getValue());
                if (result != null)
                {
                    synchronized (result)
                    {
                        result.update(entry.getValue());
                    }
                }
            }
        }
        else
        {
            synchronized (lockObj)
            {
                lockObj.notify();
            }
        }
    }

    public void awakeByTs(String src, long ts)
    {
        KeyResult keyResult = m_statusMap.get(src);
        if (keyResult != null)
        {
            if (keyResult.isCausedByTs())
            {
                KeyResult temp = StatusMap.instance.hasLatestValueOnOneSrc(m_ksName, src, m_key, m_timestamp);
                if (temp.value())
                {
                    m_statusMap.remove(ts, keyResult);
                    m_flag = true;
                    awakeImpl();
                }
                else
                {
                    keyResult.update(temp);
                }
            }
        }
    }

    public void awakeByVn(String src, long vn)
    {
        KeyResult keyResult = m_statusMap.get(src);
        if (keyResult != null)
        {
            if (keyResult.isCausedByVn() && keyResult.getExpectedVn() == vn)
            {
                awakeImpl();
            }
        }
    }

    public int size()
    {
        return m_lockSet.size();
    }
    
    private void awakeImpl()
    {
        if (m_flag && m_statusMap.isEmpty() && !m_lockIsClear)
        {
            m_lockIsClear = true;
            for (byte[] lock : m_lockSet)
            {
                synchronized (lock)
                {
                    lock.notify();
                }
            }
            m_lockSet.clear();
        }
    }
}
