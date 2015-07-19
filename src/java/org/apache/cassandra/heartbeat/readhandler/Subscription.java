package org.apache.cassandra.heartbeat.readhandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.heartbeat.status.ARResult;
import org.apache.cassandra.heartbeat.status.KeyResult;
import org.apache.cassandra.heartbeat.status.StatusMap;
import org.apache.cassandra.heartbeat.utils.HBUtils;

/**
 * Read Subscription: { Pagable, readTs, lockObject }
 * 
 * @author XiGuan
 * 
 */
public class Subscription
{
    private String m_ksName;
    private String m_key;
    private long m_timestamp;
    private ConcurrentHashMap<String, KeyResult> m_statusMap; // src, key result
    private ConcurrentHashMap<Integer, Object> m_lockMap;
    private volatile boolean m_lockIsClear;
    private AtomicInteger m_lockVn = new AtomicInteger();

    public Subscription(String ksName, String key, long inTs)
    {
        m_ksName = ksName; m_key = key; m_timestamp = inTs;
        m_statusMap = new ConcurrentHashMap<String, KeyResult>();
        m_lockMap = new ConcurrentHashMap<Integer, Object>();
    }

    public void add(Object lockObj, ARResult inResult)
    {
        if (!m_lockIsClear)
        {
            HBUtils.info(" [Read {} @ '{}'] is sinked as a subscription", m_key, HBUtils.dateFormat(m_timestamp));
            m_lockMap.put(m_lockVn.incrementAndGet(), lockObj);
            for (Map.Entry<String, KeyResult> entry : inResult.getBlockMap().entrySet())
            {
                KeyResult result = m_statusMap.putIfAbsent(entry.getKey(), entry.getValue());
                if (result != null)
                    result.update(entry.getValue());
            }
        }
        else
        {
            synchronized (lockObj)
            {
                lockObj.notify();
            }
            HBUtils.info(" [Read {} @ '{}'] has been executed", m_key, HBUtils.dateFormat(m_timestamp));
        }
    }

    public void awakeByTs(String src, long ts)
    {
        if (!m_lockIsClear)
        {
            KeyResult keyResult = m_statusMap.get(src);
            if (keyResult != null && keyResult.isCausedByTs())
            {
                KeyResult temp = StatusMap.instance.hasLatestValueOnOneSrc(m_ksName, src, m_key, m_timestamp);
                if (temp.value())
                    awakeImpl(src, keyResult, true);
                else
                    keyResult.update(temp);
            }
        }
    }

    public void awakeByVn(String src, long vn)
    {
        if (!m_lockIsClear)
        {
            KeyResult keyResult = m_statusMap.get(src);
            if (keyResult != null && keyResult.isCausedByVn() && keyResult.getExpectedVn() == vn)
            {
                awakeImpl(src, keyResult, false);
            }
        }
    }

    public int size()
    {
        return m_lockMap.size();
    }
    
    private void awakeImpl(String inSrc, KeyResult result, boolean byTs)
    {
        if (!m_lockIsClear)
        {
            HBUtils.info(" [Read {} @ '{}'] is notified by {}", m_key, HBUtils.dateFormat(m_timestamp), byTs ? "ts": "vn");
            m_statusMap.remove(inSrc, result);
            if (m_statusMap.isEmpty())
            {
                m_lockIsClear = true;
                for (Map.Entry<Integer, Object> entry : m_lockMap.entrySet())
                {
                    Object lock = entry.getValue();
                    synchronized (lock)
                    {
                        lock.notify();
                    }
                    m_lockMap.remove(lock);
                }
            }
        }
    }
    
    public boolean isEmpty()
    {
        return m_lockIsClear && m_lockMap.isEmpty();
    }
    
    public long getTs()
    {
        return m_timestamp;
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[ sub: {");
        if (!m_statusMap.isEmpty())
        {
            for (Map.Entry<String, KeyResult> entry : m_statusMap.entrySet())
            {
                sb.append(entry.getKey());
                sb.append(": ");
                sb.append(entry.getValue());
                sb.append(", ");
            }
            sb.setCharAt(sb.length() - 2, ' ');
            sb.setCharAt(sb.length() - 1, '}');
        }
        else
        {
            sb.append("}");
        }
        sb.append(", ");
        sb.append(" reads is clear: ");
        sb.append(m_lockIsClear);
        sb.append(", ");
        sb.append("sinked read no: ");
        sb.append(m_lockMap.size());
        sb.append(" ]");
        return sb.toString();
    }
}
