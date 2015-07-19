package org.apache.cassandra.heartbeat.readhandler;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.heartbeat.status.ARResult;
import org.apache.cassandra.heartbeat.utils.HBUtils;


/**
 * 
 * Contains all the subscriptions for one key
 * 
 *  { ts: [sub1, sub2, sub3] }
 * 
 * @author xig
 *
 */
public class KeySubscriptions
{
    private ConcurrentSkipListMap<Long, Subscription> m_subMap;
    private String m_ksName;
    private String m_keyStr;

    public KeySubscriptions(String ksName, String keyStr)
    {
        m_subMap = new ConcurrentSkipListMap<Long, Subscription>();
        m_ksName = ksName;
        m_keyStr = keyStr;
    }

    public void addSubscription(long readTs, Object lockObj, ARResult inResult)
    {
        Subscription subs = m_subMap.get(readTs);
        if (subs == null)
        {
            Subscription temp = new Subscription(m_ksName, m_keyStr, readTs);
            subs = m_subMap.putIfAbsent(readTs, temp);
            if (subs == null)
            {
                subs = temp;
            }
        }

        subs.add(lockObj, inResult);
    }
    
    public void notifySubscriptionByTs(String inSrc, long msgTs)
    {
        for (Map.Entry<Long, Subscription> entry : m_subMap.entrySet())
        {
            Long ts = entry.getKey();
            if (ts <= msgTs)
            {
                Subscription sub = entry.getValue();
                sub.awakeByTs(inSrc, msgTs);
                if (sub.isEmpty())
                    m_subMap.remove(ts, sub);
            }
            else
                break;
        }
    }
    
    public void notifySubscriptionByVn(String inSrc, long msgVn)
    {
        for (Map.Entry<Long, Subscription> entry : m_subMap.entrySet())
        {
            Long ts = entry.getKey();
            Subscription sub = entry.getValue();
            sub.awakeByVn(inSrc, msgVn);
            if (sub.isEmpty())
                m_subMap.remove(ts, sub);
        }
        
    }
    
    
    public int size()
    {
        int size = 0;
        for (Subscription sub : m_subMap.values())
            size += sub.size();
        return size;
    }
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        sb.append( m_keyStr);
        sb.append(" subs: {");
        if (!m_subMap.isEmpty())
        {
            for (Map.Entry<Long, Subscription> entry : m_subMap.entrySet())
            {
                sb.append("'");
                sb.append(HBUtils.dateFormat(entry.getKey()));
                sb.append("'");
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
        sb.append(" }");
        return sb.toString();
    }
    
}
