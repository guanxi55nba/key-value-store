package org.apache.cassandra.heartbeat.readhandler;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.heartbeat.status.Status;
import org.apache.cassandra.heartbeat.status.StatusMap;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 *  { ts: [sub1, sub2, sub3] }
 * 
 * @author xig
 *
 */
public class KeySubscriptions
{
    private static final Logger logger = LoggerFactory.getLogger(KeySubscriptions.class);
    private ConcurrentSkipListMap<Long, Set<Subscription>> m_subMap;
    public KeySubscriptions()
    {
        m_subMap = new ConcurrentSkipListMap<Long, Set<Subscription>>();
    }

    public void addSubscription(long inTs, Subscription inSub)
    {
        // Get subscriptions 
        Set<Subscription> subs = m_subMap.get(inTs);
        if (subs == null)
        {
            Set<Subscription> temp = Sets.newConcurrentHashSet();
            subs = m_subMap.putIfAbsent(inTs, temp);
            if (subs == null)
                subs = temp;
        }
        
        // Check before adding it
        if (StatusMap.instance.hasLatestValue(inSub))
        {
            synchronized (inSub.getLockObject())
            {
                inSub.getLockObject().notify();
                HBUtils.info("Read subscription {} is notified before adding it", inSub.m_version);
            }
        }
        else
        {
            subs.add(inSub);
        }
    }
    
    public void notifySubscription(long msgTs)
    {
        for (Long ts : m_subMap.keySet())
        {
            if (ts <= msgTs)
            {
                Set<Subscription> subs = m_subMap.get(ts);
                if (subs != null && !subs.isEmpty())
                    notifySubscriptionsImpl(msgTs, subs, false);
            }
            else
                break;
        }
    }
    
    private void notifySubscriptionsImpl(long ts, Set<Subscription> subs, boolean shouldNotify)
    {
        boolean notify = shouldNotify;
        for (Subscription sub : subs)
        {
            if (!notify)
                notify = StatusMap.instance.hasLatestValue(sub);

            if (notify)
            {
                synchronized (sub.getLockObject())
                {
                    sub.getLockObject().notify();
                    subs.remove(sub);
                    logger.error("Read subscription {} is notified", sub.m_version);
                }
            }
        }
        if (subs.isEmpty())
            m_subMap.remove(ts, subs);
    }
    
    public int size()
    {
        int size = 0;
        for (Set<Subscription> subs : m_subMap.values())
            size += subs.size();
        return size;
    }
    
}
