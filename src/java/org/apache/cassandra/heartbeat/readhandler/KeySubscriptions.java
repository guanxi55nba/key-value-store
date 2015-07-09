package org.apache.cassandra.heartbeat.readhandler;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.heartbeat.status.ARResult;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.apache.cassandra.service.pager.Pageable;


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
    private ConcurrentHashMap<String, Boolean> m_hasLatestValueMap;
    private String m_ksName;
    private ByteBuffer m_key;
    private String m_keyStr;

    public KeySubscriptions(String ksName, ByteBuffer key)
    {
        m_subMap = new ConcurrentSkipListMap<Long, Subscription>();
        m_hasLatestValueMap = new ConcurrentHashMap<String, Boolean>();
        m_ksName = ksName;
        m_key = key;
        m_keyStr = HBUtils.byteBufferToString(m_key);
        for (InetAddress src : HBUtils.getReplicaListExcludeLocal(m_ksName, m_key))
            m_hasLatestValueMap.put(src.getHostAddress(), false);
    }

    public void addSubscription(Pageable pg, byte[] lockObj, long inTs, ARResult inResult)
    {
        // Get subscriptions
        Subscription subs = m_subMap.get(inTs);
        if (subs == null)
        {
            Subscription temp = new Subscription(m_ksName, m_keyStr, inTs);
            subs = m_subMap.putIfAbsent(inTs, temp);
            if (subs == null)
                subs = temp;
        }

        subs.add(lockObj, pg, inResult);
    }
    
    public void notifySubscriptionByTs(String inSrc, long msgTs)
    {
        for (Long ts : m_subMap.keySet())
        {
            if (ts <= msgTs)
            {
                Subscription subs = m_subMap.get(ts);
                if (subs != null)
                    subs.awakeByTs(inSrc, msgTs);
            }
            else
                break;
        }
    }
    
    public void notifySubscriptionByVn(String inSrc, long msgVn)
    {
        for (Subscription sub : m_subMap.values())
        {
            sub.awakeByVn(inSrc, msgVn);
        }
    }
    
//    private void notifySubscriptionsImpl(long ts, Set<Subscription> subs, boolean shouldNotify)
//    {
//        boolean notify = shouldNotify;
//        for (Subscription sub : subs)
//        {
//            if (!notify)
//                notify = StatusMap.instance.hasLatestValue(sub);
//
//            if (notify)
//            {
//                synchronized (sub.getLockObject())
//                {
//                    sub.getLockObject().notify();
//                }
//                subs.remove(sub);
//                logger.error("Read subscription {} is notified", sub.m_version);
//            }
//        }
//        if (subs.isEmpty())
//            m_subMap.remove(ts, subs);
//    }
    
    public int size()
    {
        int size = 0;
        for (Subscription sub : m_subMap.values())
            size += sub.size();
        return size;
    }
    
}
