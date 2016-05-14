package org.apache.cassandra.heartbeat.readhandler;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.heartbeat.status.ARResult;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * {keyspace, key, KeySubscription}
 * 
 * @author xig
 *
 */
public class ReadHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ReadHandler.class);
    public static final ReadHandler instance = new ReadHandler();
    private ConcurrentHashMap<String, ConcurrentHashMap<String, KeySubscriptions>> m_subscriptionMatrics =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, KeySubscriptions>>(); // keyspace, key, KeySubscription
    
    private ConcurrentHashMap<Long, Long> m_trace = new ConcurrentHashMap<Long, Long>();
    private ReadHandler()
    {
        //scheduleTimer();
    }
    
    
    
    public static void notifyByTs(String ksName, String inSrc, String key, final long msgTs)
    {
        instance.notifySubscriptionByTs(ksName, inSrc, key, msgTs);
    }

    public static void notifyByVn(String ksName, String inSrc, String key, final long msgVn)
    {
        instance.notifySubscriptionByVn(ksName, inSrc, key, msgVn);
    }
    
    public static void notifyBySrc(String ksName, String inSrc, final long msgTs) {
    	instance.notifyAllSubscription(ksName, inSrc, msgTs);
    }
    
    public static void sinkRead(ReadCommand cmd, Object lock, ARResult inResult)
    {
        instance.sinkSubscription(cmd, lock, inResult);
    }
    
	void notifyAllSubscription(String ksName, String inSrc, final long msgTs) {
		ConcurrentHashMap<String, KeySubscriptions> keyToSubs = m_subscriptionMatrics.get(ksName);
		if (keyToSubs != null) {
			for (Entry<String, KeySubscriptions> entry : keyToSubs.entrySet()) {
				entry.getValue().notifySubscriptionByTs(inSrc, msgTs);
			}
		}
	}

    void notifySubscriptionByTs(String ksName, String inSrc, String key, final long msgTs)
    {
        ConcurrentHashMap<String, KeySubscriptions> keyToSubs = m_subscriptionMatrics.get(ksName);
        if (keyToSubs != null)
        {
            KeySubscriptions keySubs = keyToSubs.get(key);
            if (keySubs != null)
            {
                // Notify subscriptions
                keySubs.notifySubscriptionByTs(inSrc, msgTs);
            }
        }
    }
    
    void notifySubscriptionByVn(String ksName, String inSrc, String key, final long vn)
    {
        ConcurrentHashMap<String, KeySubscriptions> keyToSubs = m_subscriptionMatrics.get(ksName);
        if (keyToSubs != null)
        {
            KeySubscriptions keySubs = keyToSubs.get(key);
            if (keySubs != null)
            {
                // Notify subscriptions
                keySubs.notifySubscriptionByVn(inSrc, vn);
            }
        }
    }
    
    void sinkSubscription(ReadCommand cmd, Object lock, ARResult inResult)
    {
        String keyStr = HBUtils.byteBufferToString(cmd.key);
        KeySubscriptions keySubs = getKeySubscriptions(cmd.ksName, keyStr);
        keySubs.addSubscription(cmd.timestamp, lock, inResult);
    }
    
    private KeySubscriptions getKeySubscriptions(String ksName, String key)
    {
        ConcurrentHashMap<String, KeySubscriptions> keyToSubs = m_subscriptionMatrics.get(ksName);
        if (keyToSubs == null)
        {
            ConcurrentHashMap<String, KeySubscriptions> temp1 = new ConcurrentHashMap<String, KeySubscriptions>();
            keyToSubs = m_subscriptionMatrics.putIfAbsent(ksName, temp1);
            if (keyToSubs == null)
                keyToSubs = temp1;
        }
        
        KeySubscriptions subs = keyToSubs.get(key);
        if (subs == null)
        {
            KeySubscriptions temp2 = new KeySubscriptions(ksName, key);
            subs = keyToSubs.putIfAbsent(key, temp2);
            if (subs == null)
                subs = temp2;
        }

        return subs;
    }
    
    private void scheduleTimer() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask()
        {
            @Override
            public void run()
            {
                showSubscriptionMatrics();
            }
        }, 1000, 3000);
    }
    
    private void showSubscriptionMatrics()
    {
        StringBuilder sb = new StringBuilder();
        boolean isEmpty = m_subscriptionMatrics.isEmpty();
        sb.append("{");
        if (!m_subscriptionMatrics.isEmpty())
        {
            for (Map.Entry<String, ConcurrentHashMap<String, KeySubscriptions>> entry : m_subscriptionMatrics.entrySet())
            {
                sb.append(entry.getKey());
                sb.append(": [ ");
                for (KeySubscriptions subs : entry.getValue().values())
                    sb.append(subs);
                sb.append("]");
                sb.append(", ");
            }
            sb.setCharAt(sb.length() - 2, ' ');
            sb.setCharAt(sb.length() - 1, '}');
        }
        else
        {
            sb.append("}");
        }
        if (!isEmpty)
            HBUtils.info("ReadHanlder -> {}", sb.toString());
    }
    
    public void removeVersionToTs(long vn, long ts)
    {
        m_trace.remove(vn, ts);
    }
}
