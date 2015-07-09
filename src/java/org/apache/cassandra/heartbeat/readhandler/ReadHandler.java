package org.apache.cassandra.heartbeat.readhandler;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.heartbeat.status.ARResult;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.apache.cassandra.service.pager.Pageable;
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
    private ReadHandler()
    {
        scheduleTimer();
    }
    
    public static void notifyByTs(String ksName, String inSrc, String key, final long msgTs)
    {
        instance.notifySubscriptionByTs(ksName, inSrc, key, msgTs);
    }

    public static void notifyByVn(String ksName, String inSrc, String key, final long msgVn)
    {
        instance.notifySubscriptionByVn(ksName, inSrc, key, msgVn);
    }
    
    public static void sinkRead(Pageable page, byte[] lock, long ts, long version, ARResult inResult)
    {
        instance.sinkSubscription(page, lock, ts, version, inResult);
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
    
    void sinkSubscription(Pageable page, byte[] lock, long ts,  long version, ARResult inResult)
    {
        if (page != null)
        {
            if (page instanceof ReadCommand)
            {
                ReadCommand cmd = (ReadCommand) page;
                addSubscriptions(page, lock, cmd.ksName, cmd.key, ts, inResult );
            }
            else if (page instanceof Pageable.ReadCommands)
            {
                List<ReadCommand> readCommands = ((Pageable.ReadCommands) page).commands;
                if (readCommands.size() == 1)
                {
                    ReadCommand cmd = readCommands.get(0);
                    addSubscriptions(page, lock, cmd.ksName, cmd.key, ts, inResult);
                }
                else
                {
                    logger.info("ReadHandler::sinkSubscription, pagable is one read command list whose size > 1");
                }
            }
            
            else if (page instanceof RangeSliceCommand)
            {
                logger.info("ReadHandler::sinkSubscription, page is instance of RangeSliceCommand");
            }
            else
            {
                logger.error("ReadHandler::sinkSubscription, Unkonw pageable type");
            }
            //logger.info("sinkReadHandler: [ Pageable: {}, Timestamp: {} ", page, HBUtils.dateFormat(inTimestamp));
        }
        else
        {
            logger.info("ReadHandler::sinkSubscription, page is null");
        }
    }
    
    private void addSubscriptions(Pageable pg, byte[] lockObj, String inKsName, ByteBuffer inKey, final long ts, ARResult inResult )
    {
        KeySubscriptions keySubs = getKeySubscriptions(inKsName, inKey);
        keySubs.addSubscription(pg, lockObj, ts,inResult);
    }
    
    private KeySubscriptions getKeySubscriptions(String ksName, ByteBuffer key)
    {
        ConcurrentHashMap<String, KeySubscriptions> keyToSubs = m_subscriptionMatrics.get(ksName);
        if (keyToSubs == null)
        {
            ConcurrentHashMap<String, KeySubscriptions> temp1 = new ConcurrentHashMap<String, KeySubscriptions>();
            keyToSubs = m_subscriptionMatrics.putIfAbsent(ksName, temp1);
            if (keyToSubs == null)
                keyToSubs = temp1;
        }
        
        String keyStr = HBUtils.byteBufferToString(key);
        
        KeySubscriptions subs = keyToSubs.get(key);
        if (subs == null)
        {
            KeySubscriptions temp2 = new KeySubscriptions(ksName, key);
            subs = keyToSubs.putIfAbsent(keyStr, temp2);
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
        }, 10000, 5000);
    }
    
    private void showSubscriptionMatrics()
    {
        for (Map.Entry<String, ConcurrentHashMap<String, KeySubscriptions>> entry : m_subscriptionMatrics.entrySet())
        {
            StringBuilder sb = new StringBuilder();
            sb.append(entry.getKey());
            sb.append(": { ");
            if (entry.getValue().size() > 0)
            {
                for (Map.Entry<String, KeySubscriptions> subEntry : entry.getValue().entrySet())
                {
                    sb.append(subEntry.getKey());
                    sb.append(": ");
                    sb.append(subEntry.getValue().size());
                    sb.append(", ");
                }
                sb.setCharAt(sb.length() - 2, ' ');
                sb.setCharAt(sb.length() - 1, '}');
            }
            else
            {
                sb.append("}");
            }
            HBUtils.info("ReadHanlder -> {}",sb.toString());
        }
    }
}
