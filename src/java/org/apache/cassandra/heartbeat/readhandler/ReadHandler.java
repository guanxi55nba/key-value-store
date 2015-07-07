package org.apache.cassandra.heartbeat.readhandler;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.ReadCommand;
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

    /**
     * Check whether there is a subscription could be awake
     * 
     * @param inMutation
     */
    public void notifySubscription(Mutation inMutation)
    {
        // Get mutation Ts
        String ksName = inMutation.getKeyspaceName();
        if (!HBUtils.SYSTEM_KEYSPACES.contains(ksName))
        {
            for (ColumnFamily col : inMutation.getColumnFamilies())
            {
                String key = HBUtils.getPrimaryKeyName(col.metadata());
                Cell cell = col.getColumn(HBUtils.VERSION_CELLNAME);
                notifySubscription(ksName, key, cell.timestamp());
            }
        }
    }

    public void notifySubscription(String ksName, String key, final long msgTs)
    {
        // Get key subscriptions
        KeySubscriptions keySubs = getKeySubscriptions(ksName, key);

        // Notify subscriptions
        keySubs.notifySubscription(msgTs);
    }
    
    public void sinkSubscription(Pageable page, final long ts, byte[] lock, long version)
    {
        if (page != null)
        {
            Subscription subscription = new Subscription(page, ts, lock, version);
            if (page instanceof ReadCommand)
            {
                ReadCommand cmd = (ReadCommand) page;
                addSubscriptions(cmd.ksName, HBUtils.byteBufferToString(cmd.key), ts, subscription);
            }
            else if (page instanceof Pageable.ReadCommands)
            {
                List<ReadCommand> readCommands = ((Pageable.ReadCommands) page).commands;
                for (ReadCommand cmd : readCommands)
                {
                    addSubscriptions(cmd.ksName, HBUtils.byteBufferToString(cmd.key), ts, subscription);
                }
            }
            
            else if (page instanceof RangeSliceCommand)
            {
                logger.info("ReadHandler::sinkReadHandler, page is instance of RangeSliceCommand");
            }
            else
            {
                logger.error("StatusMap::hasLatestValue, Unkonw pageable type");
            }
            //logger.info("sinkReadHandler: [ Pageable: {}, Timestamp: {} ", page, HBUtils.dateFormat(inTimestamp));
        }
        else
        {
            logger.info("ReadHandler::sinkReadHandler, page is null");
        }
    }
    
    private void addSubscriptions(String inKsName, String inKey, final long ts, Subscription inSub)
    {
        KeySubscriptions keySubs = getKeySubscriptions(inKsName, inKey);
        keySubs.addSubscription(ts, inSub);
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
            KeySubscriptions temp2 = new KeySubscriptions();
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
        }, 10000, 5000);
    }
    
    private void showSubscriptionMatrics()
    {
//        for (Map.Entry<String, ConcurrentHashMap<String, KeySubscriptions>> entry : m_subscriptionMatrics.entrySet())
//        {
//            StringBuilder sb = new StringBuilder();
//            sb.append(entry.getKey());
//            sb.append(": { ");
//            if (entry.getValue().size() > 0)
//            {
//                for (Map.Entry<String, KeySubscriptions> subEntry : entry.getValue().entrySet())
//                {
//                    sb.append(subEntry.getKey());
//                    sb.append(": ");
//                    sb.append(subEntry.getValue().size());
//                    sb.append(", ");
//                }
//                sb.setCharAt(sb.length() - 2, ' ');
//                sb.setCharAt(sb.length() - 1, '}');
//            }
//            else
//            {
//                sb.append("}");
//            }
//            HBUtils.info("ReadHanlder -> {}",sb.toString());
//        }
    }
}
