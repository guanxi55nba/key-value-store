package org.apache.cassandra.heartbeat.readhandler;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.heartbeat.StatusSynMsg;
import org.apache.cassandra.heartbeat.status.StatusMap;
import org.apache.cassandra.heartbeat.utils.ConfReader;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.apache.cassandra.service.pager.Pageable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * 
 * {keyspace, key, { ts, subscription}}
 * 
 * @author xig
 *
 */
public class ReadHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ReadHandler.class);
    ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>>> m_subscriptionMatrics 
                    = new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>>>();

    public static final ReadHandler instance = new ReadHandler();
    Timer timer = new Timer();

    private ReadHandler()
    {
        timer.schedule(new TimerTask()
        {
            @Override
            public void run()
            {
                for (Map.Entry<String, ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>>> entry : m_subscriptionMatrics.entrySet())
                {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Keyspace ");
                    sb.append(entry.getKey());
                    sb.append(" { ");
                    for (Map.Entry<String, ConcurrentSkipListSet<Subscription>> subEntry : entry.getValue().entrySet())
                    {
                        sb.append("src ");
                        sb.append(subEntry.getKey());
                        sb.append(" { ");
                        for (Subscription sub : subEntry.getValue())
                        {
                            sb.append("key set: ");
                            sb.append(HBUtils.getReadCommandRelatedKeySpaceNames(sub.getPageable()));
                            sb.append(", ");
                            sb.append("timestamp:  ");
                            sb.append(HBUtils.dateFormat(sub.getTimestamp()));
                        }
                        sb.append(" } ");
                        subEntry.getKey();
                    }
                    sb.append(" } ");
                    logger.info(sb.toString());
                }
            }
        }, 20000);
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
                final long timestamp = cell.timestamp();
                notifySubscription(ksName, key, timestamp);
            }
        }
    }

    public void notifySubscription(String ksName, String key, final long msgTs)
    {
        ConcurrentSkipListSet<Subscription> subs = getSubscriptions(ksName, key);
        if (subs != null)
        {
            for (Subscription sub : subs)
            {
                if (sub.getTimestamp() <= msgTs)
                {
                    // if (ConfReader.instance.isLogEnabled())
                    // logger.info("ReadHandler.notifySubscription: ts<=timestamp");
                    if (StatusMap.instance.hasLatestValue(sub.getPageable(), sub.getTimestamp()))
                    {
                        // logger.info("Arraive status message with timestamp {}", HBUtils.dateFormat(msgTs));
                        synchronized (sub.getLockObject())
                        {
                            sub.getLockObject().notify();
                            subs.remove(sub);
                        }
                    }
                }
            }
        }
    }
    
    public void sinkReadHandler(Pageable page, long inTimestamp, byte[] inBytes)
    {
        if (page != null)
        {
            Subscription subscription = new Subscription(page, inTimestamp, inBytes);
            boolean isReadCommands = page instanceof Pageable.ReadCommands;
            boolean isReadcommand = !isReadCommands && page instanceof ReadCommand;
            if (isReadCommands || isReadcommand)
            {
                List<ReadCommand> readCommands = (isReadcommand) ? Lists.newArrayList((ReadCommand) page): ((Pageable.ReadCommands) page).commands;
                for (ReadCommand cmd : readCommands)
                    addSubscriptions(cmd.ksName, HBUtils.byteBufferToString(cmd.key), subscription);
            }
            else if (page instanceof RangeSliceCommand)
            {
                logger.info("ReadHandler::sinkReadHandler, page is instance of RangeSliceCommand");
            }
            else
            {
                logger.error("StatusMap::hasLatestValue, Unkonw pageable type");
            }
            logger.info("sinkReadHandler: [ Pageable: {}, Timestamp: {} ", page, HBUtils.dateFormat(inTimestamp));
        }
        else
        {
            logger.info("ReadHandler::sinkReadHandler, page is null");
        }
    }

    private ConcurrentSkipListSet<Subscription> getSubscriptions(String inKsName, String inKey)
    {
        ConcurrentSkipListSet<Subscription> subs = null;
        ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>> keyToVersionSubs = m_subscriptionMatrics.get(inKsName);
        if (keyToVersionSubs != null)
            subs = keyToVersionSubs.get(inKey);
        return subs;
    }

    private void addSubscriptions(String inKsName, String inKey, Subscription inSub)
    {
        ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>> newKeyToVersionSubs = new ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>>();
        ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>> keyToVersionSubs = m_subscriptionMatrics.putIfAbsent(inKsName, newKeyToVersionSubs);
        if (keyToVersionSubs == null)
            keyToVersionSubs = newKeyToVersionSubs;

        ConcurrentSkipListSet<Subscription> newSubs = new ConcurrentSkipListSet<Subscription>();
        ConcurrentSkipListSet<Subscription> subs = keyToVersionSubs.putIfAbsent(inKey, newSubs);
        if (subs == null)
            subs = newSubs;
        
        subs.add(inSub);
    }
    
    /**
     * Check whether there is a subscription could be awake
     * 
     * @param inMsg
     */
    /*public void notifySubscription(StatusSynMsg inMsg)
    {
        String ksName = ConfReader.instance.getKeySpaceName();
        ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> data = inMsg.getData();
        for (String key : data.keySet())
        {
            notifySubscription(ksName, key, inMsg.getTimestamp());
        }
    }*/
}
