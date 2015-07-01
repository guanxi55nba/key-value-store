package org.apache.cassandra.heartbeat.readhandler;

import java.util.List;
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
    ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>>> m_subscriptionMatrics;

    public static final ReadHandler instance = new ReadHandler();

    private ReadHandler()
    {
        m_subscriptionMatrics = new ConcurrentHashMap<String, ConcurrentHashMap<String,ConcurrentSkipListSet<Subscription>>>();
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

    /**
     * Check whether there is a subscription could be awake
     * 
     * @param inMsg
     */
    public void notifySubscription(StatusSynMsg inMsg)
    {
        String ksName = ConfReader.instance.getKeySpaceName();
        ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> data = inMsg.getData();
        for (String key : data.keySet())
        {
            notifySubscription(ksName, key, inMsg.getTimestamp());
        }
    }

    public void notifySubscription(String ksName, String key, final long msgTs)
    {
        ConcurrentSkipListSet<Subscription> vnToSubs = getSubscriptions(ksName, key);
        for (Subscription sub : vnToSubs)
        {
            if (sub.getTimestamp() <= msgTs)
            {
                // if (ConfReader.instance.isLogEnabled())
                // logger.info("ReadHandler.notifySubscription: ts<=timestamp");
                if (StatusMap.instance.hasLatestValue(sub.getPageable(), sub.getTimestamp()))
                {
                    vnToSubs.remove(sub);
                    synchronized (sub.getLockObject())
                    {
                        sub.getLockObject().notify();
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
                {
                    String key = HBUtils.byteBufferToString(cmd.ksName, cmd.cfName, cmd.key);
                    updateSubscriptions(cmd.ksName, key, inTimestamp, subscription);
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
            logger.info("sinkReadHandler: [ Pageable: {}, Timestamp: {} ", page, HBUtils.dateFormat(inTimestamp));
        }
        else
        {
            logger.info("ReadHandler::sinkReadHandler, page is null");
        }
    }

    private ConcurrentSkipListSet<Subscription> getSubscriptions(String inKsName,
            String inKey)
    {
        ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>> newKeyToVersionSubs = new ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>>();
        ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>> keyToVersionSubs = m_subscriptionMatrics.put(inKsName, newKeyToVersionSubs);
        if (keyToVersionSubs == null)
            keyToVersionSubs = newKeyToVersionSubs;

        ConcurrentSkipListSet<Subscription> newSubs = new ConcurrentSkipListSet<Subscription>();
        ConcurrentSkipListSet<Subscription> subs = keyToVersionSubs.putIfAbsent(inKey, newSubs);
        if (subs == null)
            subs = newSubs;
        return subs;
    }

    private void updateSubscriptions(String inKsName, String inKey, long inTs, Subscription inSub)
    {
        ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>> newKeyToVersionSubs = new ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>>();
        ConcurrentHashMap<String, ConcurrentSkipListSet<Subscription>> keyToVersionSubs = m_subscriptionMatrics.put(inKsName, newKeyToVersionSubs);
        if (keyToVersionSubs == null)
            keyToVersionSubs = newKeyToVersionSubs;

        ConcurrentSkipListSet<Subscription> newSubs = new ConcurrentSkipListSet<Subscription>();
        ConcurrentSkipListSet<Subscription> subs = keyToVersionSubs.putIfAbsent(inKey, newSubs);
        if (subs == null)
            subs = newSubs;

        subs.add(inSub);
    }
}
