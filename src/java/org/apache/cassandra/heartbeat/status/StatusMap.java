package org.apache.cassandra.heartbeat.status;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.heartbeat.StatusSynMsg;
import org.apache.cassandra.heartbeat.readhandler.Subscription;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.apache.cassandra.service.pager.Pageable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stand alone component to keep status msg related info map
 * 
 *  { keyspace, src, keystatus } 
 * 
 * @author XiGuan
 * 
 */
public class StatusMap
{
    private static final Logger logger = LoggerFactory.getLogger(StatusMap.class);
    ConcurrentHashMap<String, ConcurrentHashMap<String, KeyStatus>> m_currentEntries = 
            new ConcurrentHashMap<String, ConcurrentHashMap<String, KeyStatus>>(); // keyspace, src, keystatus
    public static final StatusMap instance = new StatusMap();

    private StatusMap()
    {
        scheduleTimer();
    }
    
    /**
     * Used to update status msg based on one StatusSynMsg
     * 
     * @param inSrcName
     * @param inSynMsg
     */
    public void updateStatusMap(final String inSrcName, final StatusSynMsg inSynMsg)
    {
        if (inSynMsg != null)
        {
            KeyStatus keyStatus = getKeyStatus(inSynMsg.getKsName(), inSrcName);
            keyStatus.updateStatus(inSrcName, inSynMsg);
        }
        else
        {
            HBUtils.error("inSynMsg is null");
        }
    }
    
    public void removeEntry(String inSrcName, final Mutation inMutation)
    {
        if (inSrcName != null && inMutation != null)
        {
            String ksName = inMutation.getKeyspaceName();
            if (!HBUtils.SYSTEM_KEYSPACES.contains(ksName))
            {
                KeyStatus keyStatus = getKeyStatus(ksName, inSrcName);
                keyStatus.updateStatus(inSrcName, ksName, inMutation.getColumnFamilies());
            }
        }
        else
        {
            logger.debug("removeEntry method: inSrcName or inMutation is null");
        }
    }
    
    private KeyStatus getKeyStatus(final String ksName, final String srcName)
    {
        ConcurrentHashMap<String, KeyStatus> srcToKeyStatus = m_currentEntries.get(ksName);
        if (srcToKeyStatus == null)
        {
            ConcurrentHashMap<String, KeyStatus> temp1 = new ConcurrentHashMap<String, KeyStatus>();
            srcToKeyStatus = m_currentEntries.putIfAbsent(ksName, temp1);
            if (srcToKeyStatus == null)
                srcToKeyStatus = temp1;
        }

        KeyStatus keyStatus = srcToKeyStatus.get(srcName);
        if (keyStatus == null)
        {
            KeyStatus temp2 = new KeyStatus();
            keyStatus = srcToKeyStatus.putIfAbsent(srcName, temp2);
            if (keyStatus == null)
                keyStatus = temp2;
        }

        return keyStatus;
    }
    
    public boolean hasLatestValue(Subscription inSubs)
    {
        return hasLatestValue(inSubs.getPageable(), inSubs.getTimestamp());
    }

    /**
     * @param pageable
     * @param inTimestamp
     * @return
     */
    public boolean hasLatestValue(Pageable pageable, long inTimestamp)
    {
        boolean hasLatestValue = true;
        if (pageable instanceof ReadCommand)
        {
            ReadCommand cmd = (ReadCommand) pageable;
            if (!hasLatestValueImpl(cmd.ksName, cmd.key, inTimestamp))
                hasLatestValue = false;
        }
        else if (pageable instanceof Pageable.ReadCommands)
        {
            List<ReadCommand> readCommands = ((Pageable.ReadCommands) pageable).commands;
            for (ReadCommand cmd : readCommands)
            {
                if (!hasLatestValueImpl(cmd.ksName, cmd.key, inTimestamp))
                {
                    hasLatestValue = false;
                    break;
                }
            }
        }
        else if (pageable instanceof RangeSliceCommand)
        {
            logger.error("StatusMap::hasLatestValue, RangeSliceCommand doesn't support");
        }
        else
        {
            hasLatestValue = false;
            logger.error("StatusMap::hasLatestValue, Unkonw pageable type");
        }
        return hasLatestValue;
    }

    private boolean hasLatestValueImpl(String inKSName, ByteBuffer inKey, long inReadTs)
    {
        boolean hasLatestValue = true;
        List<InetAddress> replicaList = HBUtils.getReplicaListExcludeLocal(inKSName, inKey);
        String inKeyStr = HBUtils.byteBufferToString(inKey);
        for (InetAddress sourceName : replicaList)
        {
            KeyStatus keyStatus = getKeyStatus(inKSName, sourceName.getHostAddress());
            if (!keyStatus.hasLatestValue(inKeyStr, inReadTs))
            {
                hasLatestValue = false;
                break;
            }
        }
        return hasLatestValue;
    }
    
    private void scheduleTimer()
    {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new java.util.TimerTask()
        {
            @Override
            public void run()
            {
                showStatusNo();
            }
        }, 5000, 5000);
    }
    
    private void showStatusNo()
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, ConcurrentHashMap<String, KeyStatus>> entry : m_currentEntries.entrySet())
        {
            sb.append(entry.getKey());
            sb.append(", ");
            sb.append("status number: ( ");
            sb.append(entry.getValue().keySet().size());
            sb.append(", ");
            sb.append(entry.getValue().values().size());
            sb.append(" ) ");
        }
        logger.info("StatusMap -> {}", sb.toString());
    }

}
