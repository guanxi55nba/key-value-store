package org.apache.cassandra.heartbeat.status;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.heartbeat.StatusSynMsg;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.apache.cassandra.service.pager.Pageable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

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
    HashMap<String, KeyResult> m_emptyBlockMap = Maps.newHashMap();

    private StatusMap()
    {
        //scheduleTimer();
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
    
    /**
     * Called when a new mutation arrives
     * 
     * @param inSrcName
     * @param inMutation
     */
    public void removeEntry(String inSrcName, final Mutation inMutation)
    {
        if (inSrcName != null && inMutation != null)
        {
            String ksName = inMutation.getKeyspaceName();
            if (!HBUtils.SYSTEM_KEYSPACES.contains(ksName))
            {
                KeyStatus keyStatus = getKeyStatus(ksName, inSrcName);
                keyStatus.removeEntry(inSrcName, ksName, inMutation.getColumnFamilies());
            }
        }
        else
        {
            logger.debug("removeEntry method: inSrcName or inMutation is null");
        }
    }
    
    /**
     * @param pageable
     * @param readTs
     * @return
     */
    public ARResult hasLatestValue(Pageable pageable, long readTs)
    {
        ARResult arrResult = null;
        if (pageable instanceof ReadCommand)
        {
            ReadCommand cmd = (ReadCommand) pageable;
            arrResult = hasLatestValueImpl(cmd.ksName, cmd.key, readTs);
            if (!arrResult.value())
                HBUtils.info(" [Read {} @ '{}'], doesn't have lstest data, since -> {} ", arrResult.m_key, HBUtils.dateFormat(readTs), arrResult);
        }
        else if (pageable instanceof Pageable.ReadCommands)
        {
            List<ReadCommand> readCommands = ((Pageable.ReadCommands) pageable).commands;
            if (readCommands.size() == 1)
            {
                ReadCommand cmd = readCommands.get(0);
                arrResult = hasLatestValueImpl(cmd.ksName, cmd.key, readTs);
                if (!arrResult.value())
                    HBUtils.info(" [Read {} @ '{}'], doesn't have lstest data, since -> {} ", arrResult.m_key, HBUtils.dateFormat(readTs), arrResult);
            }
            else if(readCommands.size()>1)
            {
                logger.error("StatusMap::hasLatestValue, Multiple read commands is not supported");
            }
        }
        else if (pageable instanceof RangeSliceCommand)
        {
            logger.error("StatusMap::hasLatestValue, RangeSliceCommand doesn't support");
        }
        else
        {
            logger.error("StatusMap::hasLatestValue, Unkonw pageable type");
        }
        return arrResult == null ? new ARResult("", m_emptyBlockMap) : arrResult;
    }
    
    /**
     * Used to check whether has latest value on one src
     * 
     * @param inKsName
     * @param inSrc
     * @param inKey
     * @param inReadTs
     * @return
     */
    public KeyResult hasLatestValueOnOneSrc(String inKsName, String inSrc, String inKey, long inReadTs)
    {
        KeyStatus keyStatus = getKeyStatus(inKsName, inSrc);
        return keyStatus.hasLatestValue(inKey, inReadTs);
    }
    
    private ARResult hasLatestValueImpl(String inKSName, ByteBuffer inKey, long inReadTs)
    {
        List<InetAddress> replicaList = HBUtils.getReplicaListExcludeLocal(inKSName, inKey);
        HashMap<String, KeyResult> blockMap = Maps.newHashMap();
        String inKeyStr = HBUtils.byteBufferToString(inKey);
        for (InetAddress sourceName : replicaList)
        {
            String sourceStr = sourceName.getHostAddress();
            KeyStatus keyStatus = getKeyStatus(inKSName, sourceStr);
            KeyResult keyResult = keyStatus.hasLatestValue(inKeyStr, inReadTs);
            if (!keyResult.value())
                blockMap.put(sourceStr, keyResult);
        }
        
        return new ARResult(inKeyStr, blockMap);
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
        if (m_currentEntries.size() > 0)
        {
            sb.append("{ ");
            for (Map.Entry<String, ConcurrentHashMap<String, KeyStatus>> entry : m_currentEntries.entrySet())
            {
                sb.append(" [ ");
                sb.append(entry.getKey());
                sb.append(": ");
                sb.append("{");
                if (entry.getValue().size() > 0)
                {
                    for (Map.Entry<String, KeyStatus> subEntry : entry.getValue().entrySet())
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
                sb.append("],");
            }
            sb.setCharAt(sb.length() - 1, ' ');
            sb.append("}");
        }
        HBUtils.info("StatusMap -> ({} ) ", sb.toString());
    }

}
