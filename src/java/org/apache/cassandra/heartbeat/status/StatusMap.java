package org.apache.cassandra.heartbeat.status;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.heartbeat.StatusSynMsg;
import org.apache.cassandra.heartbeat.readhandler.ReadHandler;
import org.apache.cassandra.heartbeat.utils.ConfReader;
import org.apache.cassandra.heartbeat.utils.HBUtils;
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
	public void updateStatusMap(final String inSrcName, final StatusSynMsg inSynMsg) {
		if(ConfReader.isLogEnabled())
			logger.info("Receive status msg to {} from {}", inSynMsg, inSrcName);
		
		if (inSynMsg != null) {
			KeyStatus keyStatus = getKeyStatus(inSynMsg.getKsName(), inSrcName);
			if (inSynMsg.getTimestamp() <= 0)
				return;

			keyStatus.setUpdateTs(inSynMsg.getTimestamp());
			ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>> synMsgData = inSynMsg.getData();
			// Notify sinked read handler
			for (Map.Entry<String, Status> entry : keyStatus.getKeyStatusMapCopy().entrySet())
				ReadHandler.notifyByTs(inSynMsg.getKsName(), inSrcName, entry.getKey(), keyStatus.getUpdateTs());
			
			if(synMsgData.isEmpty())
				return;
			
			for (Entry<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>> keySrcVnMapEntry : synMsgData.entrySet()) {
				if (keySrcVnMapEntry.getValue().isEmpty())
					continue;

				ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> srcVnMap = keySrcVnMapEntry.getValue();
				for (Entry<String, ConcurrentSkipListMap<Long, Long>> srcVnMapEntry : srcVnMap.entrySet()) {
					keyStatus.updateStatus(inSynMsg.getKsName(), keySrcVnMapEntry.getKey(), srcVnMapEntry.getKey(), srcVnMapEntry.getValue());
				}
			}
		} else {
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
     * <-- called by {@link SelectStatement.execute}, {@link ReadVerbHandler.doVerb}
     * 
     * @param pageable
     * @param readTs
     * @return
     */
    public ARResult hasLatestValue(ReadCommand cmd)
    {
        ARResult arrResult = hasLatestValueImpl(cmd.ksName, cmd.key, cmd.timestamp);
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
