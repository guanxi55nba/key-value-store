package org.apache.cassandra.heartbeat.status;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.heartbeat.StatusSynMsg;
import org.apache.cassandra.heartbeat.extra.Version;
import org.apache.cassandra.heartbeat.readhandler.ReadHandler;
import org.apache.cassandra.heartbeat.utils.ConfReader;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.apache.cassandra.service.pager.Pageable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Stand alone component to keep status msg related info map
 * 
 * @author XiGuan
 * 
 */
public class StatusMap
{
    private static final Logger logger = LoggerFactory.getLogger(StatusMap.class);
    private static final long DEFAULT_LATEST_VN = -2;
    ConcurrentHashMap<String, ConcurrentHashMap<String, Status>> m_currentEntries; // src, key,statusmap
    public static final StatusMap instance = new StatusMap();
    

    private StatusMap()
    {
        m_currentEntries = new ConcurrentHashMap<String, ConcurrentHashMap<String, Status>>();
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
            ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> synMsgData = inSynMsg.getData();
            long timestamp = inSynMsg.getTimestamp();
            
            ConcurrentHashMap<String, Status> keyToStatusMap = getKeyToStatusMap(m_currentEntries, inSrcName);
            for (Map.Entry<String, ConcurrentSkipListMap<Long, Long>> entry : synMsgData.entrySet())
            {
                // Get key value
                String key = entry.getKey();
                ConcurrentSkipListMap<Long, Long> valueMap = entry.getValue();
                
                // Get status object and update data 
                Status status = getStatusFromKeyToStautsMap(keyToStatusMap, key);
                status.addVnTsData(valueMap, timestamp);
                
                // Notify sinked read handler
                ReadHandler.instance.notifySubscription(inSynMsg.getKsName(), key, timestamp);
            }
            
            for (Map.Entry<String, Status> entry : keyToStatusMap.entrySet())
            {
                String key = entry.getKey();
                if (!synMsgData.contains(key))
                {
                    entry.getValue().setUpdateTs(timestamp);
                    
                    // Notify sinked read handler
                    ReadHandler.instance.notifySubscription(inSynMsg.getKsName(), key, timestamp);
                }
            }
        }
        else
        {
            if (ConfReader.instance.isLogEnabled())
                logger.error("inSynMsg is null");
        }
    }

    public void removeEntry(String inSrcName, final Mutation inMutation)
    {
        if (inSrcName != null && inMutation != null)
        {
            String ksName = inMutation.getKeyspaceName();
            if (!HBUtils.SYSTEM_KEYSPACES.contains(ksName))
            {
                for (ColumnFamily cf : inMutation.getColumnFamilies())
                {
                    Version version = HBUtils.getMutationVersion(cf);
                    if (version != null)
                    {
                        String key = HBUtils.getPrimaryKeyName(cf.metadata());
                        Status status = getStatusFromEntryMap(m_currentEntries, key, inSrcName);
                        status.removeEntry(version.getTimestamp(), version.getTimestamp());
                    }
                    else if (ConfReader.instance.isLogEnabled())
                    {
                        logger.error("StatusMap::removeEntry, version value is null, mutation: {}", inMutation);
                    }
                }
            }
        }
        else
        {
            logger.debug("removeEntry method: inSrcName or inMutation is null");
        }
    }

    /**
     * @param pageable
     * @param inTimestamp
     * @return
     */
    public boolean hasLatestValue(Pageable pageable, long inTimestamp)
    {
        boolean hasLatestValue = true;
        boolean isReadCommands = pageable instanceof Pageable.ReadCommands;
        boolean isReadcommand = !isReadCommands && pageable instanceof ReadCommand;
        if (isReadCommands || isReadcommand)
        {
            List<ReadCommand> readCommands = (isReadcommand) ? Lists.newArrayList((ReadCommand) pageable) : ((Pageable.ReadCommands) pageable).commands;
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
            Status status = getStatusFromEntryMap(m_currentEntries, inKeyStr, sourceName.getHostAddress());
            long updateTs = status.getUpdateTs();
            if (updateTs <= inReadTs)
            {
                hasLatestValue = false;
                logger.info("StatusMap::hasLatestValueImpl,  hasLatestValue == {}, status latest update ts [{}] <= read ts [{}]", hasLatestValue,
                        HBUtils.dateFormat(updateTs), HBUtils.dateFormat(inReadTs));
            }
            else
            {
                //long latestVersion = DEFAULT_LATEST_VN;
                ConcurrentSkipListMap<Long, Long> versions = status.getVnToTsMap(); // vn: ts
                // if doesn't exist entry whose timestamp < inTimestamp, then this node contains the latest data
                for (Map.Entry<Long, Long> entry : versions.entrySet())
                {
                    if (entry.getKey() >= 0)
                    {
                        long ts = entry.getValue();
                        if (ts <= inReadTs)
                        {
                            hasLatestValue = false;
                            break;
                            /*if (vn > latestVersion)
                                latestVersion = vn;*/
                        }
                    }
                }
                /*if (latestVersion != DEFAULT_LATEST_VN) // Wait for mutation
                {
                    hasLatestValue = false;
                    logger.info("StatusMap::hasLatestValueImpl, hasLatestValue == false, latestVersion == ", latestVersion);
                }*/
            }
        }
        return hasLatestValue;
    }
    
    private ConcurrentHashMap<String, Status> getKeyToStatusMap(ConcurrentHashMap<String, ConcurrentHashMap<String, Status>> inEntries, String inSrc)
    {
        ConcurrentHashMap<String, Status> newKeyToStatus = new ConcurrentHashMap<String, Status>();
        ConcurrentHashMap<String, Status> keyToStatus = inEntries.putIfAbsent(inSrc, newKeyToStatus);
        if (keyToStatus == null)
            keyToStatus = newKeyToStatus;
        return keyToStatus;
    }
    
    private Status getStatusFromKeyToStautsMap(ConcurrentHashMap<String, Status> keyToStatus, String inKey)
    {
        Status newStatus = new Status();
        Status status = keyToStatus.putIfAbsent(inKey, newStatus);
        if (status == null)
            status = newStatus;
        return status;
    }

    private Status getStatusFromEntryMap(ConcurrentHashMap<String, ConcurrentHashMap<String, Status>> inEntries,
            String inKey, String inSrc)
    {
        ConcurrentHashMap<String, Status> newKeyToStatus = new ConcurrentHashMap<String, Status>();
        ConcurrentHashMap<String, Status> keyToStatus = inEntries.putIfAbsent(inSrc, newKeyToStatus);
        if (keyToStatus == null)
            keyToStatus = newKeyToStatus;
        
        Status newStatus = new Status();
        Status status = keyToStatus.putIfAbsent(inKey, newStatus);
        if (status == null)
            status = newStatus;

        return status;
    }

//    private void updateEntryMapStatus(ConcurrentHashMap<String, ConcurrentHashMap<String, Status>> inEntries,
//            String inKey, String inSrc, Long inVn, Long inTs, long inMsgTs)
//    {
//        ConcurrentHashMap<String, Status> newSrcStatusMap = new ConcurrentHashMap<String, Status>();
//        ConcurrentHashMap<String, Status> srcToStatus = inEntries.putIfAbsent(inKey, newSrcStatusMap);
//        if (srcToStatus == null)
//            srcToStatus = newSrcStatusMap;
//
//        Status newStatus = new Status();
//        Status status = srcToStatus.putIfAbsent(inSrc, newStatus);
//        if (status == null)
//            status = newStatus;
//        status.updateVnTsData(inVn, inTs);
//        ;
//        status.setUpdateTs(inMsgTs);
//    }
    
//    protected void updateEntryMapStatus(ConcurrentHashMap<String, ConcurrentHashMap<String, Status>> inEntries,
//            String inKey, String inSrc, ConcurrentSkipListMap<Long, Long> inVnToTs, long inTs)
//    {
//        ConcurrentHashMap<String, Status> newSrcStatusMap = new ConcurrentHashMap<String, Status>();
//        ConcurrentHashMap<String, Status> srcToStatus = inEntries.putIfAbsent(inKey, newSrcStatusMap);
//        if (srcToStatus == null)
//            srcToStatus = newSrcStatusMap;
//
//        Status newStatus = new Status(inTs, inVnToTs);
//        Status status = srcToStatus.putIfAbsent(inSrc, newStatus);
//        if (status == null)
//            status = newStatus;
//
//        synchronized (status)
//        {
//            status.updateVnTsData(inVnToTs);
//            status.setUpdateTs(inTs);
//        }
//    }

    // @Deprecated
    // private boolean hasLatestValueImpl(String inKSName, String inKey, long inTimestamp) {
    // boolean hasLatestValue = true;
    // Set<String> dataCenterNames = HBUtils.getDataCenterNames(inKSName);
    // dataCenterNames.remove(DatabaseDescriptor.getLocalDataCenter());
    // for (String dcName : dataCenterNames) {
    // Status status = m_currentEntries.get(inKey, dcName);
    // if (status == null) {
    // hasLatestValue = false;
    // logger.info("StatusMap::hasLatestValueImpl, hasLatestValue == false, status == null");
    // } else {
    // if (status.getUpdateTs() <= inTimestamp) {
    // hasLatestValue = false;
    // logger.info("StatusMap::hasLatestValueImpl, {}, update ts {} <= inTimestamp {}", hasLatestValue,
    // HBUtils.dateFormat(status.getUpdateTs()), HBUtils.dateFormat(inTimestamp) );
    // } else {
    // // vn: ts
    // TreeMap<Long, Long> versions = status.getVersionTsMap();
    // // if doesn't exist entry whose timestamp < inTimestamp,
    // // then row is the latest in this datacenter
    // long latestVersion = -2;
    // for (Map.Entry<Long, Long> entry : versions.entrySet()) {
    // long vn = entry.getKey();
    // if(vn>=0) {
    // long ts = entry.getValue();
    // if (ts <= inTimestamp) {
    // hasLatestValue = false;
    // if (vn > latestVersion)
    // latestVersion = vn;
    // }
    // }
    // }
    // if (latestVersion != -2) {
    // // Wait for mutation
    // hasLatestValue = false;
    // logger.info("StatusMap::hasLatestValueImpl, hasLatestValue == false, latestVersion == ", latestVersion);
    // }
    // }
    // }
    // }
    // return hasLatestValue;
    // }
}
