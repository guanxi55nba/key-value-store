package org.apache.cassandra.heartbeat.status;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.heartbeat.StatusSynMsg;
import org.apache.cassandra.heartbeat.extra.Version;
import org.apache.cassandra.heartbeat.readhandler.ReadHandler;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  This contains all the keys status from one replica node
 * 
 *  { key, status, subscription }
 * 
 * @author xig
 *
 */
public class KeyStatus
{
    private static final Logger logger = LoggerFactory.getLogger(KeyStatus.class);
    private ConcurrentHashMap<String, Status> m_keyStatusMap; // key, status
    private volatile long m_updateTs = -1;
    
    public KeyStatus()
    {
        m_keyStatusMap = new ConcurrentHashMap<String, Status>();
    }
    
    public void updateStatus(final String inSrc, StatusSynMsg inSynMsg)
    {
        ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> synMsgData = inSynMsg.getData();
        setUpdateTs(inSynMsg.getTimestamp());
        if (m_updateTs > 0)
        {
            for (Map.Entry<String, ConcurrentSkipListMap<Long, Long>> entry : synMsgData.entrySet())
            {
                // Get status object and update data
                Status status = getStatus(entry.getKey());
                status.addVnTsData(entry.getValue());

                // Notify sinked read handler
                ReadHandler.instance.notifySubscription(inSynMsg.getKsName(), entry.getKey(), m_updateTs);
            }
        }
    }
    
    public void updateStatus(final String inSrc, final String ksName, final Collection<ColumnFamily> CFS)
    {
        if (!HBUtils.SYSTEM_KEYSPACES.contains(ksName))
        {
            for (ColumnFamily cf : CFS)
            {
                Version version = HBUtils.getMutationVersion(cf);
                if (version != null)
                {
                    String key = HBUtils.getPrimaryKeyName(cf.metadata());
                    Status status = getStatus(key);
                    status.removeEntry(version.getTimestamp(), version.getTimestamp());
                }
                else
                {
                    HBUtils.error("KeyStatus::updateStatus, version value is null, ColumnFamily: {}", cf.toString());
                }
            }
        }
    }
    
    public boolean hasLatestValue(String key, long inReadTs)
    {
        boolean hasLatestValue = true;
        Status status = getStatus(key);
        if (m_updateTs <= inReadTs)
        {
            hasLatestValue = false;
            logger.info("KeyStatus::hasLatestValue, key {}, hasLatestValue == {}, status update ts [{}] <= read ts [{}]",
                    key, hasLatestValue, HBUtils.dateFormat(m_updateTs), HBUtils.dateFormat(inReadTs));
        }
        else
        {
            ConcurrentSkipListMap<Long, Long> versions = status.getVnToTsMap(); // vn: ts
            
            // if doesn't exist version whose timestamp < read ts, then this node contains the latest data
            for (Map.Entry<Long, Long> entry : versions.entrySet())
            {
                if (entry.getKey() >= 0 && entry.getValue() <= inReadTs)
                {
                    hasLatestValue = false;
                    break;
                }
            }
        }
        return hasLatestValue;
    }
    
    private Status getStatus(String key)
    {
        Status status = m_keyStatusMap.get(key);
        if (status == null)
        {
            Status temp = new Status();
            status = m_keyStatusMap.putIfAbsent(key, temp);
            if (status == null)
                status = temp;
        }
        return status;
    }
    
    public int size()
    {
        return m_keyStatusMap.values().size();
    }
    
    public void setUpdateTs(long inUpdateTs)
    {
        if (inUpdateTs > m_updateTs)
            m_updateTs = inUpdateTs;
    }

    public long getUpdateTs()
    {
        return m_updateTs;
    }
    
}
