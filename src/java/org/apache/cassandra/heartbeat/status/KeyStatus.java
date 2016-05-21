package org.apache.cassandra.heartbeat.status;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.heartbeat.StatusSynMsg;
import org.apache.cassandra.heartbeat.extra.HBConsts;
import org.apache.cassandra.heartbeat.extra.Version;
import org.apache.cassandra.heartbeat.readhandler.ReadHandler;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;


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
    private ConcurrentHashMap<String, Status> m_keyStatusMap = new ConcurrentHashMap<String, Status>(640,8,64); // key, status
    private long m_updateTs = -1;
    
    public KeyStatus()
    {
    }
    
    
	public void updateStatusV1(final String inSrc, StatusSynMsg inSynMsg)
    {
		HashMap<String, HashMap<String, TreeMap<Long, Long>>> synMsgData = inSynMsg.getDataCopy();
        setUpdateTsV1(inSynMsg.getTimestamp());
        String ksName = inSynMsg.getKsName();
        if (m_updateTs > 0)
        {
            // Notify sinked read handler
        	ReadHandler.notifyBySrc(ksName, inSrc, m_updateTs);
            
            for (Map.Entry<String, HashMap<String, TreeMap<Long, Long>>> keySrcVnMapEntry : synMsgData.entrySet())
            {
            	String key = keySrcVnMapEntry.getKey();
            	
                // Get status object and update data
                Status status = getStatus(key);
                
				for (Map.Entry<String, TreeMap<Long, Long>> srcVnMapEntry : keySrcVnMapEntry.getValue().entrySet()) {
					String src = srcVnMapEntry.getKey();
					if (inSrc.equals(src)) {
						status.addVnTsData(srcVnMapEntry.getValue());
					} else if (src.contains(HBConsts.COORDINATOR)) {
						status.addCtrlVnTs(src, srcVnMapEntry.getValue());
					}
				}
				
                // Notify sinked read handler
                ReadHandler.notifyByTs(ksName, inSrc, keySrcVnMapEntry.getKey(), m_updateTs);
            }
        }
    }
	
	public void updateStatus(final String ksName, final String inKey, final String inSrc, TreeMap<Long, Long> inVnTsData) {
		// Get status object and update data
		Status status = getStatus(inKey);
		status.addVnTsData(inVnTsData);

		// Notify sinked read handler
		ReadHandler.notifyByTs(ksName, inSrc, inKey, m_updateTs);
	}
    
	public void removeEntry(final String inSrc, final String ksName, final Collection<ColumnFamily> CFS) {
		for (ColumnFamily cf : CFS) {
			Version version = HBUtils.getMutationVersion(cf);
			if (version != null) {
				String key = HBUtils.getPrimaryKeyName(cf.metadata());
				Status status = getStatus(key);
				status.removeEntry(version.getLocalVersion(), version.getTimestamp());

				// Notify read subscription
				ReadHandler.notifyByVn(ksName, inSrc, key, version.getLocalVersion());
			} else {
				HBUtils.error("KeyStatus::updateStatus, version value is null, ColumnFamily: {}", cf.toString());
			}
		}
	}
	
	public void removeCtrlEntry(final String inSrc, final String ksName, final Collection<ColumnFamily> CFS, String inCtrlSrc) {
		for (ColumnFamily cf : CFS) {
			Version version = HBUtils.getMutationVersion(cf);
			if (version != null) {
				String key = HBUtils.getPrimaryKeyName(cf.metadata());
				Status status = getStatus(key);
				status.removeCtrlVnTs(inCtrlSrc, version.getLocalVersion(), version.getTimestamp());
				ReadHandler.notifyByTs(ksName, inSrc, key, System.currentTimeMillis());
			} else {
				HBUtils.error("KeyStatus::updateStatus, version value is null, ColumnFamily: {}", cf.toString());
			}
		}
	}
	
	
    
    public KeyResult hasLatestValue(String key, long inReadTs)
    {
		boolean hasLatestValue = true, causedByTs = false, causedByVn = false;
		long version = -1;
		Status status = m_keyStatusMap.get(key);
		if(status==null)
			return new KeyResult(hasLatestValue, causedByTs, causedByVn, version);
		
		if (m_updateTs <= inReadTs) {
			hasLatestValue = false;
			causedByTs = true;
			// HBUtils.error("Update ts: " + HBUtils.dateFormat(m_updateTs) + // ", Read Ts: " + HBUtils.dateFormat(inReadTs));
			return new KeyResult(hasLatestValue, causedByTs, causedByVn, version);
		} else  {
			return status.hasLatestValue(key, inReadTs);
		}
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
    
    public void setUpdateTs(String inKsName, String inSrcName, long inUpdateTs)
    {
		if (inUpdateTs > m_updateTs) {
			m_updateTs = inUpdateTs;
			ReadHandler.notifyBySrc(inKsName, inSrcName, inUpdateTs);
		}
    }
    
    public void setUpdateTsV1(long inUpdateTs)
    {
        if (inUpdateTs > m_updateTs)
            m_updateTs = inUpdateTs;
    }
    
	public HashMap<String, Status> getKeyStatusMapCopy() {
		return Maps.newHashMap(m_keyStatusMap);
	}
	
	public ConcurrentHashMap<String, Status> getKeyStatusMap(){
		return m_keyStatusMap;
	}
	
	public long getUpdateTs() {
		return m_updateTs;
	}
	
    
    
}
