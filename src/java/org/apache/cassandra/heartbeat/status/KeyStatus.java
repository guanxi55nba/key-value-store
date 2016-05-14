package org.apache.cassandra.heartbeat.status;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.antlr.grammar.v3.ANTLRParser.finallyClause_return;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.heartbeat.StatusSynMsg;
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
    
    public KeyResult hasLatestValue(String key, long inReadTs)
    {
        boolean hasLatestValue = true;
        boolean causedByTs = false;
        boolean causedByVn = false;
        long version = -1;
        Status status = m_keyStatusMap.get(key);
        if (status == null || m_updateTs <= inReadTs)
        {
            hasLatestValue = false;
            causedByTs = true;
        }
        else
        {
            ConcurrentSkipListMap<Long, Long> versions = status.getVnToTsMap(); // vn: ts
            // if doesn't exist version whose timestamp <= read ts, then this node contains the latest data
            long previousVn = -1;
            for (Map.Entry<Long, Long> entry : versions.entrySet())
            {
                Long localVn = entry.getKey(), timestamp = entry.getValue();
                if (localVn >= 0)
                {
                    if (timestamp <= inReadTs)
                    {
                        hasLatestValue = false;
                    }
                    else
                    {
                        if (!hasLatestValue && (localVn - previousVn) == 1)
                        {
                            version = previousVn;
                            causedByVn = true;
                        }
                        break;
                    }
                    previousVn = entry.getKey();
                }
            }
        }
        return new KeyResult(hasLatestValue, causedByTs, causedByVn, version);
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
