package org.apache.cassandra.heartbeat.status;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.heartbeat.utils.ConfReader;

import com.google.common.collect.Maps;

/**
 * Status map structure: { vn-to-ts: { vn1: ts1, vn2: ts2 }, updateTs: ts }
 * 
 * @author XiGuan
 * 
 */
public class Status
{
    private ConcurrentSkipListMap<Long, Long> m_currentVnToTs = new ConcurrentSkipListMap<Long, Long>();
    private ConcurrentSkipListMap<Long, Long> m_removedVnToTs = new ConcurrentSkipListMap<Long, Long>();
    
	private ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> m_ctrlVnToTs = new ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>();
	private ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> m_ctrlRemovedVnToTs = new ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>();
    byte[] m_lockObj = new byte[0];
    private volatile boolean flag = true;
    private volatile boolean ctrlFlag = true;

    public Status()
    {
    }

    public void addVnTsData(long inVersionNo, long inTimestamp)
    {
        if (flag && !m_removedVnToTs.containsKey(inVersionNo))
        {
            m_currentVnToTs.put(inVersionNo, inTimestamp);
        }
    }
    
    public void addVnTsData(Map<Long, Long> inMap)
    {
        for (Map.Entry<Long, Long> entry : inMap.entrySet())
        {
            if (flag && m_removedVnToTs.get(entry.getKey()) != null )
            {
                m_currentVnToTs.put(entry.getKey(), entry.getValue());
            }
        }
    }
    
    public long removeEntry(Long inVersion, Long inTs)
    {
        boolean removed;
        m_removedVnToTs.put(inVersion, inTs);
        flag = true;
        removed = m_currentVnToTs.remove(inVersion, inTs);
        long ts = removed ? inTs : System.currentTimeMillis();
        return ts;
    }
    
	public void addCtrlVnTs(String inSrc, TreeMap<Long, Long> inVnMap) {
		if(inVnMap.isEmpty())
			return;

		ConcurrentSkipListMap<Long, Long> removedCtrlVnMap = getVnMap(m_ctrlRemovedVnToTs, inSrc);
		ConcurrentSkipListMap<Long, Long> ctrlVnMap = getVnMap(m_ctrlVnToTs, inSrc);

		for (Map.Entry<Long, Long> entry : ctrlVnMap.entrySet()) {
			if (ctrlFlag && removedCtrlVnMap.get(entry.getKey()) != null) {
				ctrlVnMap.put(entry.getKey(), entry.getValue());
			}
		}
	}
	
	public long removeCtrlVnTs(String inCtrlSrc, Long inVersion, Long inTs) {
		boolean removed;
		ConcurrentSkipListMap<Long, Long> removedCtrlVnMap = getVnMap(m_ctrlRemovedVnToTs, inCtrlSrc);
		removedCtrlVnMap.put(inVersion, inTs);
		ctrlFlag = true;
		ConcurrentSkipListMap<Long, Long> ctrlVnMap = getVnMap(m_ctrlVnToTs,inCtrlSrc);
		removed = ctrlVnMap.remove(inVersion, inTs);
		long ts = removed ? inTs : System.currentTimeMillis();
		return ts;
	}
	
	private ConcurrentSkipListMap<Long, Long> getVnMap(ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> inSrcVnMap, String inSrc){
		ConcurrentSkipListMap<Long, Long> vnTsMap = inSrcVnMap.get(inSrc);
		if (vnTsMap == null) {
			ConcurrentSkipListMap<Long, Long> newVnTsMap = new ConcurrentSkipListMap<Long, Long>();
			vnTsMap = inSrcVnMap.putIfAbsent(inSrc, newVnTsMap);
			if (vnTsMap == null)
				vnTsMap = newVnTsMap;
		}
		return vnTsMap;
	}

    public ConcurrentSkipListMap<Long, Long> getVnToTsMap()
    {
        return m_currentVnToTs;
    }
    
    public KeyResult hasLatestValue(String key, long inReadTs) {
    	boolean hasLatestValue = true, causedByTs = false, causedByVn = false;
        long version = -1;
        TreeMap<Long, Long> versions = Maps.newTreeMap(m_currentVnToTs); // vn: ts
		// if doesn't exist version whose timestamp <= read ts, then this node contains the latest data
		long previousVn = -1;
		for (Map.Entry<Long, Long> entry : versions.entrySet()) {
			Long localVn = entry.getKey(), timestamp = entry.getValue();
			if (localVn >= 0) {
				if (timestamp <= inReadTs && (inReadTs - timestamp) < ConfReader.getTimeout()) {
					hasLatestValue = false;
				} else {
					if (!hasLatestValue && (localVn - previousVn) == 1) {
						version = previousVn;
						causedByVn = true;
					}
					break;
				}
				previousVn = entry.getKey();
			}
		}
		
		for (Map.Entry<String, ConcurrentSkipListMap<Long, Long>> srcVnMapEntry : m_ctrlVnToTs.entrySet()) {
			ConcurrentSkipListMap<Long, Long> vnMap = srcVnMapEntry.getValue();
			outerloop: for (Map.Entry<Long, Long> vnMapEntry : vnMap.entrySet()) {
				Long localVn = vnMapEntry.getKey(), timestamp = vnMapEntry.getValue();
				if (localVn >= 0) {
					if (timestamp <= inReadTs && (inReadTs - timestamp) < ConfReader.getTimeout()) {
						hasLatestValue = false;
						break outerloop;
					}
				}
			}
		}
		
		return new KeyResult(hasLatestValue, causedByTs, causedByVn, version);
    }
}
