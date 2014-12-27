package org.apache.cassandra.heartbeat.status;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.heartbeat.HBUtils;
import org.apache.cassandra.heartbeat.StatusSynMsg;
import org.apache.cassandra.heartbeat.extra.Version;
import org.apache.cassandra.service.pager.Pageable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.TreeBasedTable;

/**
 * Stand alone component to keep status msg related info map
 * 
 * @author XiGuan
 * 
 */
public class StatusMap {

	private static final Logger logger = LoggerFactory.getLogger(StatusMap.class);

	TreeBasedTable<String, String, Status> m_currentEntries = null; // key,datacenter,statusmap
	TreeBasedTable<String, String, Status> m_removedEntries = null; // key,datacenter,statusmap
	public static final StatusMap instance = new StatusMap();

	private StatusMap() {
		m_currentEntries = TreeBasedTable.create();
		m_removedEntries = TreeBasedTable.create();
	}

	/**
	 * Used to update status msg based on one StatusSynMsg
	 * 
	 * @param inDCName
	 * @param inSynMsg
	 */
	public void updateStatusMap(String inDCName, StatusSynMsg inSynMsg) {
		if (inSynMsg != null) {
			TreeMap<String, TreeMap<Long, Long>> statusData = inSynMsg.getData();
			for (Map.Entry<String, TreeMap<Long, Long>> entry : statusData.entrySet()) {
				String key = entry.getKey();

				// Filter the status in the removed entries
				Status removedStatus = m_removedEntries.get(key, inDCName);
				TreeMap<Long, Long> removedVnToTs = new TreeMap<Long, Long>();
				if (removedStatus != null) {
					removedVnToTs = removedStatus.getVersionTsMap();
				}
				TreeMap<Long, Long> valueMap = entry.getValue();
				TreeMap<Long, Long> vn_ts = new TreeMap<Long, Long>();
				if (removedVnToTs == null || removedVnToTs.size() == 0) {
					vn_ts = valueMap;
				} else {
					for (Map.Entry<Long, Long> item : valueMap.entrySet()) {
						if (!removedVnToTs.containsKey(item.getKey())) {
							vn_ts.put(item.getKey(), item.getValue());
						}
					}
				}

				// Update current status
				Status status = m_currentEntries.get(key, inDCName);
				if (status == null) {
					status = new Status(inSynMsg.getTimestamp(), vn_ts);
					m_currentEntries.put(key, inDCName, status);
				} else {
					status.updateVnTsData(vn_ts);
				}
			}
		} else {
			logger.error("inSynMsg is null");
		}
	}

	public void removeEntry(String inDcName, final Mutation inMutation) {
		if (inDcName != null && inMutation != null) {
			String ksName = inMutation.getKeyspaceName();
			long currentTs = System.currentTimeMillis();
			if (!HBUtils.SYSTEM_KEYSPACES.contains(ksName)) {
				for (ColumnFamily col : inMutation.getColumnFamilies()) {
					String key = HBUtils.getPrimaryKeyName(col.metadata());
					Status currentStatus = m_currentEntries.get(key, inDcName);
					TreeMap<Long, Long> removedEntry = new TreeMap<Long, Long>();
					Version version = HBUtils.getMutationVersion(col);
					if (version != null) {
						if (currentStatus != null)
							currentStatus.removeEntry(version.getLocalVersion());
						removedEntry.put(version.getLocalVersion(), currentTs);
					} else {
						logger.error("StatusMap::removeEntry, version value is null, mutation: {}", inMutation);
					}

					// Update removed status
					Status removedStatus = m_removedEntries.get(key, inDcName);
					if (removedStatus == null && currentStatus != null) {
						removedStatus = new Status(currentTs, removedEntry);
						m_removedEntries.put(key, inDcName, removedStatus);
					} else if (removedStatus != null) {
						removedStatus.updateVnTsData(removedEntry);
					}
				}
			}
		} else {
			logger.debug("removeEntry method: inDCName or inMutation is null");
		}
	}

	public boolean hasLatestValue(Pageable inPageable, long inTimestamp) {
		boolean hasLatestValue = true;
		if (inPageable instanceof Pageable.ReadCommands) {
			List<ReadCommand> readCommands = ((Pageable.ReadCommands) inPageable).commands;
			for (ReadCommand cmd : readCommands) {
				String key = HBUtils.byteBufferToString(cmd.ksName, cmd.cfName, cmd.key);
				if(!hasLatestValue(cmd.ksName, key, inTimestamp)) {
					hasLatestValue = false;
					break;
				}
			}
		}else if(inPageable instanceof RangeSliceCommand) {
			RangeSliceCommand cmd = (RangeSliceCommand)inPageable;
			if(!hasLatestValue(cmd.keyspace,cmd.columnFamily,inTimestamp)) {
				hasLatestValue = false;
			}
		}else if(inPageable instanceof ReadCommand) {
			ReadCommand cmd = (ReadCommand)inPageable;
			if(!hasLatestValue(cmd.ksName,cmd.cfName,inTimestamp));
		}else {
			hasLatestValue = false;
			logger.error("StatusMap::hasLatestValue, Unkonw pageable type");
		}
		return hasLatestValue;
	}
	
	private boolean hasLatestValue(String inKSName, String inKey, long inTimestamp) {
		boolean hasLatestValue = true;
		Set<String> dataCenterNames = HBUtils.getDataCenterNames(inKSName);
		dataCenterNames.remove(DatabaseDescriptor.getLocalDataCenter());
		for (String dcName : dataCenterNames) {
			Status status = m_currentEntries.get(inKey, dcName);
			if (status == null) {
				hasLatestValue = false;
			} else {
				if (status.getUpdateTs() <= inTimestamp) {
					hasLatestValue = false;
				} else {
					// vn: ts
					TreeMap<Long, Long> versions = status.getVersionTsMap();
					// if doesn't exist entry whose timestamp < inTimestamp,
					// then row is the latest in this datacenter
					long latestVersion = -2;
					for (Map.Entry<Long, Long> entry : versions.entrySet()) {
						long vn = entry.getKey();
						long ts = entry.getValue();
						if (ts <= inTimestamp) {
							hasLatestValue = false;
							if (vn > latestVersion)
								latestVersion = vn;
						}
					}
					if (latestVersion != -2) {
						// Wait for mutation
						hasLatestValue = false;
					}
				}
			}
		}
		return hasLatestValue;
	}
}
