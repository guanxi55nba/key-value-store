package org.apache.cassandra.heartbeat.status;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.heartbeat.HBUtils;
import org.apache.cassandra.heartbeat.StatusSynMsg;
import org.apache.cassandra.heartbeat.extra.HBConsts;
import org.apache.cassandra.service.pager.Pageable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.TreeBasedTable;

/**
 * 
 * @author XiGuan
 * 
 */
public class StatusMap {

	private static final Logger logger = LoggerFactory.getLogger(StatusMap.class);

	TreeBasedTable<String, String, Status> m_currentEntries = null; // key, datacenter, statusmap
	TreeBasedTable<String, String, Status> m_removedEntries = null; // key, datacenter, statusmap
	public static final StatusMap instance = new StatusMap();

	private StatusMap() {
		m_currentEntries = TreeBasedTable.create();
		m_removedEntries = TreeBasedTable.create();
	}

	public void updateStatusMap(String inDCName, StatusSynMsg inSynMsg) {
		if (inSynMsg != null) {
			TreeMap<String, TreeMap<Long, Long>> statusData = inSynMsg
					.getData();
			for (Map.Entry<String, TreeMap<Long, Long>> entry : statusData
					.entrySet()) {
				String key = entry.getKey();
				Status status = m_currentEntries.get(key, inDCName);
				if (status == null) {
					status = new Status(inSynMsg.getTimestamp(),
							entry.getValue());
					m_currentEntries.put(key, inDCName, status);
				} else {
					status.updateVnTsData(entry.getValue());
				}
			}
		}else{
			logger.error("inSynMsg is null");
		}
	}

	public void removeEntry(String inDcName, Mutation inMutation) {
		if (inDcName != null && inMutation != null) {
			ByteBuffer inKey = inMutation.key();
			String key = String.valueOf(inKey.getInt(0));
			String ksName = inMutation.getKeyspaceName();
			if (!HBUtils.SYSTEM_KEYSPACES.contains(ksName) && inKey != null) {
				// Update current status
				Status currentStatus = m_currentEntries.get(key, inDcName);
				TreeMap<Long, Long> removedEntry = new TreeMap<Long, Long>();
				if (currentStatus != null) {
					for (ColumnFamily columnFamily : inMutation
							.getColumnFamilies()) {
						Cell cell = columnFamily.getColumn(HBUtils
								.cellname(HBConsts.VERSON_NO));
						if (cell instanceof BufferCell) {
							BufferCell bufferCell = (BufferCell) cell;
							Long version = bufferCell.value().getLong();
							// Long timestamp = bufferCell.timestamp();
							Long ts = currentStatus.removeEntry(version);
							if (version != null) {
								removedEntry.put(version, ts);
							}
						}
					}
				}
				// Update removed status
				Status removedStatus = m_removedEntries.get(key, inDcName);
				if (removedStatus == null && currentStatus!=null) {
					removedStatus = new Status(currentStatus.getUpdateTs(),
							removedEntry);
					m_removedEntries.put(key, inDcName, removedStatus);
				} else {
					removedStatus.updateVnTsData(removedEntry);
				}
			} else if (inKey == null) {
				logger.debug("The key in muation is null");
			}
		} else {
			logger.debug("removeEntry method: inDCName or inMutation is null");
		}
	}

	public boolean hasLatestValue(Pageable inPageable, long inTimestamp) {
		boolean hasLatestValue = true;
		if (inPageable instanceof Pageable.ReadCommands) {
			List<ReadCommand> readCommands = ((Pageable.ReadCommands) inPageable).commands;
			for (ReadCommand readCommand : readCommands) {
				String key = String.valueOf(readCommand.key.getInt(0));
				String localDcName = DatabaseDescriptor.getLocalDataCenter();
				Set<String> dataCenterNames = HBUtils.getDataCenterNames(readCommand.ksName);
				dataCenterNames.remove(localDcName);
				for (String dcName : dataCenterNames) {
					Status status = m_currentEntries.get(key, dcName);
					if (status == null) {
						hasLatestValue = false;
					} else {
						if (status.getUpdateTs() <= inTimestamp) {
							hasLatestValue = false;
						} else {
							TreeMap<Long, Long> versions = status
									.getVersionTsMap(); // vn: ts
							// if doesn't exist entry whose timestamp <
							// inTimestamp, then row is the latest in this
							// datacenter
							long latestVersion = -2;
							for (Map.Entry<Long, Long> entry : versions
									.entrySet()) {
								long version = entry.getKey();
								long timestamp = entry.getValue();
								if (timestamp <= inTimestamp) {
									hasLatestValue = false;
									if (version > latestVersion)
										latestVersion = version;
								}
							}
							if (latestVersion != -2) {
								// Wait for mutation
								hasLatestValue = false;
							}
						}
					}
				}
			}
		}
		return hasLatestValue;
	}
}
