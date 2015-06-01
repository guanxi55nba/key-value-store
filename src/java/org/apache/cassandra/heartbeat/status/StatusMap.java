package org.apache.cassandra.heartbeat.status;

import java.net.InetAddress;
import java.nio.ByteBuffer;
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
import org.apache.cassandra.heartbeat.readhandler.ReadHandler;
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

	TreeBasedTable<String, String, Status> m_currentEntries = null; // key,src,statusmap
	TreeBasedTable<String, String, Status> m_removedEntries = null; // key,src,statusmap
	public static final StatusMap instance = new StatusMap();

	private StatusMap() {
		m_currentEntries = TreeBasedTable.create();
		m_removedEntries = TreeBasedTable.create();
	}

	/**
	 * Used to update status msg based on one StatusSynMsg
	 * 
	 * @param inSrcName
	 * @param inSynMsg
	 */
	public void updateStatusMap(String inSrcName, final StatusSynMsg inSynMsg) {
		if (inSynMsg != null) {
			TreeMap<String, TreeMap<Long, Long>> statusData = inSynMsg.getData();
			for (Map.Entry<String, TreeMap<Long, Long>> entry : statusData.entrySet()) {
				String key = entry.getKey();

				// Filter the status in the removed entries
				Status removedStatus = m_removedEntries.get(key, inSrcName);
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
				Status status = m_currentEntries.get(key, inSrcName);
				synchronized (m_currentEntries) {
					if (status == null) {
						status = new Status(inSynMsg.getTimestamp(), vn_ts);
						m_currentEntries.put(key, inSrcName, status);
					} else {
						status.updateVnTsData(vn_ts);
						status.setUpdateTs(inSynMsg.getTimestamp());
					}
				}
				// Notify sinked read handler
				//String ksName = ConfReader.instance.getKeySpaceName();
				ReadHandler.instance.notifySubscription(inSynMsg.getKsName(), key, inSynMsg.getTimestamp());
			}
		} else {
			logger.error("inSynMsg is null");
		}
	}

	public void removeEntry(String inSrcName, final Mutation inMutation) {
		if (inSrcName != null && inMutation != null) {
			String ksName = inMutation.getKeyspaceName();
			long currentTs = System.currentTimeMillis();
			if (!HBUtils.SYSTEM_KEYSPACES.contains(ksName)) {
				for (ColumnFamily col : inMutation.getColumnFamilies()) {
					String key = HBUtils.getPrimaryKeyName(col.metadata());
					Status currentStatus = m_currentEntries.get(key, inSrcName);
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
					Status removedStatus = m_removedEntries.get(key, inSrcName);
					synchronized (m_removedEntries) {
						if (removedStatus == null && currentStatus != null) {
							removedStatus = new Status(currentTs, removedEntry);
							m_removedEntries.put(key, inSrcName, removedStatus);
						} else if (removedStatus != null) {
							removedStatus.updateVnTsData(removedEntry);
						}
					}
				}
			}
		} else {
			logger.debug("removeEntry method: inSrcName or inMutation is null");
		}
	}

	/**
	 * @param inPageable
	 * @param inTimestamp
	 * @return
	 */
	public boolean hasLatestValue(Pageable inPageable, long inTimestamp) {
		boolean hasLatestValue = true;
		if (inPageable instanceof Pageable.ReadCommands) {
			List<ReadCommand> readCommands = ((Pageable.ReadCommands) inPageable).commands;
			for (ReadCommand cmd : readCommands) {
				String key = HBUtils.byteBufferToString(cmd.ksName, cmd.cfName, cmd.key);
				if(!hasLatestValueImpl(cmd.ksName, key,cmd.key, inTimestamp)) {
					hasLatestValue = false;
					break;
				}
			}
		}else if(inPageable instanceof RangeSliceCommand) {
			logger.error("StatusMap::hasLatestValue, RangeSliceCommand doesn't support");
		}else if(inPageable instanceof ReadCommand) {
			ReadCommand cmd = (ReadCommand)inPageable;
			String key = HBUtils.byteBufferToString(cmd.ksName, cmd.cfName, cmd.key);
			if(!hasLatestValueImpl(cmd.ksName, key, cmd.key, inTimestamp)) {
				hasLatestValue = false;
			}
		}else {
			hasLatestValue = false;
			logger.error("StatusMap::hasLatestValue, Unkonw pageable type");
		}
		return hasLatestValue;
	}
	
	private boolean hasLatestValueImpl(String inKSName,String inKeyStr, ByteBuffer inKey, long inTimestamp) {
		boolean hasLatestValue = true;
		List<InetAddress> replicaList = HBUtils.getReplicaList(inKSName, inKey);
		replicaList.remove(HBUtils.getLocalAddress());
		for (InetAddress sourceName : replicaList) {
			Status status = m_currentEntries.get(inKeyStr, sourceName.getHostAddress());
			if (status == null) {
				hasLatestValue = false;
				logger.info("StatusMap::hasLatestValueImpl, hasLatestValue == false, status == null");
			} else {
				if (status.getUpdateTs() <= inTimestamp) {
					hasLatestValue = false;
					logger.info("StatusMap::hasLatestValueImpl, {}, update ts {} <= inTimestamp {}", hasLatestValue, 
							HBUtils.dateFormat(status.getUpdateTs()), HBUtils.dateFormat(inTimestamp) );
				} else {
					// vn: ts
					TreeMap<Long, Long> versions = status.getVersionTsMap();
					// if doesn't exist entry whose timestamp < inTimestamp,then row is the latest in this datacenter
					long latestVersion = -2;
					for (Map.Entry<Long, Long> entry : versions.entrySet()) {
						long vn = entry.getKey();
						if(vn>=0){
							long ts = entry.getValue();
							if (ts <= inTimestamp) {
								hasLatestValue = false;
								if (vn > latestVersion)
									latestVersion = vn;
							}
						}
					}
					if (latestVersion != -2) {
						// Wait for mutation
						hasLatestValue = false;
						logger.info("StatusMap::hasLatestValueImpl, hasLatestValue == false, latestVersion == ", latestVersion);
					}
				}
			}
		}
		return hasLatestValue;
	}
	
	
	@Deprecated
	private boolean hasLatestValueImpl(String inKSName, String inKey, long inTimestamp) {
		boolean hasLatestValue = true;
		Set<String> dataCenterNames = HBUtils.getDataCenterNames(inKSName);
		dataCenterNames.remove(DatabaseDescriptor.getLocalDataCenter());
		for (String dcName : dataCenterNames) {
			Status status = m_currentEntries.get(inKey, dcName);
			if (status == null) {
				hasLatestValue = false;
				logger.info("StatusMap::hasLatestValueImpl, hasLatestValue == false, status == null");
			} else {
				if (status.getUpdateTs() <= inTimestamp) {
					hasLatestValue = false;
					logger.info("StatusMap::hasLatestValueImpl, {}, update ts {} <= inTimestamp {}", hasLatestValue, 
							HBUtils.dateFormat(status.getUpdateTs()), HBUtils.dateFormat(inTimestamp) );
				} else {
					// vn: ts
					TreeMap<Long, Long> versions = status.getVersionTsMap();
					// if doesn't exist entry whose timestamp < inTimestamp,
					// then row is the latest in this datacenter
					long latestVersion = -2;
					for (Map.Entry<Long, Long> entry : versions.entrySet()) {
						long vn = entry.getKey();
						if(vn>=0) {
							long ts = entry.getValue();
							if (ts <= inTimestamp) {
								hasLatestValue = false;
								if (vn > latestVersion)
									latestVersion = vn;
							}
						}
					}
					if (latestVersion != -2) {
						// Wait for mutation
						hasLatestValue = false;
						logger.info("StatusMap::hasLatestValueImpl, hasLatestValue == false, latestVersion == ", latestVersion);
					}
				}
			}
		}
		return hasLatestValue;
	}
}
