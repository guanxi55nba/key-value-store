package org.apache.cassandra.heartbeat.extra;



public class UselessCode {

	public UselessCode() {
		// TODO Auto-generated constructor stub
	}
	
	// update key valid to field
//			String keyspace = ConfReader.instance.getKeySpaceName();
//			StatusSynMsg statusSynMsg = message.payload;
//			for (Map.Entry<String, TreeMap<Long, Long>> keyPairs : statusSynMsg.getData().entrySet()) {
//				int keyInt = Integer.valueOf(keyPairs.getKey());
//				ByteBuffer key = Util.getBytes(keyInt);
//				TreeMap<Long, Long> data = keyPairs.getValue();
//				if (data.isEmpty()) {
//					long timestamp = statusSynMsg.getTimestamp();
//					// updateValidToField(keyspace, HeartBeater.CF_NAME, keyInt, timestamp);
//					doMutation(keyspace, statusSynMsg, key);
//					
//					logger.info("Mutation Time {} ", DateFormatUtils.format(new Date(timestamp), "yyyy-MM-dd HH:mm:ss"));
//				} else {
//					// Mutation mutation = new Mutation(keyspace, key);
//					// Map.Entry<Long, BigInteger> versions = data.lastEntry();
//					// Long ts = versions.getKey();
//					// BigInteger versionNo = versions.getValue();
//					// String columnFamilyName = HeartBeater.CF_NAME;
//					// CellName cellName = Util.cellname(HeartBeater.VERSON_NO);
//					// ByteBuffer value = wrap(versionNo);
//					// mutation.add(columnFamilyName, cellName, value, ts);
//				}
//			}
	
//	public boolean isLatestValue(List<org.apache.cassandra.db.Row> inListRows, long inTimestamp) {
//		boolean isLatestValue = true;
//		for (org.apache.cassandra.db.Row row : inListRows) {
//			Set<String> dataCenterNames = getDataCenterNames(row.cf.metadata().ksName);
//			for (String dcName : dataCenterNames) {
//				if (!dcName.equals(DatabaseDescriptor.getLocalDataCenter())) {
//					StatusSynMsg statusSynMsg = m_multiDCStatusMap.get(dcName);
//					if (statusSynMsg != null) {
//						long updateTs = statusSynMsg.getTimestamp();
//						if (updateTs > inTimestamp) {
//							String key = String.valueOf(row.key.getKey().getInt(0));
//							// vn: ts
//							TreeMap<Long, Long> versions = statusSynMsg.getData().get(key);
//							if (versions != null) {
//								// if doesn't exist entry timestamp < inTimestamp,
//								// then row is the latest in this datacenter
//								long latestVersion = -1;
//								for (Map.Entry<Long, Long> entry : versions.entrySet()) {
//									long version = entry.getKey();
//									long timestamp = entry.getValue();
//									if (timestamp <= inTimestamp) {
//										isLatestValue = false;
//										if (version > latestVersion)
//											latestVersion = version;
//									}
//								}
//								if (latestVersion != -1) {
//									// Wait for mutation
//								}
//							}
//						} else {
//							// Wait status message
//						}
//					} else {
//						// Wait status message
//					}
//				}
//			}
//		}
//		
//		if(!isLatestValue) {
//			// Sink read handler
//			
//		}
//		return isLatestValue;
//	}
	
	/**
	 * When new mutation comes, remove related entry from multi datacenter status map
	 * 
	 * @param inDcName
	 * @param inMutation
	 */
//	public void removeEntryFromMTDCStatusMap(String inDcName, Mutation inMutation) {
//		if (inDcName != null && inMutation != null) {
//			ByteBuffer inKey = inMutation.key();
//			String ksName = inMutation.getKeyspaceName();
//			if (ksName.equals(getKeySpaceName()) && inKey != null) {
//				StatusSynMsg statusSynMsg = m_multiDCStatusMap.get(inDcName);
//				if (statusSynMsg != null) {
//					for (ColumnFamily columnFamily : inMutation.getColumnFamilies()) {
//						Cell cell = columnFamily.getColumn(Util.cellname(VERSON_NO));
//						// If buffer cell, get timestamp
//						
//						if (cell != null) {
//							long inVersionNo = cell.value().getLong();
//							String key = String.valueOf(inKey.getInt(0));
//							statusSynMsg.removeVnTsEntry(key, inVersionNo);
//							TreeMap<Long, Long> versions = statusSynMsg.getData().get(key);
//							if (versions != null) {
//								versions.remove(inVersionNo);
//								logger.info("removeEntryFromMTDCStatusMap: keyspace = {}, vn = {} ", ksName,
//										inVersionNo);
//							} else {
//								logger.error("VnToTs entry for key = {}, vn= {} has been removed", key, inVersionNo);
//							}
//						}
//					}
//				}
//			}
//		}
//	}
	
	//private static final long DEFAULT_LATEST_VN = -2;
	
    /*if (latestVersion != DEFAULT_LATEST_VN) // Wait for mutation
    {
        hasLatestValue = false;
        logger.info("StatusMap::hasLatestValueImpl, hasLatestValue == false, latestVersion == ", latestVersion);
    }*/
	
    /*if (vn > latestVersion)
    latestVersion = vn;*/

}
