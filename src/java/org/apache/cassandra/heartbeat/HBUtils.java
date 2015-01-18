package org.apache.cassandra.heartbeat;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.heartbeat.extra.HBConsts;
import org.apache.cassandra.heartbeat.extra.Version;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.IReadCommand;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.Pageable;
import org.apache.cassandra.service.pager.Pageable.ReadCommands;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.keyvaluestore.ConfReader;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBUtils {
	private static final Logger logger = LoggerFactory.getLogger(HBUtils.class);

	public static final List<String> SYSTEM_KEYSPACES = new ArrayList<String>(Arrays.asList("system", "system_traces"));;

	public static Row getKeyValue(String inKSName, String inCFName, ByteBuffer key) {
		Row row = null;
		CFMetaData cfMetaData = Schema.instance.getKSMetaData(inKSName).cfMetaData().get(inCFName);
		String primaryKeyName = getPrimaryKeyName(cfMetaData);
		if (primaryKeyName == null) {
			logger.debug("getKeyValue method, primary key is null");
		} else {
			try {
				StringBuilder sb = new StringBuilder();
				sb.append("select ");
				sb.append(HBConsts.VERSON_NO);
				sb.append(", ");
				sb.append(HBConsts.VERSION_WRITE_TIME);
				sb.append(" from ");
				sb.append(inKSName);
				sb.append(".");
				sb.append(inCFName);
				sb.append(" where ");
				sb.append(primaryKeyName);
				sb.append(" = ");
				sb.append(byteBufferToString(cfMetaData, key));
				sb.append(";");
				UntypedResultSet result = QueryProcessor.process(sb.toString(), ConsistencyLevel.LOCAL_ONE);
				if (result.size() > 0)
					row = result.one();
			} catch (RequestExecutionException e) {
				logger.debug("getKeyValue", e);
				e.printStackTrace();
			} catch (Exception e) {
				logger.debug("getKeyValue", e);
			}
		}
		return row;
	}

	public static String getColumnFamilyName() {
		return ConfReader.instance.getColumnFamilyName();
	}

	/**
	 * Get the replica node ip address based on the partition key
	 * 
	 * @param key
	 * @return Replica nodes' ip address list
	 */
	public static List<InetAddress> getReplicaList(String inKeySpaceName, ByteBuffer key) {
		@SuppressWarnings("rawtypes")
		Token tk = StorageService.getPartitioner().getToken(key);
		return StorageService.instance.getNaturalEndpoints(inKeySpaceName, tk);
	}

	/**
	 * Get all the local saved data's partition keys
	 * 
	 * @return list of parition key
	 */
	public static Set<KeyMetaData> getLocalSavedPartitionKeys() {
		Set<KeyMetaData> localKeys = new HashSet<KeyMetaData>();
		Set<String> ksNames = getAllLocalKeySpaceName();
		for (String ksName : ksNames) {
			Set<ColumnFamilyStore> columnFamilyStores = getColumnFamilyStores(ksName);
			for (ColumnFamilyStore store : columnFamilyStores) {
				String cfName = store.getColumnFamilyName();
				CFMetaData cfMetaData = store.metadata;
				String primaryKeyName = getPrimaryKeyName(cfMetaData);
				if (primaryKeyName != null) {
					Set<KeyMetaData> keysInOneKeyspace = getLocalPrimaryKeys(ksName, cfName, primaryKeyName);
					localKeys.addAll(keysInOneKeyspace);
				} else {
					logger.debug("getLocalSavedPartitionKeys method, primary key name is null");
				}
			}
		}
		return localKeys;
	}

	public static Set<String> getDataCenterNames(String inKeySpaces) {
		Set<String> datacenterNames = new HashSet<String>();
		Keyspace keyspace = Keyspace.open(inKeySpaces);
		AbstractReplicationStrategy strategy = keyspace.getReplicationStrategy();
		if (strategy instanceof NetworkTopologyStrategy) {
			datacenterNames.addAll(((NetworkTopologyStrategy) strategy).getDatacenters());
		} else if (strategy instanceof SimpleStrategy) {
			datacenterNames.add(DatabaseDescriptor.getLocalDataCenter());
		}
		return datacenterNames;
	}

	public static Version getMutationVersion(final ColumnFamily columnFamily) {
		Version version = null;
		Cell cell = columnFamily.getColumn(HBUtils.cellname(HBConsts.VERSON_NO));
		if (cell != null) {
			try {
				long timestamp = cell.timestamp();
				long versionNo = cell.value().asReadOnlyBuffer().getLong();
				version = new Version(versionNo, timestamp);
			} catch (Exception e) {
				logger.error("getMutationVersion exception {} ", e);
			}
		}
		return version;
	}

	public static String getMutationSource(final ColumnFamily columnFamily) {
		String source = "";
		Cell cell = columnFamily.getColumn(HBUtils.cellname(HBConsts.SOURCE));
		if (cell != null) {
			try {
				source = ByteBufferUtil.string(cell.value());
			} catch (Exception e) {
				logger.error("getMutationVersion exception {} ", e);
			}
		}
		return source;
	}

	private static Set<KeyMetaData> getLocalPrimaryKeys(String inKSName, String inCFName, String inPrimaryKeyName) {
		Set<KeyMetaData> localKeys = new HashSet<KeyMetaData>();
		if (inPrimaryKeyName != null && !inPrimaryKeyName.isEmpty()) {
			try {
				StringBuilder sb = new StringBuilder();
				sb.append("select ");
				sb.append(inPrimaryKeyName);
				sb.append(", ");
				sb.append(HBConsts.VERSON_NO);
				sb.append(", ");
				sb.append(HBConsts.SOURCE);
				sb.append(", ");
				sb.append(HBConsts.VERSION_WRITE_TIME);
				sb.append(" from ");
				sb.append(inKSName);
				sb.append(".");
				sb.append(inCFName);
				sb.append(";");
				UntypedResultSet result = QueryProcessor.process(sb.toString(), ConsistencyLevel.LOCAL_ONE);
				for (Row row : result) {
					ByteBuffer key = row.getBytes(inPrimaryKeyName);
					localKeys.add(new KeyMetaData(inKSName, inCFName, key, row));
				}
			} catch (RequestExecutionException e) {
				logger.debug("getKeyValue", e);
				e.printStackTrace();
			} catch (Exception e) {
				logger.debug("getKeyValue", e);
			}
		}
		return localKeys;
	}

	public static Set<String> getAllLocalKeySpaceName() {
		Set<String> ksNames = new HashSet<String>();
		ksNames.addAll(Schema.instance.getKeyspaces());
		ksNames.removeAll(SYSTEM_KEYSPACES);
		return ksNames;
	}

	public static Set<ColumnFamilyStore> getColumnFamilyStores(String inKeySpace) {
		Set<ColumnFamilyStore> columnFamilyStores = new HashSet<ColumnFamilyStore>();
		for (ColumnFamilyStore cfs : Keyspace.open(inKeySpace).getColumnFamilyStores()) {
			for (ColumnFamilyStore store : cfs.concatWithIndexes())
				columnFamilyStores.add(store);
		}
		return columnFamilyStores;
	}

	public static String getPrimaryKeyName(String inKSName, String inCFName) {
		CFMetaData cfMetaData = Schema.instance.getKSMetaData(inKSName).cfMetaData().get(inCFName);
		return getPrimaryKeyName(cfMetaData);
	}

	public static String getPrimaryKeyName(CFMetaData cfMetaData) {
		String primaryKeyName = null;
		for (ColumnDefinition definition : cfMetaData.allColumns()) {
			if (definition.isPrimaryKeyColumn()) {
				primaryKeyName = definition.name.toString();
				break;
			}
		}
		return primaryKeyName;
	}

	public static String byteBufferToString(String inKSName, String inCFName, ByteBuffer inKey) {
		CFMetaData cfMetaData = Schema.instance.getKSMetaData(inKSName).cfMetaData().get(inCFName);
		return byteBufferToString(cfMetaData, inKey);
	}

	public static String byteBufferToString(CFMetaData cfMetaData, ByteBuffer inKey) {
		return cfMetaData.getKeyValidator().getString(inKey);
	}

	public static ByteBuffer stringToByteBuffer(String inKSName, String inCFName, String inKey) {
		CFMetaData cfMetaData = Schema.instance.getKSMetaData(inKSName).cfMetaData().get(inCFName);
		return stringToByteBuffer(cfMetaData, inKey);
	}

	public static ByteBuffer stringToByteBuffer(CFMetaData cfMetaData, String inKey) {
		return cfMetaData.getKeyValidator().fromString(inKey);
	}

	public static Set<String> getReadCommandRelatedKeySpaceNames(Pageable inPageable) {
		Set<String> ksNames = new HashSet<String>();
		if (inPageable instanceof IReadCommand) {
			ksNames.add(((IReadCommand) inPageable).getKeyspace());
		} else if (inPageable instanceof ReadCommands) {
			for (ReadCommand cmd : ((ReadCommands) inPageable).commands) {
				ksNames.add(cmd.getKeyspace());
			}
		}
		return ksNames;
	}

	public static CellName cellname(ByteBuffer... bbs) {
		if (bbs.length == 1)
			return CellNames.simpleDense(bbs[0]);
		else
			return CellNames.compositeDense(bbs);
	}

	public static CellName cellname(String... strs) {
		ByteBuffer[] bbs = new ByteBuffer[strs.length];
		for (int i = 0; i < strs.length; i++)
			bbs[i] = ByteBufferUtil.bytes(strs[i]);
		return cellname(bbs);
	}

	public static String dateFormat(long inTs) {
		return DateFormatUtils.format(inTs, "yyyy-MM-dd HH:mm:ss");
	}

	public static void addLocalDcAndVersionNoInUpdate(UpdateParameters params, Composite clusteringPrefix, ColumnFamily cf, long vn, String dcName) throws InvalidRequestException {
		String ksName = cf.metadata().ksName;
		if (!HBUtils.SYSTEM_KEYSPACES.contains(ksName)) {
			// Add version no
			ByteBuffer vnColName = ByteBufferUtil.bytes(HBConsts.VERSON_NO);
			ColumnDefinition vnColDef = cf.metadata().getColumnDefinition(vnColName);
			CellName vnCellName = cf.getComparator().create(clusteringPrefix, vnColDef);
			ByteBuffer vnCellValue = ByteBufferUtil.bytes(vn);
			cf.addColumn(params.makeColumn(vnCellName, vnCellValue));

			// Add local dc
			ByteBuffer dcColName = ByteBufferUtil.bytes(HBConsts.SOURCE);
			ColumnDefinition dcColDef = cf.metadata().getColumnDefinition(dcColName);
			CellName dcCellName = cf.getComparator().create(clusteringPrefix, dcColDef);
			ByteBuffer dcCellValue = ByteBufferUtil.bytes(dcName);
			cf.addColumn(params.makeColumn(dcCellName, dcCellValue));
		}
	}

}
