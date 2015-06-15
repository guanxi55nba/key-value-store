package org.apache.cassandra.heartbeat.readhandler;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.heartbeat.StatusSynMsg;
import org.apache.cassandra.heartbeat.extra.HBConsts;
import org.apache.cassandra.heartbeat.status.StatusMap;
import org.apache.cassandra.heartbeat.utils.ConfReader;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.apache.cassandra.service.pager.Pageable;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.TreeBasedTable;
import com.google.common.collect.TreeMultimap;

/**
 * 
 * {keyspace, table, { ts, subscription}}
 * 
 * @author xig
 *
 */
public class ReadHandler {
	private static final Logger logger = LoggerFactory.getLogger(ReadHandler.class);
	ByteBuffer key;
	TreeBasedTable<String, String, TreeMultimap<Long, Subscription>> m_subscription;

	public static final ReadHandler instance = new ReadHandler();

	private ReadHandler() {
		m_subscription = TreeBasedTable.create();
	}

	/**
	 * Check whether there is a subscription could be awake
	 * 
	 * @param inMutation
	 */
	public void notifySubscription(Mutation inMutation) {
		// Get mutation Ts
		String ksName = inMutation.getKeyspaceName();
		if (!HBUtils.SYSTEM_KEYSPACES.contains(ksName)) {
			for (ColumnFamily col : inMutation.getColumnFamilies()) {
				String key = HBUtils.getPrimaryKeyName(col.metadata());
				Cell cell = col.getColumn(HBUtils.cellname(HBConsts.VERSON_NO));
				final long timestamp = cell.timestamp();
				notifySubscription(ksName, key, timestamp);
			}
		}
	}
	
	/**
	 * Check whether there is a subscription could be awake
	 * 
	 * @param inMsg
	 */
	public void notifySubscription(final StatusSynMsg inMsg) {
		String ksName = ConfReader.instance.getKeySpaceName();
		TreeMap<String, TreeMap<Long, Long>> data = inMsg.getData();
		for (String key : data.keySet()) {
			notifySubscription(ksName, key, inMsg.getTimestamp());
		}
	}
	
	public void notifySubscription(String ksName, String key, final long timestamp) {
		TreeMultimap<Long, Subscription> subMap = m_subscription.get(ksName, key);
		if (subMap != null) {
			for (Long ts : subMap.keySet()) {
				if (ts <= timestamp) {
					logger.info("ReadHandler.notifySubscription: ts<=timestamp");
					Set<Subscription> removed = new HashSet<Subscription>();
					for (Subscription sub : subMap.get(ts)) {
						// Check whether subscription has latest value
						if (StatusMap.instance.hasLatestValue(sub.getPageable(), sub.getTimestamp())) {
							synchronized (sub.getLockObject()) {
								sub.getLockObject().notify();
							}
							removed.add(sub);
						}
					}
					for (Subscription sub1 : removed) {
						subMap.remove(ts, sub1);
					}
				}
			}
		}
	}

	public void sinkReadHandler(Pageable page, long inTimestamp, byte[] inBytes) {
		if (page != null) {
			Subscription subscription = new Subscription(page, inTimestamp, inBytes);
			if (page instanceof Pageable.ReadCommands) {
				List<ReadCommand> readCommands = ((Pageable.ReadCommands) page).commands;
				for (ReadCommand cmd : readCommands) {
					String key = HBUtils.byteBufferToString(cmd.ksName, cmd.cfName, cmd.key);
					TreeMultimap<Long, Subscription> subMap = m_subscription.get(cmd.ksName, key);
					if (subMap == null) {
						subMap = TreeMultimap.create();
						m_subscription.put(cmd.ksName, key, subMap);
					}
					subMap.put(inTimestamp, subscription);
				}
			} else if (page instanceof RangeSliceCommand) {
				logger.info("ReadHandler::sinkReadHandler, page is instance of RangeSliceCommand");
			} else if (page instanceof ReadCommand) {
				ReadCommand cmd = (ReadCommand) page;
				String key = HBUtils.byteBufferToString(cmd.ksName, cmd.cfName, cmd.key);
				TreeMultimap<Long, Subscription> subMap = m_subscription.get(cmd.ksName, key);
				if (subMap == null) {
					subMap = TreeMultimap.create();
					m_subscription.put(cmd.ksName, key, subMap);
				}
				subMap.put(inTimestamp, subscription);
			} else {
				logger.error("StatusMap::hasLatestValue, Unkonw pageable type");
			}
			logger.info("sinkReadHandler: [ Pageable: {}, Timestamp: {} ", page, HBUtils.dateFormat(inTimestamp));
		} else {
			logger.info("ReadHandler::sinkReadHandler, page is null");
		}
	}
}
