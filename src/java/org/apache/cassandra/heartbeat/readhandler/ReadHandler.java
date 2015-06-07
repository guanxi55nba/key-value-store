package org.apache.cassandra.heartbeat.readhandler;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.heartbeat.HBUtils;
import org.apache.cassandra.heartbeat.StatusSynMsg;
import org.apache.cassandra.heartbeat.extra.HBConsts;
import org.apache.cassandra.heartbeat.status.StatusMap;
import org.apache.cassandra.service.pager.Pageable;
import org.apache.cassandra.utils.keyvaluestore.ConfReader;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * {keyspace, key, { ts, subscription}}
 * 
 * @author xig
 *
 */
public class ReadHandler {
	private static final Logger logger = LoggerFactory.getLogger(ReadHandler.class);
	ByteBuffer key;
	ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, ConcurrentSkipListSet<Subscription>> >> m_subscriptionMatrics;

	public static final ReadHandler instance = new ReadHandler();

	private ReadHandler() {
		m_subscriptionMatrics = new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, ConcurrentSkipListSet<Subscription>>>>();
	}

	/**
	 * Check whether there is a subscription could be awake
	 * 
	 * @param inMutation
	 */
	public synchronized void notifySubscription(Mutation inMutation) {
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
	public void notifySubscription(StatusSynMsg inMsg) {
		String ksName = ConfReader.instance.getKeySpaceName();
		TreeMap<String, TreeMap<Long, Long>> data = inMsg.getData();
		for (String key : data.keySet()) {
			notifySubscription(ksName, key, inMsg.getTimestamp());
		}
	}
	
	public void notifySubscription(String ksName, String key, final long timestamp) {
		ConcurrentSkipListMap<Long, ConcurrentSkipListSet<Subscription>> vnToSubs = getSubscriptions(ksName, key);
		if (vnToSubs != null) {
			for (Entry<Long, ConcurrentSkipListSet<Subscription>> entry : vnToSubs.entrySet()) {
				Long ts = entry.getKey();
				if (ts <= timestamp) {
					if (ConfReader.instance.isLogEnabled())
						logger.info("ReadHandler.notifySubscription: ts<=timestamp");
					ConcurrentSkipListSet<Subscription> subs = entry.getValue();
					if (subs != null) {
						for (Subscription sub : subs) {
							// Check whether subscription has latest value
							if (StatusMap.instance.hasLatestValue(sub.getPageable(), sub.getTimestamp())) {
								synchronized (sub.getLockObject()) {
									sub.getLockObject().notify();
								}
								subs.remove(sub);
							}
						}
					}
				}
			}
		}
	}

	public void sinkReadHandler(Pageable page, long inTimestamp, byte[] inBytes) {
		if (page != null) {
			Subscription subscription = new Subscription(page, inTimestamp, inBytes);
			if (page instanceof RangeSliceCommand) {
				logger.info("ReadHandler::sinkReadHandler, page is instance of RangeSliceCommand");
			} else if (page instanceof Pageable.ReadCommands) {
				List<ReadCommand> readCommands = ((Pageable.ReadCommands) page).commands;
				for (ReadCommand cmd : readCommands) {
					String key = HBUtils.byteBufferToString(cmd.ksName, cmd.cfName, cmd.key);
					updateSubscriptions(cmd.ksName, key, inTimestamp, subscription);
				}
			} else if (page instanceof ReadCommand) {
				ReadCommand cmd = (ReadCommand) page;
				String key = HBUtils.byteBufferToString(cmd.ksName, cmd.cfName, cmd.key);
				updateSubscriptions(cmd.ksName, key, inTimestamp, subscription);
			} else {
				logger.error("StatusMap::hasLatestValue, Unkonw pageable type");
			}
			logger.info("sinkReadHandler: [ Pageable: {}, Timestamp: {} ", page, DateFormatUtils.format(inTimestamp, "yyyy-MM-dd HH:mm:ss"));
		} else {
			logger.info("ReadHandler::sinkReadHandler, page is null");
		}
	}
	
	private ConcurrentSkipListMap<Long, ConcurrentSkipListSet<Subscription>> getSubscriptions(String inKsName, String inKey) {
		ConcurrentSkipListMap<Long, ConcurrentSkipListSet<Subscription>> subscriptions = null;
		ConcurrentHashMap<String, ConcurrentSkipListMap<Long, ConcurrentSkipListSet<Subscription>>> keyToVersionSubs = m_subscriptionMatrics.get(inKsName);
		if (keyToVersionSubs != null) {
			subscriptions = keyToVersionSubs.get(inKey);
		}
		return subscriptions;
	}
	
	private void updateSubscriptions(String inKsName, String inKey, long inTs, Subscription inSub) {
		ConcurrentHashMap<String, ConcurrentSkipListMap<Long, ConcurrentSkipListSet<Subscription>>> keyToVersionSubs = m_subscriptionMatrics.get(inKsName);
		if (keyToVersionSubs == null) {
			keyToVersionSubs = new ConcurrentHashMap<String, ConcurrentSkipListMap<Long, ConcurrentSkipListSet<Subscription>>>();
			m_subscriptionMatrics.put(inKsName, keyToVersionSubs);
		}
		
		ConcurrentSkipListMap<Long, ConcurrentSkipListSet<Subscription>> vnToSubs = keyToVersionSubs.get(inKey);
		if (vnToSubs == null) {
			vnToSubs = new ConcurrentSkipListMap<Long, ConcurrentSkipListSet<Subscription>>();
			keyToVersionSubs.put(inKey, vnToSubs);
		}
		
		ConcurrentSkipListSet<Subscription> subs = vnToSubs.get(inTs);
		if (subs == null) {
			subs = new ConcurrentSkipListSet<Subscription>();
			vnToSubs.put(inTs, subs);
		}
		
		subs.add(inSub);
	}
}
