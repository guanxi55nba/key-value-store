package org.apache.cassandra.heartbeat.readhandler;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.heartbeat.HBUtils;
import org.apache.cassandra.heartbeat.extra.HBConsts;
import org.apache.cassandra.heartbeat.status.StatusMap;
import org.apache.cassandra.service.pager.Pageable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.TreeMultimap;

public class ReadHandler {
	private static final Logger logger = LoggerFactory.getLogger(ReadHandler.class);
	ByteBuffer key;
	TreeMultimap<Long, Subscription> m_subscriptions = null;

	public static final ReadHandler instance = new ReadHandler();

	private ReadHandler() {
		m_subscriptions = TreeMultimap.create();
	}

	public void addSubscription(long inTimestamp, Subscription inSubscription) {
		m_subscriptions.put(inTimestamp, inSubscription);
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
			for (ColumnFamily columnFamily : inMutation.getColumnFamilies()) {
				Cell cell = columnFamily.getColumn(Util.cellname(HBConsts.VERSON_NO));
				if (cell instanceof BufferCell) {
					BufferCell bufferCell = (BufferCell) cell;
					final long timestamp = bufferCell.timestamp();
					for (Long ts : m_subscriptions.keySet()) {
						if (ts <= timestamp) {
							Set<Subscription> removed = new HashSet<>();
							for (Subscription sub : m_subscriptions.get(ts)) {
								// Check whether subscription has latest value
								if (StatusMap.instance.hasLatestValue(sub.getPageable(), sub.getTimestamp())) {
									sub.getLockObject().notify();
									removed.add(sub);
								}
							}
							for (Subscription sub1 : removed) {
								m_subscriptions.remove(ts, sub1);
							}
						}
					}
				}
			}
		}
	}

	public void sinkReadHandler(Pageable page, long inTimestamp, byte[] inBytes) {
		Subscription subscription = new Subscription(page, inTimestamp, inBytes);
		m_subscriptions.put(inTimestamp, subscription);
		logger.info("sinkReadHandler: [ Pageable: {}, Timestamp: {} ", page, inTimestamp);
	}
}
