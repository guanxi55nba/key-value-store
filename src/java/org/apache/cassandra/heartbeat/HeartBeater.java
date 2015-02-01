package org.apache.cassandra.heartbeat;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.heartbeat.extra.HBConsts;
import org.apache.cassandra.heartbeat.extra.Version;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.keyvaluestore.ConfReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.TreeBasedTable;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Periodically send out heart beat msgs
 * 
 * @author XiGuan
 * 
 */
public class HeartBeater implements IFailureDetectionEventListener, HeartBeaterMBean {
	private static final Logger logger = LoggerFactory.getLogger(HeartBeater.class);
	private static final String MBEAN_NAME = "org.apache.cassandra.net:type=HeartBeater";
	private static final DebuggableScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("HeartBeatTasks");
	public final static int intervalInMillis = ConfReader.instance.getHeartbeatInterval();
	private final Comparator<InetAddress> inetcomparator = new Comparator<InetAddress>() {
		public int compare(InetAddress addr1, InetAddress addr2) {
			return addr1.getHostAddress().compareTo(addr2.getHostAddress());
		}
	};
	Set<InetAddress> destinations = new ConcurrentSkipListSet<InetAddress>(inetcomparator);

	/**
	 * Used to send out status message
	 */
	ConcurrentHashMap<InetAddress, StatusSynMsg> m_statusMsgMap = new ConcurrentHashMap<InetAddress, StatusSynMsg>();
	private TreeBasedTable<String, ByteBuffer, Long> m_versionMaps = TreeBasedTable.create();
	private byte[] m_versionMaplock = new byte[0];
	private ScheduledFuture<?> scheduledHeartBeatTask;
	private AtomicInteger version = new AtomicInteger(0);
	public static final HeartBeater instance = new HeartBeater();
	private String localDCName = DatabaseDescriptor.getLocalDataCenter();
	private boolean enable = ConfReader.instance.heartbeatEnable();
	

	private HeartBeater() {
		if (enable) {
			/*
			 * register with the Failure Detector for receiving Failure detector
			 * events
			 */
			FailureDetector.instance.registerFailureDetectionEventListener(this);

			// Register this instance with JMX
			try {
				MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
				mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
			} catch (Exception e) {
				logger.error("exception when register HeartBeater", e);
				throw new RuntimeException(e);
			}
		}
	}

	private class HeartBeatTask implements Runnable {
		@Override
		public void run() {
			// wait on messaging service to start listening
			MessagingService.instance().waitUntilListening();

			if (!m_statusMsgMap.isEmpty()) {
				// update heartbeat version
				version.incrementAndGet();

				long sendTime = System.currentTimeMillis();

				// Send out status syn msg
				for (Map.Entry<InetAddress, StatusSynMsg> entry : m_statusMsgMap.entrySet()) {
					InetAddress destination = entry.getKey();
					StatusSynMsg statusSynMsg = entry.getValue();

					statusSynMsg.updateTimestamp(sendTime);
					MessageOut<StatusSynMsg> finalMsg = new MessageOut<StatusSynMsg>(MessagingService.Verb.HEARTBEAT_DIGEST, statusSynMsg, StatusSynMsg.serializer);
					MessagingService.instance().sendOneWay(finalMsg, destination);
					logger.info("Send out status msg to {} with msg {}", destination, statusSynMsg);
				}
			}

			// Clear status map
			clearStatusMap();
		}
	}

	public void start() {
		if (enable) {
			logger.info("Starting up server heartbeater");
			initializeStatusMsg();
			logger.info("Schedule task to send out heartbeat if needed");
			scheduledHeartBeatTask = executor.scheduleWithFixedDelay(new HeartBeatTask(), HeartBeater.intervalInMillis, HeartBeater.intervalInMillis, TimeUnit.MILLISECONDS);
		}
	}

	public void stop() {
		if (enable) {
			logger.info("Stop Heartbeater");
			if (scheduledHeartBeatTask != null)
				scheduledHeartBeatTask.cancel(false);
			Uninterruptibles.sleepUninterruptibly(intervalInMillis * 2, TimeUnit.MILLISECONDS);
			@SuppressWarnings("rawtypes")
			MessageOut message = new MessageOut(MessagingService.Verb.HEARTBEAT_SHOWDOWN);
			for (InetAddress ep : destinations)
				MessagingService.instance().sendOneWay(message, ep);
		}
	}

	@Override
	public void convict(InetAddress ep, double phi) {
		// Remove it from destination set
		destinations.remove(ep);
	}

	/**
	 * Called by {@link Mutation.apply}
	 * 
	 * @param mutation
	 */
	public void updateStatusMsgMap(final Mutation mutation) {
		if (mutation != null) {
			String ksName = mutation.getKeyspaceName();
			if (!HBUtils.SYSTEM_KEYSPACES.contains(ksName)) {
				ByteBuffer partitionKey = mutation.key();
				for (ColumnFamily cf : mutation.getColumnFamilies()) {
					String source = HBUtils.getMutationSource(cf);
					if (localDCName != null) {
						Version vn = HBUtils.getMutationVersion(cf);
						if (vn != null) {
							long versionNo = localDCName.equalsIgnoreCase(source) ? vn.getLocalVersion() : -1;
							long timestamp = vn.getTimestamp() / 1000;
							updateStatusMsgMap(ksName, cf.metadata().cfName, partitionKey, versionNo, timestamp);
						} else {
							logger.error("HeartBeater::updateStatusMsgMap, VersionNo is null");
						}
					} else {
						logger.error("HeartBeater::updateStatusMsgMap, localDCName is null");
					}

				}
			}
		} else {
			logger.error("HeartBeater::updateStatusMsgMap, mutation is null");
		}
	}

	public long getKeyVersionNo(String inKSName, ByteBuffer inKey) {
		long version = -1;
		synchronized (m_versionMaplock) {
			Long atomicLong = m_versionMaps.get(inKSName, inKey);
			if (atomicLong == null) {
				m_versionMaps.put(inKSName, inKey, 0l);
				version = 0;
			} else {
				version = ++atomicLong ;
			}
		}
		return version;
	}

	/**
	 * Called by {@link HeartBeater::start}
	 */
	private void initializeStatusMsg() {
		logger.info("Initalize status msg");
		Set<KeyMetaData> keys = HBUtils.getLocalSavedPartitionKeys();
		for (KeyMetaData keyMetaData : keys) {
			updateStatusMsgMap(keyMetaData.getKsName(), keyMetaData.getCfName(), keyMetaData.getKey(), keyMetaData.getRow());
		}
	}

	/**
	 * Called by {@link #initializeStatusMsg} & {@link #updateStatusMsgMap }
	 * 
	 * @param inKSName
	 * @param inCFName
	 * @param partitionKey
	 * @param value
	 */
	private void updateStatusMsgMap(String inKSName, String inCFName, ByteBuffer partitionKey, Row value) {
		if (value != null) {
			try {
				String source = value.getString(HBConsts.SOURCE);
				long vn = localDCName.equalsIgnoreCase(source) ? value.getLong(HBConsts.VERSON_NO) : -1;
				long ts = value.getLong(HBConsts.VERSION_WRITE_TIME) / 1000;
				m_versionMaps.put(inKSName, partitionKey, vn);
				updateStatusMsgMap(inKSName, inCFName, partitionKey, vn, ts);
			} catch (Exception e) {
				logger.debug("Exception when update status msg mp", e);
			}
		}
	}

	/**
	 * Update status map info
	 * 
	 * @param partitionKey
	 * @param version
	 * @param timestamp
	 */
	private void updateStatusMsgMap(String inKSName, String inCFName, ByteBuffer partitionKey, Long version, long timestamp) {
		List<InetAddress> replicaList = HBUtils.getReplicaList(inKSName, partitionKey);
		replicaList.remove(replicaList.remove(DatabaseDescriptor.getListenAddress()));
		CFMetaData cfMetaData = Schema.instance.getKSMetaData(inKSName).cfMetaData().get(inCFName);
		for (InetAddress inetAddress : replicaList) {
			StatusSynMsg statusMsgSyn = m_statusMsgMap.get(inetAddress);
			if (statusMsgSyn == null) {
				statusMsgSyn = new StatusSynMsg(DatabaseDescriptor.getLocalDataCenter(), null, System.currentTimeMillis());
				m_statusMsgMap.put(inetAddress, statusMsgSyn);
			}
			statusMsgSyn.addKeyVersion(HBUtils.byteBufferToString(cfMetaData, partitionKey), version, timestamp);
		}
	}

	/**
	 * Called by {@link HeartbeatTask.run}
	 */
	private void clearStatusMap() {
		for (StatusSynMsg msg : m_statusMsgMap.values()) {
			msg.cleanData();
		}
	}
}
