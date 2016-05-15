package org.apache.cassandra.heartbeat;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.heartbeat.extra.HBConsts;
import org.apache.cassandra.heartbeat.extra.Version;
import org.apache.cassandra.heartbeat.utils.ConfReader;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final DebuggableScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor(1,"HeartBeatTasks",Thread.MAX_PRIORITY);
//    private static final DebuggableScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("HeartBeatTasks");
    public final static int intervalInMillis = ConfReader.getHeartbeatInterval();
    private final Comparator<InetAddress> inetcomparator = new Comparator<InetAddress>()
    {
        public int compare(InetAddress addr1, InetAddress addr2)
        {
            return addr1.getHostAddress().compareTo(addr2.getHostAddress());
        }
    };
    Set<InetAddress> destinations = new ConcurrentSkipListSet<InetAddress>(inetcomparator);

    /**
     * Used to send out status message
     */
    ConcurrentHashMap<InetAddress, StatusSynMsg> m_statusMsgMap = new ConcurrentHashMap<InetAddress, StatusSynMsg>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<ByteBuffer, AtomicLong>> m_versionMaps = new ConcurrentHashMap<String, ConcurrentHashMap<ByteBuffer, AtomicLong>>();
    private ScheduledFuture<?> scheduledHeartBeatTask;
    public static final HeartBeater instance = new HeartBeater();
    private String localSrcName = HBUtils.getLocalAddress().getHostAddress();
    private boolean enable = ConfReader.heartbeatEnable();
    

    private HeartBeater()
    {
        if (enable)
        {
            /*
             * register with the Failure Detector for receiving Failure detector events
             */
            FailureDetector.instance.registerFailureDetectionEventListener(this);

            // Register this instance with JMX
            try
            {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
            }
            catch (Exception e)
            {
                logger.error("exception when register HeartBeater", e);
                throw new RuntimeException(e);
            }
        }
    }

    public void start()
    {
        if (enable)
        {
            logger.info("Starting up server heartbeater");
            initializeStatusMsg();
            logger.info("Schedule task to send out heartbeat if needed");
            scheduledHeartBeatTask = executor.scheduleAtFixedRate(new HeartBeatTask(), HeartBeater.intervalInMillis, HeartBeater.intervalInMillis, TimeUnit.MILLISECONDS);
        }
    }
    
    /**
     * Called by {@link HeartBeater::start}
     */
    private void initializeStatusMsg()
    {
        logger.info("Initalize status msg");
        Set<KeyMetaData> keys = HBUtils.getLocalSavedPartitionKeys();
        synchronized (m_statusMsgMap)
        {
            for (KeyMetaData keyMetaData : keys)
                updateStatusMsgMap(keyMetaData.getKsName(), keyMetaData.getKey(), keyMetaData.getRow());
        }
        
    }
    
    private class HeartBeatTask implements Runnable {
        @Override
        public void run()
        {
            // wait on messaging service to start listening
            MessagingService.instance().waitUntilListening();

            if (!m_statusMsgMap.isEmpty())
            {
                // Send out status syn msg
                for (Map.Entry<InetAddress, StatusSynMsg> entry : m_statusMsgMap.entrySet())
                {
                    InetAddress destination = entry.getKey();
                    StatusSynMsg statusSynMsg = entry.getValue().copy();
                    statusSynMsg.updateTimestamp(System.currentTimeMillis());
                    MessageOut<StatusSynMsg> finalMsg = new MessageOut<StatusSynMsg>(MessagingService.Verb.HEARTBEAT_DIGEST, statusSynMsg, StatusSynMsg.serializer);
                    MessagingService.instance().sendOneWay(finalMsg, destination);
                    if(ConfReader.isLogEnabled())
                    	logger.info("Send out status msg to {} with msg {}", destination, statusSynMsg.dataCopyToString());

                    // clear status syn msg once is sent out
                    entry.getValue().cleanData(statusSynMsg.getDataCopy());;
                }
            }
        }
    }
    
    public long getKeyVersionNo(String inKSName, ByteBuffer inKey)
    {
        AtomicLong version = new AtomicLong(-1);
        ConcurrentHashMap<ByteBuffer, AtomicLong> keyToVn = m_versionMaps.get(inKSName);
        if (keyToVn == null)
        {
            ConcurrentHashMap<ByteBuffer, AtomicLong> newMap = new ConcurrentHashMap<ByteBuffer, AtomicLong>();
            keyToVn = m_versionMaps.putIfAbsent(inKSName, newMap);
            if (keyToVn == null)
                keyToVn = newMap;
        }

        AtomicLong savedVersion = keyToVn.putIfAbsent(inKey, version);
        if (savedVersion == null)
            savedVersion = version;
        return savedVersion.incrementAndGet();
    }

    /**
     * <-- called by {@link Mutation.apply}
     * 
     * @param mutation
     */
    public void updateStatusMsgMap(final String ksName, ByteBuffer partitionKey, final Collection<ColumnFamily> CF)
    {
        if (!HBUtils.SYSTEM_KEYSPACES.contains(ksName))
        {
            for (ColumnFamily cf : CF)
            {
                String source = HBUtils.getMutationSource(cf);
                Version vn = HBUtils.getMutationVersion(cf);
                if (vn != null)
                {
                    long timestamp = vn.getTimestamp() / 1000;
                    
                    updateStatusMsgMap(ksName, partitionKey,source, vn.getLocalVersion(), timestamp);
                }
                else
                {
                    logger.error("HeartBeater::updateStatusMsgMap, VersionNo is null");
                }
            }
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
    private void updateStatusMsgMap(String inKSName, ByteBuffer partitionKey, Row value)
    {
        if (value != null)
        {
            try
            {
                String source = value.getString(HBConsts.SOURCE);
                long vn = value.getLong(HBConsts.VERSON_NO);
                long ts = value.getLong(HBConsts.VERSION_WRITE_TIME) / 1000;
                ConcurrentHashMap<ByteBuffer, AtomicLong> keyToVn = m_versionMaps.get(inKSName);
                if (keyToVn == null)
                {
                    keyToVn = new ConcurrentHashMap<ByteBuffer, AtomicLong>();
                    m_versionMaps.put(inKSName, keyToVn);
                }
                keyToVn.put(partitionKey, new AtomicLong(vn));
                updateStatusMsgMap(inKSName, partitionKey, source,vn, ts);
            }
            catch (Exception e)
            {
                logger.debug("Exception when update status msg mp", e);
            }
        }
    }

    /**
     * Update status map info
     * 
     * @param partitionKey
     * @param version
     * @param ts
     */
    private void updateStatusMsgMap(String inKSName, ByteBuffer partitionKey, String src, Long version, long ts)
    {
        List<InetAddress> replicaList = HBUtils.getReplicaListExcludeLocal(inKSName, partitionKey);
        for (InetAddress inetAddress : replicaList)
        {
            StatusSynMsg statusMsgSyn = m_statusMsgMap.get(inetAddress);
            if (statusMsgSyn == null)
            {
                StatusSynMsg newMsg = new StatusSynMsg(inKSName, System.currentTimeMillis());
                statusMsgSyn = m_statusMsgMap.putIfAbsent(inetAddress, newMsg);
                if (statusMsgSyn == null)
                    statusMsgSyn = newMsg;
            }
            statusMsgSyn.addKeyVersion(HBUtils.byteBufferToString( partitionKey), src, version, ts);
        }
    }
    
    @Override
    public void convict(InetAddress ep, double phi)
    {
        // Remove it from destination set
        destinations.remove(ep);
    }

    public void stop()
    {
        if (enable)
        {
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
}
