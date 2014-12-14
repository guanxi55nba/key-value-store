package org.apache.cassandra.heartbeat;

import java.net.InetAddress;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.heartbeat.status.StatusMap;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartBeatVerbHandler implements IVerbHandler<StatusSynMsg> {

	private static final Logger logger = LoggerFactory.getLogger(HeartBeatVerbHandler.class);

	@Override
	public void doVerb(MessageIn<StatusSynMsg> message, int id) {
		// Get datacenter name
		InetAddress from = message.from;
		String dcName = DatabaseDescriptor.getEndpointSnitch().getDatacenter(from);

		// Update multi dc status map
		StatusMap.instance.updateStatusMap(dcName, message.payload);

		// Check pending read

	}

	/**
	 * insert into demo.name ( id, vn, vt ) values ( 1, 3, dateof ( now ( ) ) );
	 * 
	 * @param key
	 * @param timestamp
	 */
	public static void updateValidToField(String keyspace, String columnFamily, int key, long timestamp) {
		StringBuffer sb = new StringBuffer();
		sb.append("insert into ");
		sb.append(keyspace);
		sb.append(".");
		sb.append(columnFamily);
		sb.append(" ( id, vt ) values (");
		sb.append(key);
		sb.append(" ,");
		sb.append(timestamp);
		sb.append(" )");
		try {
			QueryProcessor.process(sb.toString(), ConsistencyLevel.LOCAL_ONE);
		} catch (Exception e) {
			logger.info("updateValidToField {}", e);
		}
	}
}
