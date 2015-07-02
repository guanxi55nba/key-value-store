package org.apache.cassandra.heartbeat;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.heartbeat.status.StatusMap;
import org.apache.cassandra.heartbeat.utils.ConfReader;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.corba.se.impl.protocol.giopmsgheaders.Message;

/**
 * Registered in Storage Service
 * 
 * @author xig
 *
 */
public class HeartBeatVerbHandler implements IVerbHandler<StatusSynMsg> {

	private static final Logger logger = LoggerFactory.getLogger(HeartBeatVerbHandler.class);

	@Override
	public void doVerb(MessageIn<StatusSynMsg> message, int id) {
	    //logger.info("message size: {}", message.payload.getData().size());
		if(ConfReader.instance.heartbeatEnable()) {
			//String srcName = DatabaseDescriptor.getEndpointSnitch().getDatacenter(from);
			String srcName = message.from.getHostAddress();
			// Update multi dc status map
			StatusMap.instance.updateStatusMap(srcName, message.payload);
		}
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
