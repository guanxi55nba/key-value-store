package org.apache.cassandra.heartbeat;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * { key: [v1:ts1, v2:ts2, v3:ts3], key: [v1:ts1, v2:ts2, v3:ts3] }
 * 
 * @author XiGuan
 * 
 */
public class StatusSynMsg {
	protected static final Logger logger = LoggerFactory.getLogger(StatusSynMsg.class);
	public static final IVersionedSerializer<StatusSynMsg> serializer = new StatusMsgSerializationHelper();
	final String dcName;
	long timestamp;
	private TreeMap<String, TreeMap<Long, Long>> m_data;

	public StatusSynMsg(String dcName, TreeMap<String, TreeMap<Long, Long>> data, long timestamp) {
		this.dcName = dcName;
		this.timestamp = timestamp;
		this.m_data = data;
		if (m_data == null)
			m_data = new TreeMap<String, TreeMap<Long, Long>>();
	}

	public void addKeyVersion(String key, Long version, Long timestamp) {
		TreeMap<Long, Long> treeMap = m_data.get(key);
		if (treeMap == null) {
			treeMap = new TreeMap<Long, Long>();
			m_data.put(key, treeMap);
		}
		treeMap.put(version, timestamp);
	}

	public void updateTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public TreeMap<String, TreeMap<Long, Long>> getData() {
		return m_data;
	}

	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * set it to {key: [] }
	 */
	public void cleanData() {
		for (TreeMap<Long, Long> treeMap : m_data.values()) {
			treeMap.clear();
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{ ");
		sb.append("DCName: ");
		sb.append(dcName);
		sb.append(", ");
		for (Map.Entry<String, TreeMap<Long, Long>> dataEntry : m_data.entrySet()) {
			sb.append(dataEntry.getKey());
			sb.append(":");
			sb.append("[ ");
			for (Map.Entry<Long, Long> entry : dataEntry.getValue().entrySet()) {
				sb.append(entry.getKey());
				sb.append(":");
				sb.append("'");
				sb.append(DateFormatUtils.format(new Date(entry.getValue()), "yyyy-MM-dd HH:mm:ss"));
				sb.append("'");
				sb.append(",");
			}
			if (dataEntry.getValue().size() > 0)
				sb.setCharAt(sb.length() - 1, ']');
			else
				sb.append("]");
			sb.append(", ");
		}
		sb.append("TS : ");
		sb.append(DateFormatUtils.format(new Date(timestamp), "yyyy-MM-dd HH:mm:SS"));
		sb.append(" }");
		return sb.toString();
	}

	public String getDCName() {
		return dcName;
	}
}

class StatusMsgSerializationHelper implements IVersionedSerializer<StatusSynMsg> {
	@Override
	public void serialize(StatusSynMsg msg, DataOutputPlus out, int version) throws IOException {
		out.writeUTF(msg.dcName);
		out.writeLong(msg.getTimestamp());
		out.write(SerializationUtils.serialize(msg.getData()));
	}

	@Override
	public StatusSynMsg deserialize(DataInput in, int version) throws IOException {
		String dcName = in.readUTF();
		long timestamp = in.readLong();
		@SuppressWarnings("unchecked")
		TreeMap<String, TreeMap<Long, Long>> data = (TreeMap<String, TreeMap<Long, Long>>) SerializationUtils
				.deserialize(readByteArray(in));
		return new StatusSynMsg(dcName, data, timestamp);
	}

	public static byte[] readByteArray(DataInput in) throws IOException {
		int length = 0;
		if (in instanceof DataInputStream) {
			length = ((DataInputStream) in).available();
		}
		byte[] theBytes = new byte[length];
		in.readFully(theBytes);
		return theBytes;
	}

	@Override
	public long serializedSize(StatusSynMsg statusMsgSyn, int version) {
		long size = TypeSizes.NATIVE.sizeof(statusMsgSyn.dcName);
		size += TypeSizes.NATIVE.sizeof(statusMsgSyn.getTimestamp());
		size += SerializationUtils.serialize(statusMsgSyn.getData()).length;
		return size;
	}
}