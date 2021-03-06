package org.apache.cassandra.heartbeat;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
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
	final String ksName;
	final String srcName;
	long timestamp;
	private TreeMap<String, TreeMap<Long, Long>> m_data;

	public StatusSynMsg(String ksName, String srcName, TreeMap<String, TreeMap<Long, Long>> data, long timestamp) {
		this.ksName = ksName;
		this.srcName = srcName;
		this.timestamp = timestamp;
		this.m_data = data;
		if (m_data == null)
			m_data = new TreeMap<String, TreeMap<Long, Long>>();
	}

	public void addKeyVersion(String key, Long version, Long timestamp) {
		TreeMap<Long, Long> treeMap = m_data.get(key);
		synchronized (this) {
			if (treeMap == null) {
				treeMap = new TreeMap<Long, Long>();
				m_data.put(key, treeMap);
			}
			treeMap.put(version, timestamp);
		}
	}

	public void updateTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * { key: [vn, ts] }
	 * 
	 * @return
	 */
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
		synchronized (this) {
			for (TreeMap<Long, Long> treeMap : m_data.values()) {
				treeMap.clear();
			}
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{ ");
		sb.append("Src: ");
		sb.append(srcName);
		sb.append(", ");
		synchronized (this) {
			Iterator<Entry<String, TreeMap<Long, Long>>> iterator = m_data.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<String, TreeMap<Long, Long>> dataEntry = iterator.next();
				sb.append(dataEntry.getKey());
				sb.append(":");
				sb.append("[ ");
				for (Map.Entry<Long, Long> entry : dataEntry.getValue().entrySet()) {
					sb.append(entry.getKey());
					sb.append(":");
					sb.append("'");
					sb.append(HBUtils.dateFormat(entry.getValue()));
					sb.append("'");
					sb.append(",");
				}
				if (dataEntry.getValue().size() > 0)
					sb.setCharAt(sb.length() - 1, ']');
				else
					sb.append("]");
				sb.append(", ");
			}
		}
		sb.append("TS: ");
		sb.append(HBUtils.dateFormat(timestamp));
		sb.append(" }");
		return sb.toString();
	}

	public String getSrcName() {
		return srcName;
	}
	
	public String getKsName() {
		return ksName;
	}
}

class StatusMsgSerializationHelper implements IVersionedSerializer<StatusSynMsg> {
	@Override
	public void serialize(StatusSynMsg msg, DataOutputPlus out, int version) throws IOException {
		out.writeUTF(msg.ksName);
		out.writeUTF(msg.srcName);
		out.writeLong(msg.getTimestamp());
		out.writeLong(msg.getData().size());
		synchronized (msg) {
			for (Map.Entry<String, TreeMap<Long, Long>> entry : msg.getData().entrySet()) {
				out.writeUTF(entry.getKey());
				out.writeLong(entry.getValue().size());
				for (Map.Entry<Long, Long> inner : entry.getValue().entrySet()) {
					out.writeLong(inner.getKey());
					out.writeLong(inner.getValue());
				}
			}
		}
	}

	@Override
	public synchronized StatusSynMsg deserialize(DataInput in, int version) throws IOException {
		String ksName = in.readUTF();
		String srcName = in.readUTF();
		long timestamp = in.readLong();
		long keySize = in.readLong();
		TreeMap<String, TreeMap<Long, Long>> data = new TreeMap<String, TreeMap<Long,Long>>();
		for (int i = 0; i < keySize; i++) {
			String key = in.readUTF();
			long mapSize = in.readLong();
			TreeMap<Long, Long> maps = new TreeMap<Long, Long>();
			for (int j = 0; j < mapSize; j++) {
				maps.put(in.readLong(), in.readLong());
			}
			data.put(key, maps);
		}
		return new StatusSynMsg(ksName,srcName, data, timestamp);
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
		long size = TypeSizes.NATIVE.sizeof(statusMsgSyn.srcName);
		size += TypeSizes.NATIVE.sizeof(statusMsgSyn.getTimestamp());
		size += TypeSizes.NATIVE.sizeof(Long.valueOf(statusMsgSyn.getData().size()));
		synchronized (statusMsgSyn) {
			for (Map.Entry<String, TreeMap<Long, Long>> entry : statusMsgSyn.getData().entrySet()) {
				size += TypeSizes.NATIVE.sizeof(entry.getKey());
				size += TypeSizes.NATIVE.sizeof(Long.valueOf(entry.getValue().size()));
				for (Map.Entry<Long, Long> inner : entry.getValue().entrySet()) {
					size += TypeSizes.NATIVE.sizeof(inner.getKey());
					size += TypeSizes.NATIVE.sizeof(inner.getValue());
				}
			}
		}
		return size;
	}
}