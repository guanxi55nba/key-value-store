package org.apache.cassandra.heartbeat;

import java.io.DataInput;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;


/**
 * { key, { src1: [v1:ts1, v2:ts2, v3:ts3], src2: [v4:ts4, v5:ts5, v6:ts6] } }
 * 
 * @author XiGuan
 * 
 */
public class StatusSynMsg
{
    public static final IVersionedSerializer<StatusSynMsg> serializer = new StatusMsgSerializationHelper();
    protected static final Logger logger = LoggerFactory.getLogger(StatusSynMsg.class);
    final String ksName;
    long timestamp;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>> m_data = 
    		new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>>();
    private HashMap<String,HashMap<String, TreeMap<Long, Long>>> m_dataCopy;
    
    public StatusSynMsg(String ksName, long timestamp)
    {
        this.ksName = ksName;
        this.timestamp = timestamp;
    }
    
	public void addKeyVersion(String key, String src, Long version, Long timestamp) {
		ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> srcToVnMap = m_data.get(key);
		if (srcToVnMap == null) {
			ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> newSrcToVsTsMap = new ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>();
			srcToVnMap = m_data.putIfAbsent(key, newSrcToVsTsMap);
			if (srcToVnMap == null)
				srcToVnMap = newSrcToVsTsMap;
		}

		ConcurrentSkipListMap<Long, Long> vnTsMap = srcToVnMap.get(src);
		if (vnTsMap == null) {
			ConcurrentSkipListMap<Long, Long> newVnTsMap = new ConcurrentSkipListMap<Long, Long>();
			vnTsMap = srcToVnMap.putIfAbsent(src, newVnTsMap);
			if (vnTsMap == null)
				vnTsMap = newVnTsMap;
		}
		vnTsMap.put(version, timestamp);
	}

    public void updateTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    /**
     * { key: { src: [vn, ts] } }
     * 
     * @return
     */
    public ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>> getData()
    {
        return m_data;
    }
    
    public long getTimestamp()
    {
        return timestamp;
    }

	public void cleanData(HashMap<String, HashMap<String, TreeMap<Long, Long>>> keySrcVnMap) {
		for (Entry<String, HashMap<String, TreeMap<Long, Long>>> keySrcVnMapEntry : keySrcVnMap.entrySet()) {
			// continue if value is empty
			if (keySrcVnMapEntry.getValue().isEmpty())
				continue;

			ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> srcVnMap = m_data.get(keySrcVnMapEntry.getKey());
			if (srcVnMap == null)
				continue;

			for (Entry<String, TreeMap<Long, Long>> srcVnMapEntry : keySrcVnMapEntry.getValue().entrySet()) {
				if (srcVnMapEntry.getValue().isEmpty())
					continue;

				ConcurrentSkipListMap<Long, Long> vnMap = srcVnMap.get(srcVnMapEntry.getKey());
				if (vnMap == null)
					continue;

				for (Long vn : srcVnMapEntry.getValue().keySet())
					vnMap.remove(vn);
			}
		}
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{ ");
		for (Entry<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>> keySrcVnMapEntry : m_data.entrySet()) {
			sb.append(keySrcVnMapEntry.getKey());
			sb.append(":");
			sb.append(" {");
			for (Entry<String, ConcurrentSkipListMap<Long, Long>> srcVnMapEntry : keySrcVnMapEntry.getValue().entrySet()) {
				sb.append(srcVnMapEntry.getKey());
				sb.append(" : ");
				sb.append("[ ");
				for (Entry<Long, Long> vnMapEntry : srcVnMapEntry.getValue().entrySet()) {
					sb.append(vnMapEntry.getKey());
					sb.append(":");
					sb.append("'");
					sb.append(HBUtils.dateFormat(vnMapEntry.getValue()));
					sb.append("'");
					sb.append(",");
				}
				if (srcVnMapEntry.getValue().size() > 0)
					sb.setCharAt(sb.length() - 1, ']');
				else
					sb.append("]");
				sb.append(", ");
			}

			if (keySrcVnMapEntry.getValue().size() > 0)
				sb.setCharAt(sb.length() - 1, '}');
			else
				sb.append("}");

			sb.append(", ");
		}
		sb.append("TS: ");
		sb.append(HBUtils.dateFormat(timestamp));
		sb.append(" }");
		return sb.toString();
	}

	public String dataCopyToString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{ ");
		for (Entry<String, HashMap<String, TreeMap<Long, Long>>> keySrcVnMapEntry : m_dataCopy.entrySet()) {
			sb.append(keySrcVnMapEntry.getKey());
			sb.append(":");
			sb.append(" {");
			for (Entry<String, TreeMap<Long, Long>> srcVnMapEntry : keySrcVnMapEntry.getValue().entrySet()) {
				sb.append(srcVnMapEntry.getKey());
				sb.append(" : ");
				sb.append("[ ");
				for (Entry<Long, Long> vnMapEntry : srcVnMapEntry.getValue().entrySet()) {
					sb.append(vnMapEntry.getKey());
					sb.append(":");
					sb.append("'");
					sb.append(HBUtils.dateFormat(vnMapEntry.getValue()));
					sb.append("'");
					sb.append(",");
				}
				if (srcVnMapEntry.getValue().size() > 0)
					sb.setCharAt(sb.length() - 1, ']');
				else
					sb.append("]");
				sb.append(", ");
			}

			if (keySrcVnMapEntry.getValue().size() > 0)
				sb.setCharAt(sb.length() - 1, '}');
			else
				sb.append("}");

			sb.append(", ");
		}
		sb.append("TS: ");
		sb.append(HBUtils.dateFormat(timestamp));
		sb.append(" }");
		return sb.toString();
	}

    public String toStringLite()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        sb.append("TS: ");
        sb.append(HBUtils.dateFormat(timestamp));
        sb.append(", ");
		for (Entry<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>> keySrcVnMapSrc : m_data.entrySet()) {
			sb.append(keySrcVnMapSrc.getKey());
			sb.append(" : ");
			sb.append(keySrcVnMapSrc.getValue().size());
			sb.append(", ");
		}
        sb.append(" }");
        return sb.toString();
    }

    public String getKsName()
    {
        return ksName;
    }
    
    public StatusSynMsg copy()
    {
        return new StatusSynMsg(ksName, getNonEmmptyData(), timestamp);
    }
    
	HashMap<String, HashMap<String, TreeMap<Long, Long>>> getNonEmmptyData() {
		HashMap<String, HashMap<String, TreeMap<Long, Long>>> dataCopy = Maps.newHashMap();
		for (Entry<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>> keySrcVnMap : m_data.entrySet()) {
			if (!keySrcVnMap.getValue().isEmpty()) {
				HashMap<String, TreeMap<Long, Long>> srcToVnMap = Maps.newHashMap();
				for (Entry<String, ConcurrentSkipListMap<Long, Long>> subVnMapEntry : keySrcVnMap.getValue().entrySet()) {
					if (!subVnMapEntry.getValue().isEmpty()) {
						srcToVnMap.put(subVnMapEntry.getKey(), Maps.newTreeMap(subVnMapEntry.getValue()));
					}
				}
				dataCopy.put(keySrcVnMap.getKey(), srcToVnMap);
			}
		}
		return dataCopy;
	}
    
	public HashMap<String, HashMap<String, TreeMap<Long, Long>>> getDataCopy() {
		return m_dataCopy;
	}
    
    /**
     * Only used in serialization
     * 
     * @param ksName
     * @param dataCopy
     * @param timestamp
     */
    protected StatusSynMsg(String ksName, ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>> dataCopy, long timestamp)
    {
        this.ksName = ksName;
        this.timestamp = timestamp;
        if (dataCopy != null && !dataCopy.isEmpty())
            m_data = dataCopy;
    }
    
    /**
     * Only used to speed up the serialization
     * 
     * @param ksName
     * @param dataCopy
     * @param timestamp
     */
    protected StatusSynMsg(String ksName, HashMap<String,HashMap<String, TreeMap<Long, Long>>> dataCopy, long timestamp)
    {
        this.ksName = ksName;
        this.timestamp = timestamp;
        if (dataCopy != null)
            m_dataCopy = dataCopy;
    }
    
}

class StatusMsgSerializationHelper implements IVersionedSerializer<StatusSynMsg>
{
    @Override
	public void serialize(StatusSynMsg msg, DataOutputPlus out, int version) throws IOException {
		out.writeUTF(msg.ksName);
		out.writeLong(msg.getTimestamp());
		HashMap<String, HashMap<String, TreeMap<Long, Long>>> keySrcVnMap = msg.getDataCopy();
		int keySrcVnMapSize = keySrcVnMap.size();
		out.writeInt(keySrcVnMapSize);
		
		if (keySrcVnMapSize > 0) {
			for (Entry<String, HashMap<String, TreeMap<Long, Long>>> keySrcVnMapEntry : keySrcVnMap.entrySet()) {
				// key
				out.writeUTF(keySrcVnMapEntry.getKey());
				
				// src vn map
				HashMap<String, TreeMap<Long, Long>> srcVnMap = keySrcVnMapEntry.getValue();
				int srcVnMapSize = srcVnMap.size();
				out.writeInt(srcVnMapSize);
				
				if (srcVnMapSize > 0) {
					for (Entry<String, TreeMap<Long, Long>> srcVnMapEntry : srcVnMap.entrySet()) {
						// src
						out.writeUTF(srcVnMapEntry.getKey());

						// vnMap
						TreeMap<Long, Long> vnMap = srcVnMapEntry.getValue();
						int vnMapSize = vnMap.size();
						out.writeInt(vnMapSize);

						if (vnMapSize > 0) {
							for (Entry<Long, Long> vnMapEntry : vnMap.entrySet()) {
								out.writeLong(vnMapEntry.getKey());
								out.writeLong(vnMapEntry.getValue());
							}
						}
					}
				}
			}
		}
	}

    @Override
	public StatusSynMsg deserialize(DataInput in, int version) throws IOException {
		String ksName = in.readUTF();
		long timestamp = in.readLong();
		int keySrcVnMapSize = in.readInt();
		ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>> keySrcVnMap = 
				new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>>();

		for (int i = 0; i < keySrcVnMapSize; i++) {
			String key = in.readUTF();
			int srcVnMapSize = in.readInt();
			ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> srcVnMap = new ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>();

			for (int j = 0; j < srcVnMapSize; j++) {
				String src = in.readUTF();
				int vnMapSize = in.readInt();
				ConcurrentSkipListMap<Long, Long> vnMap = new ConcurrentSkipListMap<Long, Long>();

				for (int k = 0; k < vnMapSize; k++)
					vnMap.put(in.readLong(), in.readLong());
				srcVnMap.put(src, vnMap);
			}

			keySrcVnMap.put(key, srcVnMap);
		}
		return new StatusSynMsg(ksName, keySrcVnMap, timestamp);
	}

    @Override
	public long serializedSize(StatusSynMsg statusMsgSyn, int version) {
		long size = TypeSizes.NATIVE.sizeof(statusMsgSyn.ksName);
		size += TypeSizes.NATIVE.sizeof(statusMsgSyn.getTimestamp());
		int keySrcVnMapSize = statusMsgSyn.getData().size();
		size += TypeSizes.NATIVE.sizeof(keySrcVnMapSize);
		if (keySrcVnMapSize > 0) {
			for (Entry<String, HashMap<String, TreeMap<Long, Long>>> keySrcVnMapEntry : statusMsgSyn.getDataCopy().entrySet()) {
				size += TypeSizes.NATIVE.sizeof(keySrcVnMapEntry.getKey());
				HashMap<String, TreeMap<Long, Long>> srcVnMap = keySrcVnMapEntry.getValue(); 
				int srcVnMapSize = srcVnMap.size();
				size += TypeSizes.NATIVE.sizeof(srcVnMapSize);
				if (srcVnMapSize > 0) {
					for (Entry<String, TreeMap<Long, Long>> srcVnMapEntry : srcVnMap.entrySet()) {
						size += TypeSizes.NATIVE.sizeof(srcVnMapEntry.getKey());
						TreeMap<Long, Long> vnMap = srcVnMapEntry.getValue();
						int vnMapSize = vnMap.size();
						size += TypeSizes.NATIVE.sizeof(vnMapSize);
						if (vnMapSize > 0) {
							for (Entry<Long, Long> vnMapEntry : vnMap.entrySet()) {
								size += TypeSizes.NATIVE.sizeof(vnMapEntry.getKey());
								size += TypeSizes.NATIVE.sizeof(vnMapEntry.getValue());
							}
						}
					}
				}
			}
		}
		return size;
	}
}