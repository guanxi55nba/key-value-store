package org.apache.cassandra.heartbeat;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
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
 * { key: [v1:ts1, v2:ts2, v3:ts3], key: [v1:ts1, v2:ts2, v3:ts3] }
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
    private ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> m_data = new ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>();
    private HashMap<String, TreeMap<Long, Long>> m_dataCopy = Maps.newHashMap();
    
    public StatusSynMsg(String ksName, long timestamp)
    {
        this.ksName = ksName;
        this.timestamp = timestamp;
    }
    
    public void addKeyVersion(String key, Long version, Long timestamp)
    {
        ConcurrentSkipListMap<Long, Long> vnTsMap = m_data.get(key);
        if (vnTsMap == null)
        {
            ConcurrentSkipListMap<Long, Long> newVnTsMap = new ConcurrentSkipListMap<Long, Long>();
            vnTsMap = m_data.putIfAbsent(key, newVnTsMap);
            if (vnTsMap == null)
                vnTsMap = newVnTsMap;
        }
        vnTsMap.put(version, timestamp);
    }

    protected void initialize(int keyNumber)
    {
        for (int i = 0; i < keyNumber; i++)
        {
            m_data.put("user" + i, new ConcurrentSkipListMap<Long, Long>());
        }
    }

    public void updateTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    /**
     * { key: [vn, ts] }
     * 
     * @return
     */
    public ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> getData()
    {
        return m_data;
    }
    
    public long getTimestamp()
    {
        return timestamp;
    }

    public void cleanData(HashMap<String, TreeMap<Long, Long>> keyToVns)
    {
        for (Map.Entry<String, TreeMap<Long, Long>> entry : keyToVns.entrySet())
        {
            ConcurrentSkipListMap<Long, Long> vnTsMap = m_data.get(entry.getKey());
            if (vnTsMap != null)
            {
                for (Map.Entry<Long, Long> subEntry : entry.getValue().entrySet())
                    vnTsMap.remove(subEntry.getKey(), subEntry.getValue());
            }
        }
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        for (Entry<String, ConcurrentSkipListMap<Long, Long>> dataEntry : m_data.entrySet())
        {
            sb.append(dataEntry.getKey());
            sb.append(":");
            sb.append("[ ");
            for (Map.Entry<Long, Long> entry : dataEntry.getValue().entrySet())
            {
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
        sb.append(" }");
        sb.append(", ");
        sb.append("Size: ");
        sb.append(m_data.size());
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
    
    HashMap<String, TreeMap<Long, Long>> getNonEmmptyData()
    {
        m_dataCopy.clear();
        for (Map.Entry<String, ConcurrentSkipListMap<Long, Long>> entry : m_data.entrySet())
        {
            synchronized (entry.getValue())
            {
                if (!entry.getValue().isEmpty())
                    m_dataCopy.put(entry.getKey(), Maps.newTreeMap(entry.getValue()));
            }
        }
        return m_dataCopy;
    }
    
    public HashMap<String, TreeMap<Long, Long>> getDataCopy()
    {
        return m_dataCopy;
    }
    
    /**
     * Only used in serialization
     * 
     * @param ksName
     * @param dataCopy
     * @param timestamp
     */
    protected StatusSynMsg(String ksName, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> dataCopy, long timestamp)
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
    protected StatusSynMsg(String ksName, HashMap<String, TreeMap<Long, Long>> dataCopy, long timestamp)
    {
        this.ksName = ksName;
        this.timestamp = timestamp;
        if (dataCopy != null && !dataCopy.isEmpty())
            m_dataCopy = dataCopy;
    }
    
    
}

class StatusMsgSerializationHelper implements IVersionedSerializer<StatusSynMsg>
{
    @Override
    public void serialize(StatusSynMsg msg, DataOutputPlus out, int version) throws IOException
    {
        out.writeUTF(msg.ksName);
        out.writeLong(msg.getTimestamp());
        HashMap<String, TreeMap<Long, Long>> data = Maps.newHashMap();
        int dataSize = data.size();
        out.writeInt(dataSize);
        if (dataSize > 0)
        {
            for (Map.Entry<String, TreeMap<Long, Long>> entry : data.entrySet())
            {
                out.writeUTF(entry.getKey());
                int valueSize = entry.getValue().size(); 
                out.writeInt(valueSize);
                if (valueSize > 0)
                {
                    for (Map.Entry<Long, Long> inner : entry.getValue().entrySet())
                    {
                        out.writeLong(inner.getKey());
                        out.writeLong(inner.getValue());
                    }
                }
            }
        }
    }

    @Override
    public StatusSynMsg deserialize(DataInput in, int version) throws IOException
    {
        String ksName = in.readUTF();
        long timestamp = in.readLong();
        int dataSize = in.readInt();
        ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> data = new ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>();
        if (dataSize > 0)
        {
            for (int i = 0; i < dataSize; i++)
            {
                String key = in.readUTF();
                int valueSize = in.readInt();
                ConcurrentSkipListMap<Long, Long> maps = new ConcurrentSkipListMap<Long, Long>();
                for (int j = 0; j < valueSize; j++)
                    maps.put(in.readLong(), in.readLong());
                data.put(key, maps);
            }
        }
        return new StatusSynMsg(ksName,  data, timestamp);
    }

    public static byte[] readByteArray(DataInput in) throws IOException
    {
        int length = 0;
        if (in instanceof DataInputStream)
        {
            length = ((DataInputStream) in).available();
        }
        byte[] theBytes = new byte[length];
        in.readFully(theBytes);
        return theBytes;
    }

    @Override
    public long serializedSize(StatusSynMsg statusMsgSyn, int version)
    {
        long size = TypeSizes.NATIVE.sizeof(statusMsgSyn.ksName);
        size += TypeSizes.NATIVE.sizeof(statusMsgSyn.getTimestamp());
        int dataSize = statusMsgSyn.getData().size();
        size += TypeSizes.NATIVE.sizeof(dataSize);
        if (dataSize > 0)
        {
            for (Map.Entry<String, ConcurrentSkipListMap<Long, Long>> entry : statusMsgSyn.getData().entrySet())
            {
                size += TypeSizes.NATIVE.sizeof(entry.getKey());
                int valueSize = entry.getValue().size();
                size += TypeSizes.NATIVE.sizeof(valueSize);
                if (valueSize > 0)
                {
                    for (Map.Entry<Long, Long> inner : entry.getValue().entrySet())
                    {
                        size += TypeSizes.NATIVE.sizeof(inner.getKey());
                        size += TypeSizes.NATIVE.sizeof(inner.getValue());
                    }
                }
            }
        }
        return size;
    }
}