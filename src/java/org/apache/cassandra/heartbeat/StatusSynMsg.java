package org.apache.cassandra.heartbeat;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.heartbeat.utils.HBUtils;
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
public class StatusSynMsg
{
    protected static final Logger logger = LoggerFactory.getLogger(StatusSynMsg.class);
    public static final IVersionedSerializer<StatusSynMsg> serializer = new StatusMsgSerializationHelper();
    final String ksName;
    final String srcName;
    long timestamp;
    private volatile ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> m_data = new ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>();
    //Random randomGenerator = new Random();
    
    public StatusSynMsg(String ksName, String srcName, long timestamp)
    {
        this.ksName = ksName;
        this.srcName = srcName;
        this.timestamp = timestamp;
    }

    public StatusSynMsg(String ksName, String srcName, ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> data, long timestamp)
    {
        this.ksName = ksName;
        this.srcName = srcName;
        this.timestamp = timestamp;
        if (data != null && !data.isEmpty())
        {
            for (Map.Entry<String, ConcurrentSkipListMap<Long, Long>> entry : data.entrySet())
            {
                ConcurrentSkipListMap<Long, Long> result = m_data.putIfAbsent(entry.getKey(),new ConcurrentSkipListMap<Long, Long>(entry.getValue()));
                if (result != null)
                    result.putAll(entry.getValue());
            }
        }
    }

    public void addKeyVersion(String key, Long version, Long timestamp)
    {
//		key = "user" + randomGenerator.nextInt(10000);
//		version = (long) randomGenerator.nextInt(10000);
//		timestamp = System.currentTimeMillis();
        ConcurrentSkipListMap<Long, Long> vnTsMap = m_data.get(key);
        if (vnTsMap == null)
        {
            vnTsMap = new ConcurrentSkipListMap<Long, Long>();
//            synchronized (m_data){
                m_data.put(key, vnTsMap);
//            }
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
        // TreeMap<String, TreeMap<Long, Long>> data = new TreeMap<String, TreeMap<Long, Long>>();
        // for (Map.Entry<String, ConcurrentSkipListMap<Long, Long>> entry : m_data
        // .entrySet()) {
        // data.put(entry.getKey(), new TreeMap<Long, Long>(entry.getValue()));
        // }
        // return data;
        return m_data;
    }

    public ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> getDataImpl()
    {
        return m_data;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    /**
     * set it to {key: [] }
     */
    public void cleanData()
    {
        for (ConcurrentSkipListMap<Long, Long> vnTsMap : m_data.values()){
//            synchronized (m_data)
//            {
                vnTsMap.clear();
//            }
        }
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        sb.append("Src: ");
        sb.append(srcName);
        sb.append(", ");
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
        sb.append("Src: ");
        sb.append(srcName);
        sb.append(", ");
        sb.append("TS: ");
        sb.append(HBUtils.dateFormat(timestamp));
        sb.append(" }");
        sb.append(", ");
        sb.append("Size: ");
        sb.append(m_data.size());
        return sb.toString();
    }

    public String getSrcName()
    {
        return srcName;
    }

    public String getKsName()
    {
        return ksName;
    }
    
    public StatusSynMsg copy()
    {
        return new StatusSynMsg(ksName, srcName, m_data, timestamp);
    }
    
    ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> dataCopy(){
        ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> dataCopy = new ConcurrentHashMap<String, ConcurrentSkipListMap<Long,Long>>();
        for (Map.Entry<String, ConcurrentSkipListMap<Long, Long>> entry : m_data.entrySet())
        {
            ConcurrentSkipListMap<Long, Long> result = dataCopy.putIfAbsent(entry.getKey(),new ConcurrentSkipListMap<Long, Long>(entry.getValue()));
            if (result != null)
                result.putAll(entry.getValue());
        }
        return dataCopy;
    }
}

class StatusMsgSerializationHelper implements IVersionedSerializer<StatusSynMsg>
{
    @Override
    public void serialize(StatusSynMsg msg, DataOutputPlus out, int version) throws IOException
    {
        out.writeUTF(msg.ksName);
        out.writeUTF(msg.srcName);
        out.writeLong(msg.getTimestamp());
        ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>> data = msg.getData();
        int dataSize = data.size();
        out.writeInt(dataSize);
        if (dataSize > 0)
        {
            for (Map.Entry<String, ConcurrentSkipListMap<Long, Long>> entry : data.entrySet())
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
        String srcName = in.readUTF();
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
                if (valueSize > 0)
                {
                    for (int j = 0; j < valueSize; j++)
                        maps.put(in.readLong(), in.readLong());
                }
                data.put(key, maps);
            }
        }
        return new StatusSynMsg(ksName, srcName, data, timestamp);
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
        size += TypeSizes.NATIVE.sizeof(statusMsgSyn.srcName);
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