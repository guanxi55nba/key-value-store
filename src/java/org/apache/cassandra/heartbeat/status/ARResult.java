package org.apache.cassandra.heartbeat.status;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.heartbeat.utils.ConfReader;

/**
 * Present the result on all replica nodes
 * 
 * @author XiGuan
 *
 */
public class ARResult
{
    String m_key;
    boolean m_hasLatestValue;
    HashMap<String, KeyResult> m_blockMap; // src, key result

    public ARResult(String inKey, HashMap<String, KeyResult> blockMap)
    {
        m_key = inKey;
        m_blockMap = blockMap;
        m_hasLatestValue = m_blockMap.size()<ConfReader.getMajorityNo();
    }
    
    public boolean value()
    {
        return m_hasLatestValue;
    }
    
    public HashMap<String, KeyResult> getBlockMap()
    {
        return m_blockMap;
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        if (m_hasLatestValue)
        {
            sb.append("has latest value for this read");
        }
        else
        {
            boolean temp1 = false;
            sb.append("{ ");
            for (Map.Entry<String, KeyResult> entry : m_blockMap.entrySet())
            {
                temp1 = true;
                sb.append(entry.getKey());
                sb.append(": ");
                sb.append(entry.getValue());
                sb.append(", ");
            }
            if (temp1)
            {
                sb.setCharAt(sb.length() - 2, ' ');
                sb.setCharAt(sb.length() - 1, '}');
            }
            else
            {
                sb.append("}");
            }
        }
        return sb.toString();
    }
}
