package org.apache.cassandra.heartbeat.status;

import java.util.HashMap;

/**
 * Present the result on all replica nodes
 * 
 * @author XiGuan
 *
 */
public class ARResult
{
    boolean m_hasLatestValue;
    HashMap<String, KeyResult> m_blockMap;

    public ARResult(HashMap<String, KeyResult> blockMap)
    {
        m_blockMap = blockMap;
        m_hasLatestValue = m_blockMap.isEmpty();
    }
    
    public boolean value()
    {
        return m_hasLatestValue;
    }
    
    public HashMap<String, KeyResult> getBlockMap()
    {
        return m_blockMap;
    }
}
