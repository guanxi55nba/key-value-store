package org.apache.cassandra.heartbeat.status;

/**
 * Bean object to present whether one key has latest value on one replica node
 * 
 * @author XiGuan
 *
 */
public class KeyResult
{
    boolean m_hasLatestValue;
    boolean m_causedByTs;
    boolean m_causedByVn;
    long m_expectedVn = -1;

    public KeyResult(boolean hasLatestValue, boolean causedByTs, boolean causedByVn, long expectedVn)
    {
        this.m_hasLatestValue = hasLatestValue;
        if (!m_hasLatestValue)
        {
            this.m_causedByTs = causedByTs;
            this.m_causedByVn = causedByVn;
            m_expectedVn = expectedVn;
        }
    }
    
    public void update(KeyResult inResult)
    {
        this.m_hasLatestValue = inResult.value();
        if (!m_hasLatestValue)
        {
            this.m_causedByTs = inResult.isCausedByTs();
            this.m_causedByVn = inResult.isCausedByVn();
            m_expectedVn = inResult.getExpectedVn();
        }
    }

    public boolean value()
    {
        return m_hasLatestValue;
    }
    
    public boolean isCausedByTs()
    {
        return m_causedByTs;
    }

    public boolean isCausedByVn()
    {
        return m_causedByVn;
    }

    public long getExpectedVn()
    {
        return m_expectedVn;
    }
    
    
    public void setValue(boolean inValue)
    {
        m_hasLatestValue = inValue;
    }
    
    public void setCausedByTs(boolean inValue)
    {
        m_causedByTs = inValue;
    }
    
    public void setcausedByVn(boolean inValue)
    {
        m_causedByVn = inValue;
    }
    
    public void setExpectedVn(long expectedVn)
    {
        m_expectedVn = expectedVn;
    }
    
    

}
