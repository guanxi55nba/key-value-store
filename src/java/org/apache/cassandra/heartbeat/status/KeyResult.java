package org.apache.cassandra.heartbeat.status;

import org.apache.cassandra.heartbeat.utils.HBUtils;

/**
 * Bean object to present whether one key has latest value on one replica node
 * 
 * @author XiGuan
 *
 */
public class KeyResult
{
    volatile boolean m_hasLatestValue;
    volatile boolean m_causedByTs;
    volatile boolean m_causedByVn;
    long m_expectedVn = -1;
    Object m_lock = new Object();

    public KeyResult(boolean hasLatestValue, boolean causedByTs, boolean causedByVn, long expectedVn)
    {
        this.m_hasLatestValue = hasLatestValue;
        if (!m_hasLatestValue)
        {
            this.m_causedByTs = causedByTs;
            this.m_causedByVn = causedByVn;
            if (m_causedByTs)
                m_expectedVn = expectedVn;
        }
    }
    
    public void update(KeyResult inResult)
    {
        if (!m_hasLatestValue)
        {
            synchronized (m_lock)
            {
                this.m_hasLatestValue = inResult.value();
                this.m_causedByTs = inResult.isCausedByTs();
                this.m_causedByVn = inResult.isCausedByVn();
                if (m_causedByTs)
                    m_expectedVn = inResult.getExpectedVn();
            }
            checkValid();
        }
    }
    
    private void checkValid()
    {
        if ((m_causedByTs && m_causedByVn) || (m_hasLatestValue && (m_causedByTs || m_causedByVn)))
            HBUtils.error("============================ Error In KeyResult ============================");
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
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        if (m_hasLatestValue)
        {
            sb.append("has latest value");
        }
        else if (m_causedByTs)
        {
            sb.append("waiting for syn msg ");
        }
        else if (m_causedByVn)
        {
            sb.append("waiting for vn ");
            sb.append(m_expectedVn);
        }
        return sb.toString();
    }

}
