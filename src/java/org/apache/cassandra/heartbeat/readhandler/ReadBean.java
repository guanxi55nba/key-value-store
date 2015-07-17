package org.apache.cassandra.heartbeat.readhandler;

import java.nio.ByteBuffer;

public class ReadBean
{
    public final String ksName;
    public final ByteBuffer key;
    public ReadBean(String ksName, ByteBuffer key)
    {
        super();
        this.ksName = ksName;
        this.key = key;
    }
}
