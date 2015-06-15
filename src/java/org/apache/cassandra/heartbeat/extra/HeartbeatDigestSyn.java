package org.apache.cassandra.heartbeat.extra;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class HeartbeatDigestSyn {
	
	public static final IVersionedSerializer<HeartbeatDigestSyn> serializer = new HeartBeatDigestSynSerializer();

    final String clusterId;
    final String partioner;
    final List<HeartBeatDigest> heartBeatDigests;
    
    public HeartbeatDigestSyn(String clusterId, String partioner, List<HeartBeatDigest> heartbeatDigests) {
    	this.clusterId = clusterId;
    	this.partioner = partioner;
    	this.heartBeatDigests = heartbeatDigests;
	}

	public List<HeartBeatDigest> getHeartBeatDigests() {
		return heartBeatDigests;
	}

}


class HeartbeatDigestSerializationHelper
{
    static void serialize(List<HeartBeatDigest> hDigestList, DataOutputPlus out, int version) throws IOException
    {
        out.writeInt(hDigestList.size());
        for (HeartBeatDigest gDigest : hDigestList)
        	HeartBeatDigest.serializer.serialize(gDigest, out, version);
    }

    static List<HeartBeatDigest> deserialize(DataInput in, int version) throws IOException
    {
        int size = in.readInt();
        List<HeartBeatDigest> hDigests = new ArrayList<HeartBeatDigest>(size);
        for (int i = 0; i < size; ++i)
            hDigests.add(HeartBeatDigest.serializer.deserialize(in, version));
        return hDigests;
    }

    static int serializedSize(List<HeartBeatDigest> digests, int version)
    {
        int size = TypeSizes.NATIVE.sizeof(digests.size());
        for (HeartBeatDigest digest : digests)
            size += HeartBeatDigest.serializer.serializedSize(digest, version);
        return size;
    }
}

class HeartBeatDigestSynSerializer implements IVersionedSerializer<HeartbeatDigestSyn>
{
    public void serialize(HeartbeatDigestSyn gDigestSynMessage, DataOutputPlus out, int version) throws IOException
    {
        out.writeUTF(gDigestSynMessage.clusterId);
        out.writeUTF(gDigestSynMessage.partioner);
        HeartbeatDigestSerializationHelper.serialize(gDigestSynMessage.heartBeatDigests, out, version);
    }

    public HeartbeatDigestSyn deserialize(DataInput in, int version) throws IOException
    {
        String clusterId = in.readUTF();
        String partioner = null;
        partioner = in.readUTF();
        List<HeartBeatDigest> gDigests = HeartbeatDigestSerializationHelper.deserialize(in, version);
        return new HeartbeatDigestSyn(clusterId, partioner, gDigests);
    }

	public long serializedSize(HeartbeatDigestSyn syn, int version) {
		long size = TypeSizes.NATIVE.sizeof(syn.clusterId);
		size += TypeSizes.NATIVE.sizeof(syn.partioner);
		size += HeartbeatDigestSerializationHelper.serializedSize(syn.heartBeatDigests, version);
		return size;
	}
}
