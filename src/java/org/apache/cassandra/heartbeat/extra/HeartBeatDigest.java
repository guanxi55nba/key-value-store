package org.apache.cassandra.heartbeat.extra;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;

public class HeartBeatDigest implements Comparable<HeartBeatDigest> {
	
	public static final IVersionedSerializer<HeartBeatDigest> serializer = new HeartBeatDigestSerializer();

	final InetAddress endpoint;
	final int generation;
	final int maxVersion;

	public HeartBeatDigest(InetAddress ep, int gen, int version) {
		endpoint = ep;
		generation = gen;
		maxVersion = version;
	}

	InetAddress getEndpoint() {
		return endpoint;
	}

	int getGeneration() {
		return generation;
	}

	int getMaxVersion() {
		return maxVersion;
	}

	public int compareTo(HeartBeatDigest heartBeatDigest) {
		if (generation != heartBeatDigest.generation)
			return (generation - heartBeatDigest.generation);
		return (maxVersion - heartBeatDigest.maxVersion);
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(endpoint);
		sb.append(":");
		sb.append(generation);
		sb.append(":");
		sb.append(maxVersion);
		return sb.toString();
	}
}

class HeartBeatDigestSerializer implements IVersionedSerializer<HeartBeatDigest> {

	@Override
	public void serialize(HeartBeatDigest t, DataOutputPlus out, int version) throws IOException {
		CompactEndpointSerializationHelper.serialize(t.endpoint, out);
		out.writeInt(t.generation);
		out.writeInt(t.maxVersion);
	}

	@Override
	public HeartBeatDigest deserialize(DataInput in, int version) throws IOException {
		InetAddress endpoint = CompactEndpointSerializationHelper.deserialize(in);
		int generation = in.readInt();
		int maxVersion = in.readInt();
		return new HeartBeatDigest(endpoint, generation, maxVersion);
	}

	@Override
	public long serializedSize(HeartBeatDigest t, int version) {
		long size = CompactEndpointSerializationHelper.serializedSize(t.endpoint);
		size += TypeSizes.NATIVE.sizeof(t.generation);
		size += TypeSizes.NATIVE.sizeof(t.maxVersion);
		return size;
	}

}
