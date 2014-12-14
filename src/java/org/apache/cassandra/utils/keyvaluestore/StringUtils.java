package org.apache.cassandra.utils.keyvaluestore;

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.Sets;

public class StringUtils {

    public static String toString(Collection<String> items) {
	StringBuilder sb = new StringBuilder();
	boolean isFirst = true;
	for (String str : items) {
	    if (!isFirst) {
		sb.append(",");
	    }

	    sb.append(str);
	    isFirst = false;
	}
	return sb.toString();
    }

    public static Set<String> fromString(String str) {
	String[] nodesArr = str.split(",");
	Set<String> nodes = Sets.newHashSet(nodesArr);
	return nodes;
    }
}
