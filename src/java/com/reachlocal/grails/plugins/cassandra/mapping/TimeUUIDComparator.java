package com.reachlocal.grails.plugins.cassandra.mapping;

import java.util.Comparator;
import java.util.UUID;

public class TimeUUIDComparator implements Comparator<UUID>
{
	@Override
	public boolean equals(Object obj)
	{
		return super.equals(obj);
	}

	@Override
	public int compare(UUID o1, UUID o2)
	{
		long t1 = o1.timestamp();
		long t2 = o2.timestamp();
		if (t1 < t2) {
			return -1;
		}
		else if (t1 > t2) {
			return 1;
		}
		else {
			return 0;
		}
	}
}
