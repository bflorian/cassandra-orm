package com.reachlocal.grails.plugins.cassandra.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class UtcDate
{
	static SimpleDateFormat isoFormatter()
	{
		return formatter("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	}

	static SimpleDateFormat yearFormatter() {
		return formatter("yyyy");
	}

	static SimpleDateFormat monthFormatter()
	{
		return formatter("yyyy-MM");
	}

	static SimpleDateFormat dayFormatter()
	{
		return formatter("yyyy-MM-dd");
	}

	static SimpleDateFormat hourFormatter()
	{
		return formatter("yyyy-MM-dd'T'HH");
	}

	static SimpleDateFormat hourOnlyFormatter()
	{
		return formatter("HH");
	}

	static SimpleDateFormat formatter(String s) {
		SimpleDateFormat f = new SimpleDateFormat(s);
		f.setTimeZone(TimeZone.getTimeZone("GMT"));
		return f;
	}
}
