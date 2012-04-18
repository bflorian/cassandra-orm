package com.reachlocal.grails.plugins.cassandra.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.*;

/**
 * @author: Bob Florian
 */
public class DateHelper
{
	static public boolean isLastDayOfMonth(Calendar cal)
	{
		int m1 = cal.get(Calendar.MONTH);
		cal.add(Calendar.DAY_OF_MONTH, 1);

		int m2 = cal.get(Calendar.MONTH);
		cal.add(Calendar.DAY_OF_MONTH, -1);

		return m1 != m2;
	}

	static public Calendar setBeginningOfWholeMonth(Calendar calendar)
	{
		Calendar cal = (Calendar)calendar.clone();
		int day = cal.get(Calendar.DAY_OF_MONTH);
		int hour = cal.get(Calendar.HOUR_OF_DAY);
		if (day != 1 || hour != 0) {
			cal.add(Calendar.MONTH, 1);
			cal.set(Calendar.DAY_OF_MONTH, 1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
		}
		return cal;
	}

	static public Calendar setEndOfWholeMonth(Calendar calendar)
	{
		Calendar cal = (Calendar)calendar.clone();
		if (cal.get(Calendar.HOUR_OF_DAY) != 23 || !isLastDayOfMonth(cal)) {
			cal.set(Calendar.DAY_OF_MONTH, 1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			cal.add(Calendar.MILLISECOND, -1);
		}
		return cal;
	}

	static public Calendar setBeginningOfWholeDay(Calendar calendar)
	{
		Calendar cal = (Calendar)calendar.clone();
		if (cal.get(Calendar.HOUR_OF_DAY) != 0) {
			cal.add(Calendar.DAY_OF_MONTH, 1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
		}
		return cal;
	}

	static public Calendar setEndOfWholeDay(Calendar calendar)
	{
		Calendar cal = (Calendar)calendar.clone();
		if (cal.get(Calendar.HOUR_OF_DAY) != 23) {
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.MILLISECOND, 0);
			cal.add(Calendar.MILLISECOND, -1);
		}
		return cal;
	}

	static public Map rollUpCounterDates(Map<String, Object> map, DateFormat fromFormat, DateFormat toFormat) throws ParseException
	{
		Map result = new LinkedHashMap();
		for (Map.Entry<String, Object> entry: map.entrySet()) {
			String newKey = toFormat.format(fromFormat.parse(entry.getKey()));
			Object value = entry.getValue();
			mergeCounterMap(result, newKey, value);
		}
		return result;
	}

	static void mergeCounterMap(Map map, String key, Object value)
	{
		if (value instanceof Map) {
			mergeCounterMap(map, key, (Map)value);
		}
		else {
			mergeCounterMap(map, key, (Number)value);
		}
	}

	static void mergeCounterMap(Map<String, Map> map, String key, Map<String, Map> value)
	{
		Map item = map.get(key);
		if (item != null) {
			for (Map.Entry<String, Map> entry: value.entrySet()) {
				String k = entry.getKey();
				Object v = entry.getValue();
				mergeCounterMap(item, k, v);
			}
		}
		else {
			item = value;
		}
		map.put(key, item);
	}

	static void mergeCounterMap(Map<String, Number> map, String key, Number value)
	{
		Number total = map.get(key);
		if (total == null) {
			total = 0L;
		}
		map.put(key, total.longValue() + value.longValue());
	}
}
