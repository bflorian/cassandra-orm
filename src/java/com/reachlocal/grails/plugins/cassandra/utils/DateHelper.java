/*
 * Copyright 2012 ReachLocal Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.reachlocal.grails.plugins.cassandra.utils;

import java.security.InvalidParameterException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author: Bob Florian
 */
public class DateHelper
{
	public static Map<String, Object> fillDates(Map<String, Object> source, int grain)
			throws ParseException
	{
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		if (source.size() == 0) {
			return result;
		}

		DateFormat format;
		switch (grain) {
			case Calendar.DAY_OF_MONTH:
				format = new SimpleDateFormat("yyyy-MM-dd");
				break;
			case Calendar.MONTH:
				format = new SimpleDateFormat("yyyy-MM");
				break;
			case Calendar.YEAR:
				format = new SimpleDateFormat("yyyy");
				break;
			case Calendar.HOUR_OF_DAY:
				format = new SimpleDateFormat("yyyy-MM-dd'T'HH");
				break;
			default:
				throw new InvalidParameterException("Specified time grain is not supported.  Must be HOUR_OF_DAY, DAY_OF_MONTH, MONTH, or YEAR");
		}

		Set<String> keys = source.keySet();
		Iterator<String> iter = keys.iterator();
		String minKey = "z";
		String maxKey = "";
		while (iter.hasNext()) {
			String key = iter.next();
			if (key.compareTo(minKey) < 0) {
				minKey = key;
			}
			else if (key.compareTo(maxKey) > 0) {
				maxKey = key;
			}
		}

		Date date = format.parse(minKey);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);

		Date endDate = format.parse(maxKey);
		Object firstValue = source.get(format.format(date));
		Object defaultValue = firstValue instanceof Long ? 0L : null;
		while (!date.after(endDate)) {
			String key = format.format(date);
			Object value = source.get(key);
			result.put(key, value == null ? defaultValue : value);
			cal.add(grain, 1);
			date = cal.getTime();
		}
		return result;
	}

	static public Map<String, Object> sort(Map<String, Object> source, boolean reverse)
	{
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		List<String> keys = new ArrayList<String>(source.keySet());
		Collections.sort(keys);
		if (reverse) {
			Collections.reverse(keys);
		}
		for (String key: keys) {
			result.put(key, source.get(key));
		}
		return result;
	}

	static public Map<String, Object> reverse(Map<String, Object> source)
	{
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		List<String> keys = new ArrayList<String>(source.keySet());
		Collections.reverse(keys);
		for (String key: keys) {
			result.put(key, source.get(key));
		}
		return result;
	}

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
