package com.reachlocal.grails.plugins.cassandra.utils;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author: Bob Florian
 */
public class TimeZoneAdjuster
{
	private TimeZone dataTimeZone;
	private TimeZone clientTimeZone;
	private int grain;
	private Calendar calendar;
	private long offsetHours;
	private Set<String> rowKeys = new LinkedHashSet<String>();
	private List<String> columnNames = new ArrayList<String>();
	private int offsetDirection;
	private DateFormat keyFormat;
	private DateFormat dayFormat;

	public TimeZoneAdjuster(Date start, Date finish, TimeZone dataTimeZone, TimeZone clientTimeZone, int grain)
	{
		this.dataTimeZone = dataTimeZone;
		this.clientTimeZone = clientTimeZone;
		this.grain = grain;

		this.calendar = Calendar.getInstance(dataTimeZone);
		this.calendar.setTime(start);

		this.keyFormat = grain == Calendar.DAY_OF_MONTH ? new SimpleDateFormat("yyyy-MM-dd") : new SimpleDateFormat("yyyy-MM");
		this.keyFormat.setTimeZone(dataTimeZone);

		this.dayFormat = new SimpleDateFormat("yyyy-MM-dd");
		this.dayFormat.setTimeZone(dataTimeZone);

		this.offsetHours = Math.round((clientTimeZone.getRawOffset() - dataTimeZone.getRawOffset())/3600000L);

		// to make sure we have enough hours considering daylight savings time
		if (this.offsetHours > 0) {
			this.offsetHours += 1;
			this.offsetDirection = 1;
		}
		else {
			this.offsetDirection = -1;
		}

		if (offsetHours != 0) {
			// row keys
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			df.setTimeZone(dataTimeZone);
			if (this.offsetHours < 0) {
				while (!calendar.getTime().after(finish)) {
					calendar.add(Calendar.DAY_OF_MONTH, -1);
					rowKeys.add(df.format(calendar.getTime()));

					calendar.add(Calendar.DAY_OF_MONTH, 1);
					rowKeys.add(df.format(calendar.getTime()));

					calendar.add(grain, 1);
				}
			}
			else {
				while (!calendar.getTime().after(finish)) {
					rowKeys.add(df.format(calendar.getTime()));
					calendar.add(Calendar.DAY_OF_MONTH, 1);

					rowKeys.add(df.format(calendar.getTime()));
					calendar.add(Calendar.DAY_OF_MONTH, -1);

					calendar.add(grain, 1);
				}
			}

			// columnNames
			int increment = this.offsetHours < 0 ? 1 : -1;
			for (long i=this.offsetHours; i != 0; i += increment) {
				long hour = i < 0 ? i+24 : i;
				columnNames.add(NF.format(hour));
			}
		}
	}

	public boolean getHasOffset()
	{
		return offsetHours != 0;
	}

	public Collection getRowKeys()
	{
		return rowKeys;
	}

	public Collection getColumnNames()
	{
		return columnNames;
	}

	public Map mergeCounts(Map<String, Long> primary, Map<String, Map<String, Object>> hour) throws ParseException
	{
		Map<String, Object> result = new LinkedHashMap<String, Object>();

		for (Map.Entry<String, Long> entry: primary.entrySet()) {
			String addKey;
			String subtractKey;
			String key = entry.getKey();
			Object value = entry.getValue();
			Calendar cal = calendarFromKey(key);

			if (offsetDirection < 0) {
				subtractKey = dayFormat.format(cal.getTime());
				cal.add(Calendar.DAY_OF_MONTH, -1);
				addKey = dayFormat.format(cal.getTime());
			}
			else {
				addKey = dayFormat.format(cal.getTime());
				cal.add(Calendar.DAY_OF_MONTH, 1);
				subtractKey = dayFormat.format(cal.getTime());
			}

			Map<String, Object> addMap = hour.get(addKey);
			Map<String, Object> subtractMap = hour.get(subtractKey);
			for(String col: columnNameSlice(cal.getTime()))
			{
				if (addMap != null) {
					value = addValues(value, addMap.get(col));
				}
				if (subtractMap != null) {
					value = subtractValues(value, subtractMap.get(col));
				}
			}
			result.put(key, value);
		}
		return result;
	}

	private List<String> columnNameSlice(Date date)
	{
		List<String> result;
		long time = date.getTime();
		int offset = Math.round((clientTimeZone.getOffset(time) - dataTimeZone.getOffset(time))/3600000);
		if (offset < 0) {
			result = columnNames.subList(columnNames.size() + offset, columnNames.size());
		}
		else {
			result = columnNames.subList(0, offset);
		}
		return result;
	}

	private Object addValues(Object value1, Object value2)
	{
		if (value2 == null) {
			return value1;
		}
		else if (value1 instanceof Number) {
			return addValues((Number) value1, (Number) value2);
		}
		else {
			return addValues((Map<String, Object>) value1, (Map<String, Object>) value2);
		}
	}

	private Object addValues(Number value1, Number value2)
	{
		return value1.longValue() + value2.longValue();
	}

	private Object addValues(Map<String, Object> value1, Map<String, Object> value2)
	{
		for (Map.Entry<String, Object> entry: value2.entrySet()) {
			DateHelper.mergeCounterMap(value1, entry.getKey(), entry.getValue());
		}
		return value1;
	}

	private Object subtractValues(Object value1, Object value2)
	{
		if (value2 == null) {
			return value1;
		}
		else if (value1 instanceof Number) {
			return subtractValues((Number) value1, (Number) value2);
		}
		else {
			return subtractValues((Map) value1, (Map) value2);
		}

	}

	private Object subtractValues(Number value1, Number value2)
	{
		return value1.longValue() - value2.longValue();
	}

	private Object subtractValues(Map value1, Map value2)
	{
		return value1;
	}

	private Calendar calendarFromKey(String key) throws ParseException
	{
		Calendar cal = Calendar.getInstance(dataTimeZone);
		cal.setTime(keyFormat.parse(key));
		return cal;
	}

	static private DecimalFormat NF = new DecimalFormat("00");
}
