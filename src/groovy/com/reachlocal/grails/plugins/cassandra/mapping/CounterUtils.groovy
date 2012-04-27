package com.reachlocal.grails.plugins.cassandra.mapping

import com.reachlocal.grails.plugins.cassandra.utils.NestedHashMap
import com.reachlocal.grails.plugins.cassandra.utils.DateRangeParser
import java.text.SimpleDateFormat
import java.text.DateFormat
import com.reachlocal.grails.plugins.cassandra.utils.DateHelper

/**
 * @author: Bob Florian
 */
class CounterUtils extends KeyUtils
{
	static protected final END_CHAR = "\u00ff"
	static protected final int MAX_COUNTER_COLUMNS = Integer.MAX_VALUE
	static protected final UTC_YEAR_FORMAT = new SimpleDateFormat("yyyy")
	static protected final UTC_MONTH_FORMAT = new SimpleDateFormat("yyyy-MM")
	static protected final UTC_DAY_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
	static protected final UTC_HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH")
	static protected final UTC_HOUR_ONLY_FORMAT = new SimpleDateFormat("HH")
	static protected final UTC = TimeZone.getTimeZone("GMT") //.getDefault() //getTimeZone("GMT")

	static {
		UTC_YEAR_FORMAT.setTimeZone(UTC)
		UTC_MONTH_FORMAT.setTimeZone(UTC)
		UTC_DAY_FORMAT.setTimeZone(UTC)
		UTC_HOUR_FORMAT.setTimeZone(UTC)
		UTC_HOUR_ONLY_FORMAT.setTimeZone(UTC)
	}

	static dateFormat(int grain, TimeZone timeZone)
	{
		def result = dateFormat(grain)
		if (timeZone) {
			result = new SimpleDateFormat(result.pattern)
			result.setTimeZone(timeZone);
		}
		return result
	}

	static dateFormat(int grain)
	{
		switch(grain) {
			case Calendar.YEAR:
				return UTC_YEAR_FORMAT
			case Calendar.MONTH:
				return UTC_MONTH_FORMAT
			case Calendar.DAY_OF_MONTH:
				return UTC_DAY_FORMAT
			default:
				return UTC_HOUR_FORMAT
		}
	}

	static getCounterColumns(clazz, filterList, counterDef, params)
	{
		def options = addOptionDefaults(params, MAX_COUNTER_COLUMNS)
		def cf = clazz.counterColumnFamily
		def persistence = clazz.cassandra.persistence
		def groupBy = collection(counterDef.groupBy)

		clazz.cassandra.withKeyspace(clazz.keySpace) {ks ->

			def result = new NestedHashMap()
			filterList.each {filter ->

				//def groupKeys = params.dateFormat ? makeGroupKeyList(groupBy, params.dateFormat.toPattern()) : groupBy
				def groupKeys = groupBy
				def rowKey = counterRowKey(counterDef.whereEquals, groupKeys, filter)
				def cols = persistence.getColumnRange(
						ks,
						cf,
						rowKey,
						options.start ? counterColumnKey(options.start, UTC_HOUR_FORMAT) : null,
						options.finish ? counterColumnKey(options.finish, UTC_HOUR_FORMAT) : null,
						options.reversed,
						MAX_COUNTER_COLUMNS)

				cols.each {col ->
					result.increment(parseComposite(persistence.name(col)) + persistence.longValue(col))
				}
			}
			return result
		}
	}

	static getDateCounterColumns(clazz, filterList, counterDef, params)
	{
		def options = addOptionDefaults(params, MAX_COUNTER_COLUMNS)
		def cf = clazz.counterColumnFamily
		def persistence = clazz.cassandra.persistence
		def groupBy = collection(counterDef.groupBy)

		clazz.cassandra.withKeyspace(clazz.keySpace) {ks ->

			def result = new NestedHashMap()
			filterList.each {filter ->

				def start = options.start
				if (!start) {
					def day = getEarliestDay(persistence, ks, cf, counterDef.whereEquals, groupBy, filter)
					if (day) {
						start = UTC_MONTH_FORMAT.parse(day)
					}
				}

				if (start) {
					def cols = getDateCounterColumns(
							persistence,
							ks,
							cf,
							counterDef.whereEquals,
							groupBy,
							filter,
							start,
							options.finish ?: new Date(),
							Calendar.HOUR_OF_DAY)

					cols.each {col ->
						result.increment(parseComposite(persistence.name(col)) + persistence.longValue(col))
					}
				}
			}
			if (params.sort) {
				sort(result);
			}
			else {
				return result;
			}
		}
	}

	static getDateCounterColumnsForTotals (clazz, filterList, counterDef, start, finish)
	{
		def cf = clazz.counterColumnFamily
		def persistence = clazz.cassandra.persistence
		def groupBy = collection(counterDef.groupBy)

		clazz.cassandra.withKeyspace(clazz.keySpace) {ks ->


			def firstStart = start
			if (!firstStart) {
				filterList.each {filter ->
					def day = getEarliestDay(persistence, ks, cf, counterDef.whereEquals, groupBy, filter)
					if (day) {
						def date = UTC_MONTH_FORMAT.parse(day)
						if (firstStart == null || date.before(firstStart)) {
							firstStart = date
						}
					}
				}
			}

			def dateRangeParser = new DateRangeParser(firstStart, finish, UTC)
			def dateRanges = dateRangeParser.dateRanges

			def result = new NestedHashMap()
			filterList.each {filter ->
				dateRanges.each{dateRange ->
					def cols = getDateCounterColumns(
							persistence,
							ks,
							cf,
							counterDef.whereEquals,
							groupBy,
							filter,
							dateRange.start,
							dateRange.finish,
							dateRange.grain)

					cols.each {col ->
						result.increment(parseComposite(persistence.name(col)) + persistence.longValue(col))
					}
				}
			}
			return result
		}
	}

	static getDateCounterColumns(persistence, ks, cf, whereEquals, groupBy, filter, start, finish, grain)
	{
		def cols
		if (grain == Calendar.MONTH) {
			cols = getMonthRange(persistence, ks, cf, whereEquals, groupBy, filter, start, finish)
		}
		else if (grain == Calendar.DAY_OF_MONTH) {
			cols = getDayRange(persistence, ks, cf, whereEquals, groupBy, filter, start, finish)
		}
		else {
			cols = getHourRange(persistence, ks, cf, whereEquals, groupBy, filter, start, finish)
		}
		return cols
	}

	static private getMonthRange(persistence, ks, cf, whereEquals, groupBy, filter, start, finish)
	{
		def groupKeys = makeGroupKeyList(groupBy, 'yyyy-MM')
		def rowKey = counterRowKey(whereEquals, groupKeys, filter)

		columnsList(persistence.getColumnRange(
				ks,
				cf,
				rowKey,
				start ? counterColumnKey(start, UTC_MONTH_FORMAT) : null,
				finish ? counterColumnKey(finish, UTC_MONTH_FORMAT)+END_CHAR : null,
				false,
				MAX_COUNTER_COLUMNS))
	}

	static private getDayRange(persistence, ks, cf, whereEquals, groupBy, filter, start, finish)
	{
		def cal = Calendar.getInstance(UTC)
		cal.setTime(start)
		cal.set(Calendar.MONTH, 0)
		cal.set(Calendar.DAY_OF_MONTH, 1)
		cal.set(Calendar.HOUR_OF_DAY, 0)
		cal.set(Calendar.MINUTE, 0)
		cal.set(Calendar.SECOND, 0)
		cal.set(Calendar.MILLISECOND, 0)

		def rowKeys = []
		while (cal.time.before(finish)) {
			def groupKeys = makeGroupKeyList(groupBy, UTC_YEAR_FORMAT.format(cal.time))
			rowKeys << counterRowKey(whereEquals, groupKeys, filter)
			cal.add(Calendar.YEAR, 1)
		}

		columnsListFromRowList(persistence.getRowsColumnRange(
				ks,
				cf,
				rowKeys,
				start ? counterColumnKey(start, UTC_DAY_FORMAT) : null,
				finish ? counterColumnKey(finish, UTC_DAY_FORMAT)+END_CHAR : null,
				false,
				MAX_COUNTER_COLUMNS), persistence)
	}

	static private getHourRange(persistence, ks, cf, whereEquals, groupBy, filter, start, finish)
	{
		def cal = Calendar.getInstance(UTC)
		cal.setTime(start)
		cal.set(Calendar.DAY_OF_MONTH, 1)
		cal.set(Calendar.HOUR_OF_DAY, 0)
		cal.set(Calendar.MINUTE, 0)
		cal.set(Calendar.SECOND, 0)
		cal.set(Calendar.MILLISECOND, 0)

		def rowKeys = []
		while (cal.time.before(finish)) {
			def format = UTC_MONTH_FORMAT.format(cal.time)
			def groupKeys = makeGroupKeyList(groupBy, format)
			rowKeys << counterRowKey(whereEquals, groupKeys, filter)
			cal.add(Calendar.MONTH, 1)
		}

		columnsListFromRowList(persistence.getRowsColumnRange(
				ks,
				cf,
				rowKeys,
				start ? counterColumnKey(start, UTC_HOUR_FORMAT) : null,
				finish ? counterColumnKey(finish, UTC_HOUR_FORMAT)+END_CHAR : null,
				false,
				MAX_COUNTER_COLUMNS), persistence)

	}

	static private getEarliestDay(persistence, ks, cf, whereEquals, groupBy, filter)
	{
		def groupKeys = makeGroupKeyList(groupBy, 'yyyy-MM')
		def rowKey = counterRowKey(whereEquals, groupKeys, filter)
		def cols = persistence.getColumnRange(ks, cf, rowKey, null, null, false, 1)
		cols?.size() ? persistence.name(persistence.getColumnByIndex(cols, 0)) : null
	}


	static columnsList(columnsIterator)
	{
		// TODO - performance!
		def cols = []
		columnsIterator.each {
			cols << it
		}
		cols
	}

	static columnsListFromRowList(rowList, persistence)
	{
		// TODO - performance!
		def cols = []
		rowList.each {row ->
			persistence.getColumns(row).each {
				cols << it
			}
		}
		cols
	}

	static rollUpCounterDates(Map map, DateFormat fromFormat, params)
	{
		int grain = params.grain
		def toFormat = dateFormat(params.grain, params.timeZone)
		def result = DateHelper.rollUpCounterDates(map, fromFormat, toFormat)
		if (params.fill) {
			// TODO - implement
			if (!params.sort) {
				result = sort(result)
			}

			def rollUp = [:]
			def cal = null
			result.each {key, value ->
				def date = fromFormat.parse(key)
				if (cal == null) {
					cal = Calendar.getInstance(UTC)
					cal.setTime(date)
				}
				else {
					while (date.before(cal.time)) {
						def key2 = fromFormat.format(cal.time)
						rollUp[key2] = null
						cal.add(grain, 1)
					}
				}
				rollUp[key] = value
				cal.add(grain, 1)
			}
			result = rollUp
		}
		return result
	}
}
