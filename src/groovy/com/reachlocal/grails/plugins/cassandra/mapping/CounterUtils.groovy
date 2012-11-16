package com.reachlocal.grails.plugins.cassandra.mapping

import com.reachlocal.grails.plugins.cassandra.utils.NestedHashMap
import com.reachlocal.grails.plugins.cassandra.utils.DateRangeParser
import java.text.SimpleDateFormat
import java.text.DateFormat
import com.reachlocal.grails.plugins.cassandra.utils.DateHelper
import com.reachlocal.grails.plugins.cassandra.utils.CounterHelper
import com.reachlocal.grails.plugins.cassandra.utils.OrmHelper
import com.reachlocal.grails.plugins.cassandra.utils.KeyHelper

/**
 * @author: Bob Florian
 */
class CounterUtils extends KeyUtils
{
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

	static toDateFormat(Integer grain, TimeZone timeZone, dateFormatArg)
	{
		if (dateFormatArg) {
			def result = new SimpleDateFormat(dateFormatArg)
			if (timeZone) {
				result.setTimeZone(timeZone);
			}
			return result
		}
		else {
			return dateFormat(grain, timeZone)
		}
	}

	static toDateFormat(Integer grain, TimeZone timeZone, DateFormat dateFormatArg)
	{
		// we ignore the grain if there is a date format
		if (timeZone) {
			def result = new SimpleDateFormat(dateFormatArg.toPattern())
			result.setTimeZone(timeZone);
			return result
		}
		else {
			return dateFormatArg
		}
	}

	static dateFormat(int grain, TimeZone timeZone)
	{
		def result = dateFormat(grain)
		if (timeZone) {
			result = new SimpleDateFormat(result.toPattern())
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

	static counterColumnName(List groupKeys, Object bean, DateFormat dateFormat = UTC_HOUR_FORMAT)
	{
		try {
			return KeyHelper.makeComposite(
					groupKeys.collect{
						KeyHelper.counterColumnKey(bean.getProperty(it), dateFormat)
					}
			)
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static List counterColumnNames(List groupKeys, Object bean, DateFormat dateFormat = UTC_HOUR_FORMAT)
	{
		try {
			def result = []
			def keys = groupKeys.collect{
				KeyHelper.counterColumnKey(bean.getProperty(it), dateFormat)
			}
			OrmHelper.expandNestedArray(keys).each {
				result << KeyHelper.makeComposite(it)
			}
			return result
		}
		catch (CassandraMappingNullIndexException e) {
			return []
		}
	}

	static getCounterColumns(clazz, filterList, multiWhereKeys, columnFilter, counterDef, start, finish, reversed, consistencyLevel, clusterName)
	{
		def cf = clazz.counterColumnFamily
		def persistence = clazz.cassandra.persistence
		def groupBy = OrmHelper.collection(counterDef.groupBy)
		def matchIndexes = columnFilter ? CounterHelper.filterMatchIndexes(columnFilter, groupBy) : null
		def cluster = clusterName ?: clazz.cassandraCluster

		clazz.cassandra.withKeyspace(clazz.keySpace, cluster) {ks ->

			def result = new NestedHashMap()
			filterList.each {filter ->

				def rowKeyValues = multiRowKeyValues(filter, multiWhereKeys)
				def groupKeys = groupBy
				def rowKey = KeyHelper.counterRowKey(counterDef.findBy, groupKeys, filter)
				def cols = persistence.getColumnRange(
						ks,
						cf,
						rowKey,
						start ? KeyHelper.counterColumnKey(start, UTC_HOUR_FORMAT) : '',
						finish ? KeyHelper.counterColumnKey(finish, UTC_HOUR_FORMAT) : '',
						reversed ?: false,
						MAX_COUNTER_COLUMNS,
						consistencyLevel)

				if (columnFilter) {
					cols.each {col ->
						def keyValues = KeyHelper.parseComposite(persistence.name(col))
						def passed = CounterHelper.filterPassed(matchIndexes, keyValues, groupBy, columnFilter)
						if (passed) {
							def resultKeyValues = CounterHelper.filterResultKeyValues(keyValues, matchIndexes)
							result.increment(CounterHelper.mergeNonDateKeys(rowKeyValues, resultKeyValues) + persistence.longValue(col))
						}
					}
				}
				else {
					cols.each {col ->
						result.increment(CounterHelper.mergeNonDateKeys(rowKeyValues, KeyHelper.parseComposite(persistence.name(col))) + persistence.longValue(col))
					}
				}
			}
			return result
		}
	}

	static multiRowKeyValues(filter, multiWhereKeys)
	{
		def result = []
		multiWhereKeys.each {key ->
			result << filter[key]
		}
		return result
	}

	static getDateCounterColumns(clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, sortResult, consistencyLevel, clusterName)
	{
		def cf = clazz.counterColumnFamily
		def persistence = clazz.cassandra.persistence
		def groupBy = OrmHelper.collection(counterDef.groupBy)
		def matchIndexes = columnFilter ? CounterHelper.filterMatchIndexes(columnFilter, groupBy) : null
		def cluster = clusterName ?: clazz.cassandraCluster

		clazz.cassandra.withKeyspace(clazz.keySpace, cluster) {ks ->

			def result = new NestedHashMap()
			rowFilterList.each {filter ->

				def rowKeyValues = multiRowKeyValues(filter, multiWhereKeys)

				if (!start) {
					def day = getEarliestDay(persistence, ks, cf, counterDef.findBy, groupBy, filter, consistencyLevel)
					if (day) {
						start = UTC_MONTH_FORMAT.parse(day)
					}
				}

				if (start) {
					def cols = doGetDateCounterColumns(
							persistence,
							ks,
							cf,
							counterDef.findBy,
							groupBy,
							filter,
							start,
							finish ?: new Date(),
							Calendar.HOUR_OF_DAY,
							consistencyLevel)

					if (columnFilter) {
						cols.each {col ->
							def keyValues = KeyHelper.parseComposite(persistence.name(col))
							def passed = CounterHelper.filterPassed(matchIndexes, keyValues, groupBy, columnFilter)
							if (passed) {
								def resultKeyValues = CounterHelper.filterResultKeyValues(keyValues, matchIndexes)
								result.increment(CounterHelper.mergeDateKeys(rowKeyValues, resultKeyValues) + persistence.longValue(col))
							}
						}
					}
					else {
						cols.each {col ->
							result.increment(CounterHelper.mergeDateKeys(rowKeyValues, KeyHelper.parseComposite(persistence.name(col))) + persistence.longValue(col))
						}
					}
				}
			}
			if (sortResult) {
				OrmHelper.sort(result);
			}
			else {
				return result;
			}
		}
	}

	static getDateCounterColumnsForTotals (clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, consistencyLevel, clusterName)
	{
		def cf = clazz.counterColumnFamily
		def persistence = clazz.cassandra.persistence
		def groupBy = OrmHelper.collection(counterDef.groupBy)
		def matchIndexes = columnFilter ? CounterHelper.filterMatchIndexes(columnFilter, groupBy) : null
		def cluster = clusterName ?: clazz.cassandraCluster

		clazz.cassandra.withKeyspace(clazz.keySpace, cluster) {ks ->
			def firstStart = start
			if (!firstStart) {
				rowFilterList.each {filter ->
					def day = getEarliestDay(persistence, ks, cf, counterDef.findBy, groupBy, filter, consistencyLevel)
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
			rowFilterList.each {filter ->
				def rowKeyValues = multiRowKeyValues(filter, multiWhereKeys)

				dateRanges.each{dateRange ->
					def cols = doGetDateCounterColumns(
							persistence,
							ks,
							cf,
							counterDef.findBy,
							groupBy,
							filter,
							dateRange.start,
							dateRange.finish,
							dateRange.grain,
							consistencyLevel)

					if (columnFilter) {
						cols.each {col ->
							def keyValues = KeyHelper.parseComposite(persistence.name(col))
							def passed = CounterHelper.filterPassed(matchIndexes, keyValues, groupBy, columnFilter)
							if (passed) {
								def resultKeyValues = CounterHelper.filterResultKeyValues(keyValues, matchIndexes)
								result.increment(CounterHelper.mergeDateKeys(rowKeyValues, resultKeyValues) + persistence.longValue(col))
							}
						}
					}
					else {
						cols.each {col ->
							result.increment(CounterHelper.mergeDateKeys(rowKeyValues, KeyHelper.parseComposite(persistence.name(col))) + persistence.longValue(col))
						}
					}
				}
			}
			return result
		}
	}

	static doGetDateCounterColumns(persistence, ks, cf, findBy, groupBy, filter, start, finish, grain, consistencyLevel)
	{
		def cols
		if (grain == Calendar.MONTH) {
			cols = getMonthRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
		}
		else if (grain == Calendar.DAY_OF_MONTH) {
			cols = getDayRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
		}
		else {
			cols = getHourRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
		}
		return cols
	}

	static private getMonthRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
	{
		def groupKeys = KeyHelper.makeGroupKeyList(groupBy, 'yyyy-MM')
		def rowKey = KeyHelper.counterRowKey(findBy, groupKeys, filter)

		columnsList(persistence.getColumnRange(
				ks,
				cf,
				rowKey,
				start ? KeyHelper.counterColumnKey(start, UTC_MONTH_FORMAT) : null,
				finish ? KeyHelper.counterColumnKey(finish, UTC_MONTH_FORMAT)+END_CHAR : null,
				false,
				MAX_COUNTER_COLUMNS,
				consistencyLevel))
	}

	static private getDayRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
	{
		def groupKeys = KeyHelper.makeGroupKeyList(groupBy, 'yyyy-MM-dd')
		def rowKey = KeyHelper.counterRowKey(findBy, groupKeys, filter)

		columnsList(persistence.getColumnRange(
				ks,
				cf,
				rowKey,
				start ? KeyHelper.counterColumnKey(start, UTC_DAY_FORMAT) : null,
				finish ? KeyHelper.counterColumnKey(finish, UTC_DAY_FORMAT)+END_CHAR : null,
				false,
				MAX_COUNTER_COLUMNS,
				consistencyLevel))
	}

	static private getHourRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
	{
		def groupKeys = groupBy //makeGroupKeyList(groupBy, "yyyy-MM-dd'T'HH")
		def rowKey = KeyHelper.counterRowKey(findBy, groupKeys, filter)

		columnsList(persistence.getColumnRange(
				ks,
				cf,
				rowKey,
				start ? KeyHelper.counterColumnKey(start, UTC_HOUR_FORMAT) : null,
				finish ? KeyHelper.counterColumnKey(finish, UTC_HOUR_FORMAT)+END_CHAR : null,
				false,
				MAX_COUNTER_COLUMNS,
				consistencyLevel))
	}


	static private getShardedDayRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
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
			def groupKeys = KeyHelper.makeGroupKeyList(groupBy, UTC_YEAR_FORMAT.format(cal.time))
			rowKeys << KeyHelper.counterRowKey(findBy, groupKeys, filter)
			cal.add(Calendar.YEAR, 1)
		}

		columnsListFromRowList(persistence.getRowsColumnRange(
				ks,
				cf,
				rowKeys,
				start ? KeyHelper.counterColumnKey(start, UTC_DAY_FORMAT) : null,
				finish ? KeyHelper.counterColumnKey(finish, UTC_DAY_FORMAT)+END_CHAR : null,
				false,
				MAX_COUNTER_COLUMNS,
				consistencyLevel), persistence)
	}

	static private getShardedHourRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
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
			def groupKeys = KeyHelper.makeGroupKeyList(groupBy, format)
			rowKeys << KeyHelper.counterRowKey(findBy, groupKeys, filter)
			cal.add(Calendar.MONTH, 1)
		}

		columnsListFromRowList(persistence.getRowsColumnRange(
				ks,
				cf,
				rowKeys,
				start ? KeyHelper.counterColumnKey(start, UTC_HOUR_FORMAT) : null,
				finish ? KeyHelper.counterColumnKey(finish, UTC_HOUR_FORMAT)+END_CHAR : null,
				false,
				MAX_COUNTER_COLUMNS,
				consistencyLevel), persistence)

	}

	static private getEarliestDay(persistence, ks, cf, findBy, groupBy, filter, consistencyLevel)
	{
		def groupKeys = KeyHelper.makeGroupKeyList(groupBy, 'yyyy-MM')
		def rowKey = KeyHelper.counterRowKey(findBy, groupKeys, filter)
		def cols = persistence.getColumnRange(ks, cf, rowKey, null, null, false, 1, consistencyLevel)
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

	static rollUpCounterDates(Map map, DateFormat fromFormat, grain, timeZone, toFormatArg)
	{
		def toFormat = toDateFormat(grain, timeZone, toFormatArg)
		def result = DateHelper.rollUpCounterDates(map, fromFormat, toFormat)
		return result
	}
}
