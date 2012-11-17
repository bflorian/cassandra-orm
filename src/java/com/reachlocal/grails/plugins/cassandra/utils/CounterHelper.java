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

import com.reachlocal.grails.plugins.cassandra.mapping.CassandraMappingNullIndexException;
import com.reachlocal.grails.plugins.cassandra.mapping.PersistenceProvider;
import groovy.lang.GroovyObject;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author: Bob Florian
 */
public class CounterHelper
{
	public static final int MAX_COUNTER_COLUMNS = Integer.MAX_VALUE ;
	public static final SimpleDateFormat UTC_YEAR_FORMAT = new SimpleDateFormat("yyyy");
	public static final SimpleDateFormat UTC_MONTH_FORMAT = new SimpleDateFormat("yyyy-MM");
	public static final SimpleDateFormat UTC_DAY_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	public static final SimpleDateFormat UTC_HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH");
	public static final SimpleDateFormat UTC_HOUR_ONLY_FORMAT = new SimpleDateFormat("HH");
	public static final TimeZone UTC = TimeZone.getTimeZone("GMT"); //.getDefault() //getTimeZone("GMT")
	static final boolean WRITE_ALTERNATES = false;

	static {
		UTC_YEAR_FORMAT.setTimeZone(UTC);
		UTC_MONTH_FORMAT.setTimeZone(UTC);
		UTC_DAY_FORMAT.setTimeZone(UTC);
		UTC_HOUR_FORMAT.setTimeZone(UTC);
		UTC_HOUR_ONLY_FORMAT.setTimeZone(UTC);
	}

	public static void updateCounterColumns(PersistenceProvider persistence, Object counterColumnFamily, Map counterDef, Object m, GroovyObject oldObj, GroovyObject thisObj) throws IOException
	{
		List<String> whereKeys = OrmHelper.stringList(counterDef.get("findBy"));
		List<String> groupKeys = OrmHelper.stringList(counterDef.get("groupBy"));
		//Object counterColumnFamily = clazz.counterColumnFamily
		//def cassandra = clazz.cassandra

		if ((Boolean)counterDef.get("isDateIndex")) {
			if (oldObj != null) {
				List<String> oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_HOUR_FORMAT);
				List<String> gKeys = groupKeys;
				String ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj);
				if (oldColNames != null && ocrk != null) {

					/** ALTERNATE ONE **/
					// all hours row (currently not used)
					for(String oldColName: oldColNames) {
						persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);
					}

					// all days row
					oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_DAY_FORMAT);
					for(String oldColName: oldColNames) {
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, "yyyy-MM-dd");
						ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);
					}

					/** COMMON TO ALL**/
					// all months row
					oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_MONTH_FORMAT);
					for(String oldColName: oldColNames) {
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, "yyyy-MM");
						ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);
					}

					if (WRITE_ALTERNATES) {
						/** ALTERNATE TWO **/
						// specific year/hour row (currently not used)
						oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_HOUR_FORMAT);
						for(String oldColName: oldColNames) {
							gKeys = KeyHelper.makeGroupKeyList(groupKeys, UTC_YEAR_FORMAT.format(oldObj.getProperty(groupKeys.get(0)))+"THH");
							ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj);
							persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);
						}

						/** ALTERNATE THREE (current) **/
						// specific month/hour row
						oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_HOUR_FORMAT);
						for(String oldColName: oldColNames) {
							gKeys = KeyHelper.makeGroupKeyList(groupKeys, UTC_MONTH_FORMAT.format(oldObj.getProperty(groupKeys.get(0))));
							ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj);
							persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);
						}

						/** COMMON TO TWO AND THREE **/
						// specific year/day row
						oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_DAY_FORMAT);
						for(String oldColName: oldColNames) {
							gKeys = KeyHelper.makeGroupKeyList(groupKeys, UTC_YEAR_FORMAT.format(oldObj.getProperty(groupKeys.get(0))));
							ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj);
							persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);
						}
					}
				}
			}

			if (thisObj != null) {
				List<String> colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_HOUR_FORMAT);
				List<String> gKeys = groupKeys;
				String crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj);
				if (colNames != null && crk != null) {


					/** ALTERNATE ONE **/
					// all hours row (currently not used)
					for (String colName: colNames) {
						persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);
					}
					// all days row
					colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_DAY_FORMAT);
					for (String colName: colNames) {
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, "yyyy-MM-dd");
						crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);
					}

					/** COMMON TO ALL**/
					// all month row
					colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_MONTH_FORMAT);
					for (String colName: colNames) {
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, "yyyy-MM");
						crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);
					}

					if (WRITE_ALTERNATES) {
						/** ALTERNATE TWO **/
						// specific year/hour row (currently not used)
						colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_HOUR_FORMAT);
						for (String colName: colNames) {
							gKeys = KeyHelper.makeGroupKeyList(groupKeys, UTC_YEAR_FORMAT.format(thisObj.getProperty(groupKeys.get(0)))+"THH");
							crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj);
							persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);
						}

						/** ALTERNATE THREE (current) **/
						// specific month/hour row
						colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_HOUR_FORMAT);
						for (String colName: colNames) {
							gKeys = KeyHelper.makeGroupKeyList(groupKeys, UTC_MONTH_FORMAT.format(thisObj.getProperty(groupKeys.get(0))));
							crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj);
							persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);
						}

						/** COMMON TO TWO AND THREE **/
						// specific year/day row
						colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_DAY_FORMAT);
						for (String colName: colNames) {
							gKeys = KeyHelper.makeGroupKeyList(groupKeys, UTC_YEAR_FORMAT.format(thisObj.getProperty(groupKeys.get(0))));
							crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj);
							persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);
						}
					}
				}
			}
		}
		else {
			if (oldObj != null) {
				List<String> oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_HOUR_FORMAT);
				for(String oldColName: oldColNames) {
					String ocrk = KeyHelper.counterRowKey(whereKeys, groupKeys, oldObj);
					if (oldColName != null && ocrk != null) {
						persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);
					}
				}
			}
			if (thisObj != null) {
				List<String> colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_HOUR_FORMAT);
				for (String colName: colNames) {
					String crk = KeyHelper.counterRowKey(whereKeys, groupKeys, thisObj);
					if (colName != null && crk != null) {
						persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);
					}
				}
			}
		}
	}

	public static DateFormat toDateFormat(Integer grain, TimeZone timeZone, String dateFormatArg)
	{
		if (dateFormatArg != null) {
			DateFormat result = new SimpleDateFormat(dateFormatArg);
			if (timeZone != null) {
				result.setTimeZone(timeZone);
			}
			return result;
		}
		else {
			return dateFormat(grain, timeZone);
		}
	}

	public static DateFormat toDateFormat(Integer grain, TimeZone timeZone, SimpleDateFormat dateFormatArg)
	{
		// we ignore the grain if there is a date format
		if (timeZone != null) {
			DateFormat result = new SimpleDateFormat(dateFormatArg.toPattern());
			result.setTimeZone(timeZone);
			return result;
		}
		else {
			return dateFormatArg;
		}
	}

	public static DateFormat dateFormat(int grain, TimeZone timeZone)
	{
		SimpleDateFormat result = dateFormat(grain);
		if (timeZone != null) {
			result = new SimpleDateFormat(result.toPattern());
			result.setTimeZone(timeZone);
		}
		return result;
	}

	public static SimpleDateFormat dateFormat(int grain)
	{
		switch(grain) {
			case Calendar.YEAR:
				return UTC_YEAR_FORMAT;
			case Calendar.MONTH:
				return UTC_MONTH_FORMAT;
			case Calendar.DAY_OF_MONTH:
				return UTC_DAY_FORMAT;
			default:
				return UTC_HOUR_FORMAT;
		}
	}

	public static String counterColumnName(List<String> groupKeys, GroovyObject bean)  throws IOException
	{
		return counterColumnName(groupKeys, bean, UTC_HOUR_FORMAT);
	}

	public static String counterColumnName(List<String> groupKeys, GroovyObject bean, DateFormat dateFormat) throws IOException
	{
		try {
			List<String> items = new ArrayList<String>(groupKeys.size());
			for (String it: groupKeys) {
				items.add(KeyHelper.counterColumnKey(bean.getProperty(it), dateFormat));
			}
			return KeyHelper.makeComposite(items);
		}
		catch (CassandraMappingNullIndexException e) {
			return null;
		}
	}

	public static List<String> counterColumnNames(List<String> groupKeys, GroovyObject bean) throws IOException
	{
		return counterColumnNames(groupKeys, bean, UTC_HOUR_FORMAT);
	}

	public static List<String> counterColumnNames(List<String> groupKeys, GroovyObject bean, DateFormat dateFormat) throws IOException
	{
		try {
			List<Object> keys = new ArrayList<Object>();
			for (String it: groupKeys) {
				keys.add(KeyHelper.counterColumnKey(bean.getProperty(it), dateFormat));
			}

			List<List<String>> items = OrmHelper.expandNestedArray(keys);
			List<String> result = new ArrayList<String>();
			for (List<String> it: items) {
				result.add(KeyHelper.makeComposite(it));
			}
			return result;
		}
		catch (CassandraMappingNullIndexException e) {
			return new ArrayList<String>();
		}
	}
	/*
	 public static Map getCounterColumns(
			 //Class clazz,
			 Object cf,
			 Object keySpace,
			 Object cassandra,
			 Object persistence,
			 String cluster,

			 List filterList,
			 List multiWhereKeys,
			 Map<String, List<String>> columnFilter,
			 Map<String, Object> counterDef,
			 Object start,
			 Object finish,
			 Boolean reversed,
			 Object consistencyLevel,
			 String clusterName)
	 {
		 //def cf = clazz.counterColumnFamily
		 //def persistence = clazz.cassandra.persistence
		 List<String> groupBy = (List<String>)OrmHelper.stringList(counterDef.get("groupBy"));
		 List<Integer> matchIndexes = columnFilter != null ? CounterHelper.filterMatchIndexes(columnFilter, groupBy) : null;
		 //def cluster = clusterName ?: clazz.cassandraCluster

		 cassandra.withKeyspace(keySpace, cluster) {ks ->

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

	 public static multiRowKeyValues(filter, multiWhereKeys)
	 {
		 def result = []
		 multiWhereKeys.each {key ->
			 result << filter[key]
	 }
		 return result
	 }

	 public static getDateCounterColumns(clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, sortResult, consistencyLevel, clusterName)
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

	 public static getDateCounterColumnsForTotals (clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, consistencyLevel, clusterName)
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

	 public static doGetDateCounterColumns(persistence, ks, cf, findBy, groupBy, filter, start, finish, grain, consistencyLevel)
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

	 public static private getMonthRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
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

	 public static private getDayRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
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

	 public static private getHourRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
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


	 public static private getShardedDayRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
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

	 public static private getShardedHourRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
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

	 public static private getEarliestDay(persistence, ks, cf, findBy, groupBy, filter, consistencyLevel)
	 {
		 def groupKeys = KeyHelper.makeGroupKeyList(groupBy, 'yyyy-MM')
		 def rowKey = KeyHelper.counterRowKey(findBy, groupKeys, filter)
		 def cols = persistence.getColumnRange(ks, cf, rowKey, null, null, false, 1, consistencyLevel)
		 cols?.size() ? persistence.name(persistence.getColumnByIndex(cols, 0)) : null
	 }

	 public static columnsList(columnsIterator)
	 {
		 // TODO - performance!
		 def cols = []
		 columnsIterator.each {
		 cols << it
	 }
		 cols
	 }

	 public static columnsListFromRowList(rowList, persistence)
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

	 public static rollUpCounterDates(Map map, DateFormat fromFormat, grain, timeZone, toFormatArg)
	 {
		 def toFormat = toDateFormat(grain, timeZone, toFormatArg)
		 def result = DateHelper.rollUpCounterDates(map, fromFormat, toFormat)
		 return result
	 }
 */
	// ORIGINAL COUNTER HELPER
	static public List<Integer> filterMatchIndexes(Map<String, List<String>> columnFilter, List<String> groupBy)
	{
		Set<String> matchKeys = columnFilter.keySet();
		List<Integer> matchIndexes = new ArrayList<Integer>();
		Integer index = 0;
		for (String key: groupBy) {
			if (matchKeys.contains(key)) {
				matchIndexes.add(index);
			}
			index++;
		}
		return matchIndexes;
	}

	static public boolean filterPassed(List<Integer>matchIndexes, List<String>keyValues, List<String>groupBy, Map<String, List<String>>columnFilter)
	{
		boolean passed = true;
		for (Integer index: matchIndexes) {
			String kv = keyValues.get(index);
			String k = groupBy.get(index);
			List<String> fv = columnFilter.get(k);
			if (!fv.contains(kv)) {
				passed = false;
				break;
			}
		}
		return passed;
	}

	static public List<String> filterResultKeyValues(List<String>keyValues, List<Integer>matchIndexes)
	{
		List<String> resultKeyValues = new ArrayList<String>();
		Integer index = 0;
		for (String kv: keyValues) {
			if (!matchIndexes.contains(index)) {
				resultKeyValues.add(kv);
			}
			index++;
		}
		return resultKeyValues;
	}

	static public List<String> mergeDateKeys(List<String> rowKeys, List<String> columnKeys)
	{
		if (rowKeys.size() > 0) {
			List<String> result = new ArrayList<String>(rowKeys.size() + columnKeys.size());
			if (columnKeys.size() > 1) {
				result.add(columnKeys.get(0));
				result.addAll(rowKeys);
				result.addAll(columnKeys.subList(1, columnKeys.size()));
			}
			else {
				result.addAll(rowKeys);
				result.addAll(columnKeys);
			}
			return result;
		}
		else {
			return columnKeys;
		}
	}

	static public List<String> mergeNonDateKeys(List<String> rowKeys, List<String> columnKeys)
	{
		if (rowKeys.size() > 0) {
			List<String> result = new ArrayList<String>(rowKeys.size() + columnKeys.size());
			result.addAll(rowKeys);
			result.addAll(columnKeys);
			return result;
		}
		else {
			return columnKeys;
		}
	}
}
