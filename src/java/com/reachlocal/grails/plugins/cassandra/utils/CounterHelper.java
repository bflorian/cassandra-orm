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
	static final boolean WRITE_ALTERNATES = false;
	static final boolean ROLL_UP_COUNTS = false;

	public static void updateAllCounterColumns(PersistenceProvider persistence, Object counterColumnFamily, List<Map> counterDefs, Object m, GroovyObject oldObj, GroovyObject thisObj) throws IOException
	{
		if (counterDefs != null) {
			for (Map counterDef: counterDefs) {
				updateCounterColumns(persistence, counterColumnFamily, counterDef, m, oldObj, thisObj);
			}
		}
	}

	public static void updateCounterColumns(PersistenceProvider persistence, Object counterColumnFamily, Map counterDef, Object m, GroovyObject oldObj, GroovyObject thisObj) throws IOException
	{
		List<String> whereKeys = OrmHelper.stringList(counterDef.get("findBy"));
		List<String> groupKeys = OrmHelper.stringList(counterDef.get("groupBy"));
		//Object counterColumnFamily = clazz.counterColumnFamily
		//def cassandra = clazz.cassandra

		if ((Boolean)counterDef.get("isDateIndex")) {
			if (oldObj != null) {
				String oldColName = CounterHelper.counterColumnName(groupKeys, oldObj, UtcDate.hourFormatter());
				List gKeys = groupKeys;
				String ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj);
				if (oldColName != null && ocrk != null) {

					/** ALTERNATE ONE **/
					// all hours row
					persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);

					if (ROLL_UP_COUNTS) {
						// all days row
						oldColName = CounterHelper.counterColumnName(groupKeys, oldObj, UtcDate.dayFormatter());
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, "yyyy-MM-dd");
						ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);


						/** COMMON TO ALL**/
						// all months row
						oldColName = CounterHelper.counterColumnName(groupKeys, oldObj, UtcDate.monthFormatter());
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, "yyyy-MM");
						ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);
					}

					if (WRITE_ALTERNATES) {
						/** ALTERNATE TWO **/
						// specific year/hour row (currently not used)
						oldColName = CounterHelper.counterColumnName(groupKeys, oldObj, UtcDate.hourFormatter());
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, UtcDate.yearFormatter().format(oldObj.getProperty(groupKeys.get(0)))+"THH");
						ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);

						/** ALTERNATE THREE (current) **/
						// specific month/hour row
						oldColName = CounterHelper.counterColumnName(groupKeys, oldObj, UtcDate.hourFormatter());
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, UtcDate.monthFormatter().format(oldObj.getProperty(groupKeys.get(0))));
						ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);

						/** COMMON TO TWO AND THREE **/
						// specific year/day row
						oldColName = CounterHelper.counterColumnName(groupKeys, oldObj, UtcDate.dayFormatter());
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, UtcDate.yearFormatter().format(oldObj.getProperty(groupKeys.get(0))));
						ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);
					}
				}
			}

			if (thisObj != null) {
				String colName = CounterHelper.counterColumnName(groupKeys, thisObj, UtcDate.hourFormatter());
				List<String> gKeys = groupKeys;
				String crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj);
				if (colName != null && crk != null) {


					/** ALTERNATE ONE **/
					// all hours row
					persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);

					if (ROLL_UP_COUNTS) {
						// all days row
						colName = CounterHelper.counterColumnName(groupKeys, thisObj, UtcDate.dayFormatter());
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, "yyyy-MM-dd");
						crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);

						/** COMMON TO ALL**/
						// all month row
						colName = CounterHelper.counterColumnName(groupKeys, thisObj, UtcDate.monthFormatter());
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, "yyyy-MM");
						crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);
					}

					if (WRITE_ALTERNATES) {
						/** ALTERNATE TWO **/
						// specific year/hour row (currently not used)
						colName = CounterHelper.counterColumnName(groupKeys, thisObj, UtcDate.hourFormatter());
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, UtcDate.yearFormatter().format(thisObj.getProperty(groupKeys.get(0)))+"THH");
						crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);

						/** ALTERNATE THREE (current) **/
						// specific month/hour row
						colName = CounterHelper.counterColumnName(groupKeys, thisObj, UtcDate.hourFormatter());
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, UtcDate.monthFormatter().format(thisObj.getProperty(groupKeys.get(0))));
						crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);

						/** COMMON TO TWO AND THREE **/
						// specific year/day row
						colName = CounterHelper.counterColumnName(groupKeys, thisObj, UtcDate.dayFormatter());
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, UtcDate.yearFormatter().format(thisObj.getProperty(groupKeys.get(0))));
						crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj);
						persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);
					}
				}
			}
		}
		else {
			if (oldObj != null) {
				String oldColName = CounterHelper.counterColumnName(groupKeys, oldObj, UtcDate.hourFormatter());
				String ocrk = KeyHelper.counterRowKey(whereKeys, groupKeys, oldObj);
				if (oldColName != null && ocrk != null) {
					persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1L);
				}
			}
			if (thisObj != null) {
				String colName = CounterHelper.counterColumnName(groupKeys, thisObj, UtcDate.hourFormatter());
				String crk = KeyHelper.counterRowKey(whereKeys, groupKeys, thisObj);
				if (colName != null && crk != null) {
					persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName, 1L);
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
				return UtcDate.yearFormatter();
			case Calendar.MONTH:
				return UtcDate.monthFormatter();
			case Calendar.DAY_OF_MONTH:
				return UtcDate.dayFormatter();
			default:
				return UtcDate.hourFormatter();
		}
	}

	public static String counterColumnName(List<String> groupKeys, GroovyObject bean, DateFormat dateFormat) throws IOException
	{
		try {
			List items = new ArrayList(groupKeys.size());
			for (String it: groupKeys) {
				items.add(KeyHelper.counterColumnKey(bean.getProperty(it), dateFormat));
			}
			return KeyHelper.makeComposite(items).toString();
		}
		catch (CassandraMappingNullIndexException e) {
			return null;
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
					 start ? KeyHelper.counterColumnKey(start, UtcDate.hourFormatter()) : '',
					 finish ? KeyHelper.counterColumnKey(finish, UtcDate.hourFormatter()) : '',
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
					 start = UtcDate.monthFormatter().parse(day)
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
					 def date = UtcDate.monthFormatter().parse(day)
					 if (firstStart == null || date.before(firstStart)) {
						 firstStart = date
					 }
				 }
			 }
		 }

		 def dateRangeParser = new DateRangeParser(firstStart, finish, TimeZone.getTimeZone("GMT"))
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
				 start ? KeyHelper.counterColumnKey(start, UtcDate.monthFormatter()) : null,
				 finish ? KeyHelper.counterColumnKey(finish, UtcDate.monthFormatter())+END_CHAR : null,
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
				 start ? KeyHelper.counterColumnKey(start, UtcDate.dayFormatter()) : null,
				 finish ? KeyHelper.counterColumnKey(finish, UtcDate.dayFormatter())+END_CHAR : null,
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
				 start ? KeyHelper.counterColumnKey(start, UtcDate.hourFormatter()) : null,
				 finish ? KeyHelper.counterColumnKey(finish, UtcDate.hourFormatter())+END_CHAR : null,
				 false,
				 MAX_COUNTER_COLUMNS,
				 consistencyLevel))
	 }


	 public static private getShardedDayRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
	 {
		 def cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
		 cal.setTime(start)
		 cal.set(Calendar.MONTH, 0)
		 cal.set(Calendar.DAY_OF_MONTH, 1)
		 cal.set(Calendar.HOUR_OF_DAY, 0)
		 cal.set(Calendar.MINUTE, 0)
		 cal.set(Calendar.SECOND, 0)
		 cal.set(Calendar.MILLISECOND, 0)

		 def rowKeys = []
		 while (cal.time.before(finish)) {
			 def groupKeys = KeyHelper.makeGroupKeyList(groupBy, UtcDate.yearFormatter().format(cal.time))
			 rowKeys << KeyHelper.counterRowKey(findBy, groupKeys, filter)
			 cal.add(Calendar.YEAR, 1)
		 }

		 columnsListFromRowList(persistence.getRowsColumnRange(
				 ks,
				 cf,
				 rowKeys,
				 start ? KeyHelper.counterColumnKey(start, UtcDate.dayFormatter()) : null,
				 finish ? KeyHelper.counterColumnKey(finish, UtcDate.dayFormatter())+END_CHAR : null,
				 false,
				 MAX_COUNTER_COLUMNS,
				 consistencyLevel), persistence)
	 }

	 public static private getShardedHourRange(persistence, ks, cf, findBy, groupBy, filter, start, finish, consistencyLevel)
	 {
		 def cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
		 cal.setTime(start)
		 cal.set(Calendar.DAY_OF_MONTH, 1)
		 cal.set(Calendar.HOUR_OF_DAY, 0)
		 cal.set(Calendar.MINUTE, 0)
		 cal.set(Calendar.SECOND, 0)
		 cal.set(Calendar.MILLISECOND, 0)

		 def rowKeys = []
		 while (cal.time.before(finish)) {
			 def format = UtcDate.monthFormatter().format(cal.time)
			 def groupKeys = KeyHelper.makeGroupKeyList(groupBy, format)
			 rowKeys << KeyHelper.counterRowKey(findBy, groupKeys, filter)
			 cal.add(Calendar.MONTH, 1)
		 }

		 columnsListFromRowList(persistence.getRowsColumnRange(
				 ks,
				 cf,
				 rowKeys,
				 start ? KeyHelper.counterColumnKey(start, UtcDate.hourFormatter()) : null,
				 finish ? KeyHelper.counterColumnKey(finish, UtcDate.hourFormatter())+END_CHAR : null,
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

	static public List columnsList(Iterable columnsIterator)
	{
		List cols = new ArrayList();
		for (Object it: columnsIterator) {
			cols.add(it);
		}
		return cols;
	}

	static public List columnsListFromRowList(Iterable rowList, PersistenceProvider persistence)
	{
		// TODO - performance!
		List cols = new ArrayList();
		for (Object row: rowList) {
			for (Object it: persistence.getColumns(row)) {
				cols.add(it);
			}
		}
		return cols;
	}
}
