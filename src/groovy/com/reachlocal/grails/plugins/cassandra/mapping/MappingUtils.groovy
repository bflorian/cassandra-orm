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

package com.reachlocal.grails.plugins.cassandra.mapping

import org.apache.commons.beanutils.PropertyUtils
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.text.DateFormat
import com.reachlocal.grails.plugins.cassandra.utils.NestedHashMap
import com.reachlocal.grails.plugins.cassandra.utils.DateRangeParser
import com.reachlocal.grails.plugins.cassandra.utils.TimeZoneAdjuster

/**
 * @author: Bob Florian
 */
class MappingUtils extends BaseUtils
{
	static protected final int MAX_ROWS = 2000
	static protected final int MAX_COUNTER_COLUMNS = Integer.MAX_VALUE
	static protected final INDEX_OPTIONS = ["start","finish","keys"]
	static protected final OBJECT_OPTIONS = ["column","columns", "rawColumn", "rawColumns"]
	static protected final ALL_OPTIONS = INDEX_OPTIONS + OBJECT_OPTIONS
	static protected final CLASS_NAME_KEY = '_class_name_'
	static protected final GLOBAL_TRANSIENTS = ["class","id","cassandra","indexColumnFamily","columnFamily","metaClass","keySpace"] as Set
	static protected final KEY_SUFFIX = "_key"
	static protected final DIRTY_SUFFIX = "_dirty"
	static protected final END_CHAR = "\u00ff"

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


	static Map addOptionDefaults(options, defaultCount)
	{
		Map result = [
				reversed : options.reversed ? true : false,
				max : options.max ?: defaultCount,
		]
		ALL_OPTIONS.each {
			if (options[it]) {
				result[it] = options[it]
			}
		}
		return result
	}

	static String makeComposite(list)
	{
		list.join("__")
	}

	static List parseComposite(String value)
	{
		value.split("__")
	}

	static joinRowKey(fromClass, toClass, propName, object)
	{
		def fromClassName = fromClass.name.split("\\.")[-1]
		"${fromClassName}?${propName}=${URLEncoder.encode(object.id)}".toString()
	}

	static primaryKeyIndexRowKey()
	{
		"this"
	}

	static counterRowKey(List whereKeys, List groupKeys, Map map)
	{
		def key = objectIndexRowKey(whereKeys, map)
		key ? "${key}#${makeComposite(groupKeys)}".toString() : null
	}

	static counterRowKey(List whereKeys, List groupKeys, Object bean)
	{
		def key = objectIndexRowKey(whereKeys, bean)
		key ? "${key}#${makeComposite(groupKeys)}".toString() : null
	}

	static counterColumnName(List groupKeys, Object bean, DateFormat dateFormat = UTC_HOUR_FORMAT)
	{
		try {
			return makeComposite(
					groupKeys.collect{
						counterColumnKey(bean.getProperty(it), dateFormat)
					}
			)
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static makeGroupKeyList(keys, dateSuffix)
	{
		def result = keys.clone()
		result[0] = "${keys[0]}[${dateSuffix}]"
		return result
	}

	static updateCounterColumns(Class clazz, Map counterDef, m, oldObj, thisObj)
	{
		def whereKeys = counterDef.whereEquals
		def groupKeys = collection(counterDef.groupBy)
		def counterColumnFamily = clazz.counterColumnFamily
		def cassandra = clazz.cassandra

		if (counterDef.isDateIndex) {
			if (oldObj) {
				def oldColName = counterColumnName(groupKeys, oldObj, UTC_HOUR_FORMAT)
				def gKeys = groupKeys
				def ocrk = counterRowKey(whereKeys, gKeys, oldObj)
				if (oldColName && ocrk) {

					// all hours row
					cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)

					// all days row
					oldColName = counterColumnName(groupKeys, oldObj, UTC_DAY_FORMAT)
					gKeys = makeGroupKeyList(groupKeys, "yyyy-MM-dd")
					ocrk = counterRowKey(whereKeys, gKeys, oldObj)
					cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)

					// specific month/hour row
					oldColName = counterColumnName(groupKeys, oldObj, UTC_HOUR_FORMAT)
					gKeys = makeGroupKeyList(groupKeys, UTC_MONTH_FORMAT.format(oldObj.getProperty(groupKeys[0])))
					ocrk = counterRowKey(whereKeys, gKeys, oldObj)
					cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)
/*
					// specific day/hour row
					oldColName = counterColumnName(groupKeys, oldObj, UTC_HOUR_ONLY_FORMAT)
					gKeys = makeGroupKeyList(groupKeys, UTC_DAY_FORMAT.format(oldObj.getProperty(groupKeys[0])))
					ocrk = counterRowKey(whereKeys, gKeys, oldObj)
					cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)
*/
					// all months row
					oldColName = counterColumnName(groupKeys, oldObj, UTC_MONTH_FORMAT)
					gKeys = makeGroupKeyList(groupKeys, "yyyy-MM")
					ocrk = counterRowKey(whereKeys, gKeys, oldObj)
					cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)

					// specific year/day row
					oldColName = counterColumnName(groupKeys, oldObj, UTC_DAY_FORMAT)
					gKeys = makeGroupKeyList(groupKeys, UTC_YEAR_FORMAT.format(oldObj.getProperty(groupKeys[0])))
					ocrk = counterRowKey(whereKeys, gKeys, oldObj)
					cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)
				}
			}

			def colName = counterColumnName(groupKeys, thisObj, UTC_HOUR_FORMAT)
			def gKeys = groupKeys
			def crk = counterRowKey(whereKeys, gKeys, thisObj)
			if (colName && crk) {

				// all hours row (to be deprecated)
				cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)

				// all days row
				colName = counterColumnName(groupKeys, thisObj, UTC_DAY_FORMAT)
				gKeys = makeGroupKeyList(groupKeys, "yyyy-MM-dd")
				crk = counterRowKey(whereKeys, gKeys, thisObj)
				cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)

				// specific month/hour row
				colName = counterColumnName(groupKeys, thisObj, UTC_HOUR_FORMAT)
				gKeys = makeGroupKeyList(groupKeys, UTC_MONTH_FORMAT.format(thisObj.getProperty(groupKeys[0])))
				crk = counterRowKey(whereKeys, gKeys, thisObj)
				cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)
/*
				// specific day/hour row
				colName = counterColumnName(groupKeys, thisObj, UTC_HOUR_ONLY_FORMAT)
				gKeys = makeGroupKeyList(groupKeys, UTC_DAY_FORMAT.format(thisObj.getProperty(groupKeys[0])))
				crk = counterRowKey(whereKeys, gKeys, thisObj)
				cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)
*/
				// all month row
				colName = counterColumnName(groupKeys, thisObj, UTC_MONTH_FORMAT)
				gKeys = makeGroupKeyList(groupKeys, "yyyy-MM")
				crk = counterRowKey(whereKeys, gKeys, thisObj)
				cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)

				// specific year/day row
				colName = counterColumnName(groupKeys, thisObj, UTC_DAY_FORMAT)
				gKeys = makeGroupKeyList(groupKeys, UTC_YEAR_FORMAT.format(thisObj.getProperty(groupKeys[0])))
				crk = counterRowKey(whereKeys, gKeys, thisObj)
				cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)

			}
		}
		else {
			if (oldObj) {
				def oldColName = counterColumnName(groupKeys, oldObj, UTC_HOUR_FORMAT)
				def ocrk = counterRowKey(whereKeys, groupKeys, oldObj)
				if (oldColName && ocrk) {
					cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)
				}
			}
			def colName = counterColumnName(groupKeys, thisObj, UTC_HOUR_FORMAT)
			def crk = counterRowKey(whereKeys, groupKeys, thisObj)
			if (colName && crk) {
				cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)
			}
		}
	}

	static objectIndexRowKey(String propName, Map map)
	{
		try {
			return indexRowKey(propName, map[propName])
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static objectIndexRowKey(String propName, Object bean)
	{
		try {
			return indexRowKey(propName, bean.getProperty(propName))
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static objectIndexRowKey(List propNames, Map map)
	{
		try {
			return indexRowKey(propNames.collect{[it, map[it]]})
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static objectIndexRowKey(List propNames, Object bean)
	{
		try {
			return indexRowKey(propNames.collect{[it, bean.getProperty(it)]})
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static indexRowKey(String name, value)
	{
		try {
			"this?${name}=${URLEncoder.encode(primaryRowKey(value))}".toString()
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static indexRowKey(List pairs)
	{
		try {
			def sep = "?"
			def sb = new StringBuilder("this")
			pairs.each {
				sb << sep
				sb << it[0]
				sb << '='
				sb << URLEncoder.encode(primaryRowKey(it[1]))
				sep = "&"
			}
			return sb.toString()
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static void saveJoinRow(persistence, m, objClass, object, itemClass, item, propName)
	{
		def columnFamily = itemClass.indexColumnFamily
		def rowKey = joinRowKey(objClass, itemClass, propName, object)
		persistence.putColumn(m, columnFamily,rowKey, item.id, '')
	}

	static void removeJoinRow(persistence, m, objClass, object, itemClass, item, propName)
	{
		def columnFamily = itemClass.indexColumnFamily
		def rowKey = joinRowKey(objClass, itemClass, propName, object)
		persistence.deleteColumn(m, columnFamily, rowKey, item.id)
	}

	static getFromHasMany(thisObj, propName, options=[:], clazz=LinkedHashSet)
	{
		getByMappedObject(thisObj, propName, thisObj.hasMany[propName], options, clazz)
	}

	static countFromHasMany(thisObj, propName, options=[:])
	{
		countByMappedObject(thisObj, propName, thisObj.hasMany[propName], options)
	}

	static boolean isMappedClass(clazz) {
		return clazz.metaClass.hasMetaProperty("cassandraMapping")
	}

	static boolean isMappedObject(object) {
		return object ? object.class.metaClass.hasMetaProperty("cassandraMapping") : false
	}

	static counterColumnKey(List items, DateFormat dateFormat) throws CassandraMappingNullIndexException
	{
		if (items) {
			makeComposite(items.collect{counterColumnKey(it, dateFormat)})
		}
		else {
			throw new CassandraMappingNullIndexException("Counter column keys cannot bean null or blank")
		}
	}

	static counterColumnKey(Date date, DateFormat dateFormat)
	{
		if (date) {
			dateFormat.format(date)
		}
		else {
			throw new CassandraMappingNullIndexException("Counter column keys cannot bean null or blank")
		}
	}

	static counterColumnKey(String str, DateFormat dateFormat)
	{
		if (str) {
			return str
		}
		else {
			throw new CassandraMappingNullIndexException("Counter column keys cannot bean null or blank")
		}
	}

	static counterColumnKey(obj, DateFormat dateFormat) throws CassandraMappingNullIndexException
	{
		primaryRowKey(obj)
	}

	static primaryRowKey(List objs)
	{
		makeComposite(objs.collect{primaryRowKey(it)})
	}

	static primaryRowKey(String id)
	{
		id
	}

	static primaryRowKey(Number id)
	{
		id.toString()
	}

	static primaryRowKey(Boolean id)
	{
		id.toString()
	}

	static primaryRowKey(UUID id)
	{
		dataProperty(id)
	}

	static primaryRowKey(obj) throws CassandraMappingNullIndexException
	{
		if (obj == null) {
			throw new CassandraMappingNullIndexException("Primary keys and indexed properties cannot have null values")
		}
		else if (isMappedObject(obj)) {
			return obj.id
		}
		else {
			return dataProperty(obj)
		}
	}

	static safeGetStaticProperty(clazz, name)
	{
		if (clazz.metaClass.hasMetaProperty(name)) {
			return clazz.metaClass.getMetaProperty(name).getProperty()
		}
		return null
	}

	static safeGetProperty(data, name, clazz, defaultValue)
	{
		def value
		try {
			value = clazz.isInstance(data.getProperty(name)) ? data.getProperty(name) : defaultValue
		}
		catch (MissingPropertyException e) {
			value = defaultValue
		}
		catch (MissingMethodException e) {
			value = defaultValue
		}
		return value
	}

	static safeSetProperty(object, name, value)
	{
		if (PropertyUtils.getPropertyType(object, name)) {
			PropertyUtils.setProperty(object, name, value)
		}
	}

	// @params [eventType:'Radar', subType:['Review','Mention']]
	// @return [[eventType:'Radar', subType:'Review'],[eventType:'Radar', subType:'Mention']]
	static expandFilters(params)
	{
		def result = []
		def len = 1
		def lengths = []
		params.each {name, value ->
			if (value instanceof List || value.class.isArray()) {
				len = len * value.size()
			}
			lengths << len
		}
		for (i in 1..len) {
			result << [:]
		}

		params.eachWithIndex {name, value, pindex ->
			if (value instanceof List || value.class.isArray()) {
				result.eachWithIndex {item, index ->
				   def i = (index * lengths[pindex] / len).toInteger() % value.size()
				   item[name] = value[i]
				}
			}
			else {
				result.each {item ->
					item[name] = value
				}
			}
		}
		return result
	}

	static findIndex(indexList, filterList)
	{
		def filter1 = filterList[0]
		for (index in indexList) {
			def params = index instanceof List ? index.clone() : [index]
			def all = false
			if (params.size() == filter1.size()) {
				all = true
				for (item in filter1) {
					if (!params.contains(item.key)) {
						all = false
						break
					}
				}
				if (all) {
					return index
				}
			}
		}
		return null
	}

	static findCounter(counterList, filterList, groupByList)
	{
		def filter1 = filterList[0]
		for (counter in counterList) {
			def index = counter.whereEquals ?: [] //TODO - right?
			def groupBy = collection(counter.groupBy)
			def params = index instanceof List ? index.clone() : [index]
			if (params.size() == filter1.size()) {
				def all = true
				for (item in filter1) {
					if (!params.contains(item.key)) {
						all = false
						break
					}
				}
				if (all) {
					for (item in groupByList) {
						if (!groupBy.contains(item)) {
							all = false
							break
						}
					}
				}
				if (all) {
					return counter
				}
			}
		}
		return null
	}

	static mergeKeys(List<List> keys, Integer max)
	{
		if (keys.size() == 1) {
			def last = Math.min(keys[0].size(), max)-1
			def result =  keys[0]
			return result ? result[0..last] : []
		}
		else {
			def treeSet = new TreeSet()
			keys.each {list ->
				treeSet.addAll(list)
			}

			def result = []
			def iter = treeSet.iterator()
			for (int i=0; i < max && iter.hasNext(); i++) {
				result << iter.next()
			}
			return result
		}
	}

	static queryByExplicitIndex(clazz, filterList, index, opts)
	{
		def options = addOptionDefaults(opts, MAX_ROWS)
		def indexCf = clazz.indexColumnFamily
		def persistence = clazz.cassandra.persistence
		clazz.cassandra.withKeyspace(clazz.keySpace) {ks ->
			def columns = []
			filterList.each {filter ->
				def rowKey = objectIndexRowKey(index, filter)
				def cols = persistence.getColumnRange(
						ks,
						indexCf,
						rowKey,
						options.start,
						options.finish,
						options.reversed,
						options.max)

				columns << cols.collect{persistence.name(it)}
			}
			def keys = mergeKeys(columns, options.max)

			def result
			def names = columnNames(options)
			if (names) {
				def rows = persistence.getRowsColumnSlice(ks, clazz.columnFamily, keys, names)
				result = clazz.cassandra.mapping.makeResult(keys, rows, options)
			}
			else {
				def rows = persistence.getRows(ks, clazz.columnFamily, keys)
				result = clazz.cassandra.mapping.makeResult(keys, rows, options)
			}
			return result
		}
	}

	static countByExplicitIndex(clazz, filterList, index, opts)
	{
		def options = addOptionDefaults(opts, MAX_ROWS)
		def indexCf = clazz.indexColumnFamily
		def persistence = clazz.cassandra.persistence
		clazz.cassandra.withKeyspace(clazz.keySpace) {ks ->
			def total = 0
			filterList.each {filter ->
				def rowKey = objectIndexRowKey(index, filter)
				def count = persistence.countColumnRange(
						ks,
						indexCf,
						rowKey,
						options.start,
						options.finish)

				total += count
			}
			return total
		}
	}


	static getCounterColumns(clazz, filterList, counterDef, params)
	{
		def options = addOptionDefaults(params, MAX_COUNTER_COLUMNS)
		def cf = clazz.counterColumnFamily
		def persistence = clazz.cassandra.persistence
		def groupBy = collection(counterDef.groupBy)
		def colDateFormat = UTC_HOUR_FORMAT
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
						options.start ? counterColumnKey(options.start, colDateFormat) : null,
						options.finish ? counterColumnKey(options.finish, colDateFormat) : null,
						options.reversed,
						MAX_COUNTER_COLUMNS)

				cols.each {col ->
					result.increment(parseComposite(persistence.name(col)) + persistence.longValue(col))
				}
			}
			return result
		}
	}

	/// NEW COUNTERS

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
				MAX_COUNTER_COLUMNS))
	}

	static private getHourRange(persistence, ks, cf, whereEquals, groupBy, filter, start, finish)
	{
		def cal = Calendar.getInstance(UTC)
		cal.setTime(start)

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
				MAX_COUNTER_COLUMNS))

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

	static columnsListFromRowList(rowList)
	{
		// TODO - performance!
		def cols = []
		rowList.each {row ->
			row.columns.each {
				cols << it
			}
		}
		cols
	}

	static getDateCounterColumns_new (clazz, filterList, counterDef, start, finish, timeZone, grain)
	{
		def cf = clazz.counterColumnFamily
		def persistence = clazz.cassandra.persistence
		def groupBy = collection(counterDef.groupBy)

		clazz.cassandra.withKeyspace(clazz.keySpace) {ks ->

			def result = new NestedHashMap()
			filterList.each {filter ->
				def cols = getDateCounterColumns(persistence, ks, cf, counterDef.whereEquals, groupBy, filter, start, finish, grain)
				cols.each {col ->
					result.increment(parseComposite(persistence.name(col)) + persistence.longValue(col))
				}

				// get time zone offset
				def timeZoneAdjuster = new TimeZoneAdjuster(start, finish, UTC, timeZone, grain)
				if (timeZoneAdjuster.hasOffset) {
					// TODO this is not right
					def rowKeys = timeZoneAdjuster.rowKeys.collect{counterRowKey(counterDef.whereEquals, makeGroupKeyList(groupBy, it), filter)}
					def colNames = timeZoneAdjuster.columnNames
					def dayRows = persistence.getRowsColumnSlice(ks, cf, rowKeys, colNames)
					def dayRowMap = new NestedHashMap()
					dayRows.each {row ->
						def key = persistence.getRowKey(row)
						def p1 = key.indexOf('[')
						def p2 = key.indexOf(']')
						def day = key[p1+1..p2-1]
						row.columns.each {col ->
							dayRowMap.increment([day] + parseComposite(persistence.name(col)) + persistence.longValue(col))
						}
					}
					result = timeZoneAdjuster.mergeCounts(result, dayRowMap)
				}
			}
			return result
		}
	}

	static getDateCounterColumnTotal (clazz, filterList, counterDef, start, finish)
	{
		def cf = clazz.counterColumnFamily
		def persistence = clazz.cassandra.persistence
		def groupBy = collection(counterDef.groupBy)
		def dateRangeParser = new DateRangeParser(start, finish, UTC)
		def dateRanges = dateRangeParser.dateRanges
		clazz.cassandra.withKeyspace(clazz.keySpace) {ks ->

			def result = new NestedHashMap()
			filterList.each {filter ->
				dateRanges.each{dateRange ->
					def cols = getDateCounterColumns(persistence, ks, cf, counterDef.whereEquals, groupBy, filter, dateRange.start, dateRange.finish, dateRange.grain)
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

	/// END NEW COUNTERS


	static getByMappedObject(thisObj, propName, itemClass, opts=[:], listClass=LinkedHashSet)
	{
		def result = []
		def options = addOptionDefaults(opts, MAX_ROWS)
		def itemColumnFamily = itemClass.columnFamily
		def persistence = thisObj.cassandra.persistence
		def belongsToPropName = itemClass.belongsToPropName(thisObj.getClass())

		thisObj.cassandra.withKeyspace(thisObj.keySpace) {ks ->
			def indexCF = itemClass.indexColumnFamily
			def indexKey = joinRowKey(thisObj.class, itemClass, propName, thisObj)

			def keys = persistence.getColumnRange(ks, indexCF, indexKey, options.start, options.finish, options.reversed, options.max)
					.collect{persistence.name(it)}

			def names = columnNames(options)
			if (names) {
				def rows = persistence.getRowsColumnSlice(ks, itemColumnFamily, keys, names)
				result = thisObj.cassandra.mapping.makeResult(keys, rows, options, listClass)
			}
			else {
				def rows = persistence.getRows(ks, itemColumnFamily, keys)
				result = thisObj.cassandra.mapping.makeResult(keys, rows, options, listClass)
				if (belongsToPropName) {
					result.each {
						it.setProperty(belongsToPropName, thisObj)
					}
				}
			}
		}
		return result
	}

	static countByMappedObject(thisObj, propName, itemClass, opts=[:])
	{
		def result = []
		def options = addOptionDefaults(opts, MAX_ROWS)
		def persistence = thisObj.cassandra.persistence
		thisObj.cassandra.withKeyspace(thisObj.keySpace) {ks ->
			def indexCF = itemClass.indexColumnFamily
			def indexKey = joinRowKey(thisObj.class, itemClass, propName, thisObj)

			result = persistence.countColumnRange(ks, indexCF, indexKey, options.start, options.finish)
		}
		return result
	}

	static columnNames(Map options)
	{
		def result = []
		if (options.columns) {
			result = options.columns
		}
		else if (options.column) {
			result = [options.column]
		}
		else if (options.rawColumns) {
			result = options.rawColumns
		}
		else if (options.rawColumn) {
			result = [options.rawColumn]
		}
		return result
	}

	static dataProperty(Date value)
	{
		return value.time.toString()
	}

	static dataProperty(Integer value)
	{
		return value.toString()
	}

	static dataProperty(Long value)
	{
		return value.toString()
	}

	static dataProperty(Double value)
	{
		return value.toString()
	}

	static dataProperty(BigDecimal value)
	{
		return value.toString()
	}

	static dataProperty(BigInteger value)
	{
		return value.toString()
	}

	static dataProperty(UUID value)
	{
		return value.toString()
	}

	static dataProperty(Collection value)
	{
		return value.encodeAsJSON()
	}

	static dataProperty(Map value)
	{
		return value.encodeAsJSON()
	}

	static dataProperty(Boolean value)
	{
		return value
	}

	static dataProperty(String value)
	{
		return value
	}

	static dataProperty(byte[] value)
	{
		return value

	}

	static dataProperty(ByteBuffer value)
	{
		return value
	}

	static dataProperty(Class value)
	{
		return null
	}

	def dataProperty(value)
	{
		if (value?.getClass()?.isEnum()) {
			value.toString()
		}
		else {
			null
		}
	}
}
