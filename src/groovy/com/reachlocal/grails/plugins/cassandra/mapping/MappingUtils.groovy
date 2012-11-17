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

import com.reachlocal.grails.plugins.cassandra.utils.DateHelper
import com.reachlocal.grails.plugins.cassandra.utils.OrmHelper
import com.reachlocal.grails.plugins.cassandra.utils.KeyHelper
import com.reachlocal.grails.plugins.cassandra.utils.CounterHelper

/**
 * @author: Bob Florian
 */
class MappingUtils extends CounterUtils
{
	static final Boolean WRITE_ALTERNATES = false

    static boolean isMappedClass(clazz) {
        return clazz.metaClass.hasMetaProperty("cassandraMapping")
    }

    static boolean isMappedProperty(Class clazz, String name) {
        try {
            def propClass = clazz.getDeclaredField(name)?.type // TODO - support superclasses?
            return propClass ? isMappedClass(propClass) : false
        }
        catch (NoSuchFieldException e) {
            return false
        }
    }

    static safeGetStaticProperty(clazz, name)
    {
        if (clazz.metaClass.hasMetaProperty(name)) {
            return clazz.metaClass.getMetaProperty(name).getProperty()
        }
        return null
    }

    static getCounters(
			Class clazz,
			List counterDefs,
			Map whereFilter,
			byPropNames,
			start, finish, sort, reversed, grain, timeZone, toFormatArg, fill, consistencyLevel, clusterName)
	{
		def nochunk=false
		def cluster = clusterName ?: clazz.cassandraCluster

		// TODO - combine with rowFilterList
		def multiWhereKeys = []
		whereFilter.each {key, values ->
			if (OrmHelper.collection(values).size() > 1) {
				multiWhereKeys << key
			}
		}

		def counterDef = findCounter(counterDefs, whereFilter, OrmHelper.collection(byPropNames), multiWhereKeys)
		if (counterDef == null) {
			throw new CassandraMappingException("Counter definition not found, where: ${whereFilter}, groupBy: ${byPropNames}")
		}
		def counterGroupByNames = counterDef.groupBy
		def rowFilterList = expandFilters(counterRowFilter(whereFilter, counterDef))
		def columnFilter = counterColumnFilter(whereFilter, counterDef)

		def value
		def groupByPropNames = byPropNames ? OrmHelper.collection(byPropNames) : []
		if (groupByPropNames) {
			def i = 0
			def indexes = []

			if (counterDef.isDateIndex) {
				if (groupByPropNames.contains(counterDef.dateIndexProp)) {
					groupByPropNames.remove(counterDef.dateIndexProp)
					// TODO - should we bother doing the sort here, doesn't currently work
					value = getDateCounterColumns(clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, false, consistencyLevel, cluster)
					indexes << i
				}
				else if (nochunk) {
					// TODO - should we bother doing the sort here, doesn't currently work
					value = getDateCounterColumns (clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, false, consistencyLevel, cluster)
				}
				else {
					value = getDateCounterColumnsForTotals (clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, consistencyLevel, cluster)
				}
				i = 1
			}
			else {
				value = getCounterColumns(clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, reversed, consistencyLevel, cluster)
			}

			groupByPropNames.clone().each {gbpName ->
				multiWhereKeys?.eachWithIndex {key, index ->
					if (key == gbpName) {
						groupByPropNames.remove(key)
						indexes << index + i
					}
				}
			}

			def j = multiWhereKeys.size()
			groupByPropNames.each {gbpName ->
				def idx = 0
				for (cName in counterGroupByNames) {
					if (cName == gbpName) {
						indexes << idx + j
						break
					}
					if (!columnFilter.containsKey(cName)) {
						idx++
					}
				}
			}
 			value = value.groupBy(indexes)
		}
		else {
			if (counterDef.isDateIndex)  {
				value = getDateCounterColumns(clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, sort, consistencyLevel, cluster)
			}
			else {
				value = getCounterColumns(clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, reversed, consistencyLevel, cluster)
			}
		}
		if (grain || timeZone || toFormatArg) {
			value = rollUpCounterDates(value, UTC_HOUR_FORMAT, grain ?: Calendar.HOUR_OF_DAY , timeZone, toFormatArg)
		}
		if (fill) {
			value = DateHelper.fillDates(value, grain ?: Calendar.HOUR_OF_DAY)
		}
		if (sort) {
			value = DateHelper.sort(value, reversed ? true : false)
		}
		else if (reversed) {
			value = DateHelper.reverse(value)
		}
		return value
	}

	static getCountersForTotals(
			Class clazz,
			List counterDefs,
			Map whereFilter,
			byPropNames, start, finish, consistencyLevel, clusterName)
	{
		def cluster = clusterName ?: clazz.cassandraCluster
		def counterDef = findCounter(counterDefs, whereFilter, OrmHelper.collection(byPropNames))
		def rowFilterList = expandFilters(counterRowFilter(whereFilter, counterDef))
		def columnFilter = counterColumnFilter(whereFilter, counterDef)
		def cols = getDateCounterColumnsForTotals (clazz, rowFilterList, [], columnFilter, counterDef, start, finish, consistencyLevel, cluster)
		//return mapTotal(cols)
		return cols.total()
	}

	static updateCounterColumns_New(Class clazz, Map counterDef, Object m, GroovyObject oldObj, GroovyObject thisObj)
	{
		def counterColumnFamily = clazz.counterColumnFamily
		def cassandra = clazz.cassandra
		CounterHelper.updateCounterColumns(cassandra.persistence, counterColumnFamily, counterDef, m, oldObj, thisObj);
	}

	static updateCounterColumns(Class clazz, Map counterDef, m, oldObj, thisObj)
	{
		def whereKeys = counterDef.findBy
		def groupKeys = OrmHelper.collection(counterDef.groupBy)
		def counterColumnFamily = clazz.counterColumnFamily
		def cassandra = clazz.cassandra

		if (counterDef.isDateIndex) {
			if (oldObj) {
				def oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_HOUR_FORMAT)
				def gKeys = groupKeys
				def ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj)
				if (oldColNames && ocrk) {

					/** ALTERNATE ONE **/
					// all hours row (currently not used)
					oldColNames.each {oldColName ->
						cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)
					}

					// all days row
					oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_DAY_FORMAT)
					oldColNames.each {oldColName ->
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, "yyyy-MM-dd")
						ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj)
						cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)
					}

					/** COMMON TO ALL**/
					// all months row
					oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_MONTH_FORMAT)
					oldColNames.each {oldColName ->
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, "yyyy-MM")
						ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj)
						cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)
					}

					if (WRITE_ALTERNATES) {
						/** ALTERNATE TWO **/
						// specific year/hour row (currently not used)
						oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_HOUR_FORMAT)
						oldColNames.each {oldColName ->
							gKeys = KeyHelper.makeGroupKeyList(groupKeys, UTC_YEAR_FORMAT.format(oldObj.getProperty(groupKeys[0]))+"THH")
							ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj)
							cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)
						}

						/** ALTERNATE THREE (current) **/
						// specific month/hour row
						oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_HOUR_FORMAT)
						oldColNames.each {oldColName ->
							gKeys = KeyHelper.makeGroupKeyList(groupKeys, UTC_MONTH_FORMAT.format(oldObj.getProperty(groupKeys[0])))
							ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj)
							cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)
						}

						/** COMMON TO TWO AND THREE **/
						// specific year/day row
						oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_DAY_FORMAT)
						oldColNames.each {oldColName ->
							gKeys = KeyHelper.makeGroupKeyList(groupKeys, UTC_YEAR_FORMAT.format(oldObj.getProperty(groupKeys[0])))
							ocrk = KeyHelper.counterRowKey(whereKeys, gKeys, oldObj)
							cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)
						}
					}
				}
			}

			if (thisObj) {
				def colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_HOUR_FORMAT)
				def gKeys = groupKeys
				def crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj)
				if (colNames && crk) {


					/** ALTERNATE ONE **/
					// all hours row (currently not used)
					colNames.each {colName ->
						cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)
					}
					// all days row
					colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_DAY_FORMAT)
					colNames.each {colName ->
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, "yyyy-MM-dd")
						crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj)
						cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)
					}

					/** COMMON TO ALL**/
					// all month row
					colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_MONTH_FORMAT)
					colNames.each {colName ->
						gKeys = KeyHelper.makeGroupKeyList(groupKeys, "yyyy-MM")
						crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj)
						cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)
					}

					if (WRITE_ALTERNATES) {
						/** ALTERNATE TWO **/
						// specific year/hour row (currently not used)
						colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_HOUR_FORMAT)
						colNames.each {colName ->
							gKeys = KeyHelper.makeGroupKeyList(groupKeys, UTC_YEAR_FORMAT.format(thisObj.getProperty(groupKeys[0]))+"THH")
							crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj)
							cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)
						}

						/** ALTERNATE THREE (current) **/
						// specific month/hour row
						colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_HOUR_FORMAT)
						colNames.each {colName ->
							gKeys = KeyHelper.makeGroupKeyList(groupKeys, UTC_MONTH_FORMAT.format(thisObj.getProperty(groupKeys[0])))
							crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj)
							cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)
						}

						/** COMMON TO TWO AND THREE **/
						// specific year/day row
						colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_DAY_FORMAT)
						colNames.each {colName ->
							gKeys = KeyHelper.makeGroupKeyList(groupKeys, UTC_YEAR_FORMAT.format(thisObj.getProperty(groupKeys[0])))
							crk = KeyHelper.counterRowKey(whereKeys, gKeys, thisObj)
							cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)
						}
					}
				}
			}
		}
		else {
			if (oldObj) {
				def oldColNames = CounterHelper.counterColumnNames(groupKeys, oldObj, UTC_HOUR_FORMAT)
				oldColNames.each {oldColName ->
					def ocrk = KeyHelper.counterRowKey(whereKeys, groupKeys, oldObj)
					if (oldColName && ocrk) {
						cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)
					}
				}
			}
			if (thisObj) {
				def colNames = CounterHelper.counterColumnNames(groupKeys, thisObj, UTC_HOUR_FORMAT)
				colNames.each {colName ->
					def crk = KeyHelper.counterRowKey(whereKeys, groupKeys, thisObj)
					if (colName && crk) {
						cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)
					}
				}
			}
		}
	}

	static void saveJoinRow(persistence, m, objClass, object, itemClass, item, propName)
	{
		// the row itself
		def columnFamily = itemClass.indexColumnFamily
		def rowKey = KeyHelper.joinRowKey(objClass, itemClass, propName, object)
		persistence.putColumn(m, columnFamily, rowKey, item.id, '')

		// the back pointer
		def backIndexRowKey = KeyHelper.manyBackIndexRowKey(item.id)
		persistence.putColumn(m, columnFamily, backIndexRowKey, rowKey, '')
	}

	static void removeJoinRow(persistence, m, objClass, object, itemClass, item, propName)
	{
		// the row itself
		def columnFamily = itemClass.indexColumnFamily
		def rowKey = KeyHelper.joinRowKey(objClass, itemClass, propName, object)
		persistence.deleteColumn(m, columnFamily, rowKey, item.id)

		// the back pointer
		def backIndexRowKey = KeyHelper.manyBackIndexRowKey(item.id)
		persistence.deleteColumn(m, columnFamily, backIndexRowKey, rowKey)
	}

	static void removeAllJoins(persistence, m, objClass, object, itemClass, items, propName)
	{
		// the row itself
		def columnFamily = itemClass.indexColumnFamily
		def rowKey = KeyHelper.joinRowKey(objClass, itemClass, propName, object)
		persistence.deleteRow(m, columnFamily, rowKey)

		// the back pointer
		items.each{item ->
			def backIndexRowKey = KeyHelper.manyBackIndexRowKey(item.id)
			persistence.deleteColumn(m, columnFamily, backIndexRowKey, rowKey)
		}
	}

	static getFromHasMany(thisObj, propName, options=[:], clazz=LinkedHashSet)
	{
		getByMappedObject(thisObj, propName, thisObj.hasMany[propName], options, clazz)
	}

	static countFromHasMany(thisObj, propName, options=[:])
	{
		countByMappedObject(thisObj, propName, thisObj.hasMany[propName], options)
	}

	// @params [eventType:'Radar', subType:['Review','Mention']]
	// @return [[eventType:'Radar', subType:'Review'],[eventType:'Radar', subType:'Mention']]
	static expandFilters(params)
	{
		def result = []
		def len = 1
		def lengths = []
		params.each {name, value ->
			if (value instanceof List || value.getClass().isArray()) {
				len = len * value.size()
			}
			lengths << len
		}
		for (i in 1..len) {
			result << [:]
		}

		params.eachWithIndex {name, value, pindex ->
			if (value instanceof List || value.getClass().isArray()) {
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

	static findCounterOld(counterList, filterList, byList)
	{
		def filter1 = filterList[0]
		for (counter in counterList) {
			def findBy = counter.findBy ?: [] //TODO - right?
			def groupBy = OrmHelper.collection(counter.groupBy)
			def params = findBy instanceof List ? findBy : [findBy]
			if (params.size() == filter1.size()) {
				def all = true
				for (item in filter1) {
					if (!params.contains(item.key)) {
						all = false
						break
					}
				}

				// found an exact findBy match, look for matches of all groupBy options
				if (all) {
					for (item in byList) {
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

	static counterRowFilter(whereFilter, counterDef)
	{
		def findByNames = counterDef.findBy
		def result = [:]
		whereFilter.each {name, values ->
			if (findByNames?.contains(name)) {
				result[name] = values
			}
		}
		return result
	}

	static counterColumnFilter(whereFilter, counterDef)
	{
		def findByNames = counterDef.findBy
		def result = [:]
		whereFilter.each {name, values ->
			if (!findByNames?.contains(name)) {
				result[name] = OrmHelper.collection(values).collect{it.toString()}
			}
		}
		return result
	}

	static findCounter(counterList, queryFilter, queryGroupByList, multiWhereKeys=[])
	{
		def exactMatches = []
		def bestMatches = []
		def queryFilterPropNames = queryFilter.collect{it.key}
		for (counter in counterList) {
			def counterFindBy = counter.findBy ?: [] //TODO - right?
			def counterFilterPropNames = counterFindBy instanceof List ? counterFindBy : [counterFindBy]
			def counterGroupPropNames = OrmHelper.collection(counter.groupBy)
			def queryFilterPropsRemaining = new LinkedHashSet()
			queryFilterPropsRemaining.addAll(queryFilterPropNames)
			def found = true

			// check each filter prop in this counter to see if its in the query
			for (name in counterFilterPropNames) {
				if (queryFilterPropsRemaining.contains(name)) {
					queryFilterPropsRemaining.remove(name)
				}
				else {
					found = false
					break
				}
			}

			// found a counter with a findBy that matches, check that all query groupBy are in the counter
			if (found) {
				for (item in queryGroupByList) {
					if (!counterGroupPropNames.contains(item) && !multiWhereKeys.contains(item)) {
						found = false
						break
					}
				}
			}

			// now check for exact or partial filter match
			if (found) {
				if (queryFilterPropsRemaining.size() == 0) {
					exactMatches << counter
				}
				else {
					for (name in queryFilterPropsRemaining) {
						if (!counterGroupPropNames.contains(name)) {
							found = false
							break
						}
					}
					if (found) {
						if (bestMatches) {
							//if(queryGroupByList && counterGroupPropNames.size() < collection(bestMatch.groupBy).size()) {
								bestMatches << counter
							//}
						}
						else {
							bestMatches << counter
						}
					}
				}
			}
		}
		def matches = exactMatches ?: bestMatches
		def bestTotalMatch = matches ? matches[0] : null
		if (matches.size() > 1) {
			matches[1..-1].each {counter ->
				if (counter.groupBy.size() < bestTotalMatch.groupBy.size()) {
					bestTotalMatch = counter
				}
			}
		}
		return bestTotalMatch
	}

	static mergeKeys(List<List> keys, Integer max, Boolean reverse=false)
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
			def iter = reverse ? treeSet.descendingIterator() : treeSet.iterator()
			for (int i=0; i < max && iter.hasNext(); i++) {
				result << iter.next()
			}
			return result
		}
	}

	static queryByExplicitIndex(clazz, filterList, index, opts)
	{
		def options = OrmHelper.addOptionDefaults(opts, MAX_ROWS)
		def start = KeyHelper.nullablePrimaryRowKey(options.startAfter ?: options.start)
		def max = options.startAfter ? options.max + 1 : options.max
		def indexCf = clazz.indexColumnFamily
		def persistence = clazz.cassandra.persistence
		def cluster = opts.cluster ?: clazz.cassandraCluster
		clazz.cassandra.withKeyspace(clazz.keySpace, cluster) {ks ->
			def columns = []
			filterList.each {filter ->
				def rowKey = KeyHelper.objectIndexRowKey(index, filter)
				def cols = persistence.getColumnRange(
						ks,
						indexCf,
						rowKey,
						start,
						KeyHelper.nullablePrimaryRowKey(options.finish),
						options.reversed,
						max,
						opts.consistencyLevel)

				columns << cols.collect{persistence.name(it)}
			}
			def keys = mergeKeys(columns, max, options.reversed)
			OrmHelper.checkForDefaultRowsInsufficient(opts.max, keys.size())

			def result
			def names = columnNames(options)
			def resultClass = options.startAfter ? LinkedList : LinkedHashSet
			if (names) {
				def rows = persistence.getRowsColumnSlice(ks, clazz.columnFamily, keys, names, opts.consistencyLevel)
				result = clazz.cassandra.mapping.makeResult(keys, rows, options, resultClass)
			}
			else {
				def rows = persistence.getRows(ks, clazz.columnFamily, keys, opts.consistencyLevel)
				result = clazz.cassandra.mapping.makeResult(keys, rows, options, resultClass)
			}
			return options.startAfter && result ? result[1..-1] : result
		}
	}

	static countByExplicitIndex(clazz, filterList, index, opts)
	{
		def options = OrmHelper.addOptionDefaults(opts, MAX_ROWS)
		def start = KeyHelper.nullablePrimaryRowKey(options.startAfter ?: options.start)
		def indexCf = clazz.indexColumnFamily
		def persistence = clazz.cassandra.persistence
		def cluster = opts.cluster ?: clazz.cassandraCluster
		clazz.cassandra.withKeyspace(clazz.keySpace, cluster) {ks ->
			def total = 0
			filterList.each {filter ->
				def rowKey = KeyHelper.objectIndexRowKey(index, filter)
				def count = persistence.countColumnRange(
						ks,
						indexCf,
						rowKey,
						start,
						KeyHelper.nullablePrimaryRowKey(options.finish),
						opts.consistencyLevel)

				total += count
			}
			return options.startAfter && total ? total - 1 : total
		}
	}

	static queryBySecondaryIndex(clazz, propertyMap, opts)
	{
		def options = OrmHelper.addOptionDefaults(opts, MAX_ROWS)
		def cluster = opts.cluster ?: clazz.cassandraCluster
		clazz.cassandra.withKeyspace(clazz.keySpace, cluster) {ks ->
			def properties = [:]
			propertyMap.each {k, v ->
				properties[k] = KeyHelper.primaryRowKey(v)
			}
			def rows = clazz.cassandra.persistence.getRowsWithEqualityIndex(ks, clazz.columnFamily, properties, options.max, opts.consistencyLevel)
			OrmHelper.checkForDefaultRowsInsufficient(opts.max, rows.size())
			return clazz.cassandra.mapping.makeResult(rows, options)
		}
	}

	static countBySecondaryIndex(clazz, propertyMap, opts)
	{
		def cluster = opts.cluster ?: clazz.cassandraCluster
		clazz.cassandra.withKeyspace(clazz.keySpace, cluster) {ks ->
			def properties = [:]
			propertyMap.each {k, v ->
				properties[k] = KeyHelper.primaryRowKey(v)
			}
			return clazz.cassandra.persistence.countRowsWithEqualityIndex(ks, clazz.columnFamily, properties, opts.consistencyLevel)
		}
	}

	static queryByCql(clazz, opts) throws IllegalArgumentException
	{
		def options = OrmHelper.addOptionDefaults(opts, MAX_ROWS)
		def cluster = opts.cluster ?: clazz.cassandraCluster
		clazz.cassandra.withKeyspace(clazz.keySpace, cluster) {ks ->
			if (options.columns) {
				def rows = clazz.cassandra.persistence.getRowsColumnSliceWithCqlWhereClause(ks, clazz.columnFamily, options.where, options.max, options.columns, opts.consistencyLevel)
				return clazz.cassandra.mapping.makeResult(rows, options)
			}
			else if (options.column) {
				def rows = clazz.cassandra.persistence.getRowsColumnSliceWithCqlWhereClause(ks, clazz.columnFamily, options.where, options.max, [options.column], opts.consistencyLevel)
				return clazz.cassandra.mapping.makeResult(rows, options)
			}
			else {
				def rows = clazz.cassandra.persistence.getRowsWithCqlWhereClause(ks, clazz.columnFamily, options.where, options.max, opts.consistencyLevel)
				return clazz.cassandra.mapping.makeResult(rows, options)
			}
		}
	}

	static countByCql(clazz, opts) throws IllegalArgumentException
	{
		def options = OrmHelper.addOptionDefaults(opts, MAX_ROWS)
		def cluster = opts.cluster ?: clazz.cassandraCluster
		clazz.cassandra.withKeyspace(clazz.keySpace, cluster) {ks ->
			return clazz.cassandra.persistence.getRowsWithCqlWhereClause(ks, clazz.columnFamily, options.where, opts.consistencyLevel)
		}
	}

	static cqlWhereExpression(String where, values) throws IllegalArgumentException
	{
		def result = new StringBuilder()
		def strings = where.split('?')
		values.each {value, i ->
			result << strings[i]
			result << "'"
			result << KeyHelper.primaryRowKey(value)
			result << "'"
		}
		result << strings[-1]
		return result.toString()
	}

	static cqlWhereExpression(Map exp, values) throws IllegalArgumentException
	{
		throw new IllegalArgumentException("The 'where' parameter must be a String. Use findWhere or countWhere when specifiying a map of properties.")
	}

	static cqlWhereExpression(exp, values) throws IllegalArgumentException
	{
		throw new IllegalArgumentException("The 'where' parameter must be a String")
	}

	static getByMappedObject(thisObj, propName, itemClass, opts=[:], listClass=LinkedHashSet)
	{
		def result = []
		def options = OrmHelper.addOptionDefaults(opts, MAX_ROWS, thisObj.getProperty(CLUSTER_PROP))
		def start = KeyHelper.nullablePrimaryRowKey(options.startAfter ?: options.start)
		def finish = KeyHelper.nullablePrimaryRowKey(options.finish)
		def max = options.startAfter ? options.max + 1 : options.max
		def itemColumnFamily = itemClass.columnFamily
		def persistence = thisObj.cassandra.persistence
		def belongsToPropName = itemClass.belongsToPropName(thisObj.getClass())

		thisObj.cassandra.withKeyspace(thisObj.keySpace, thisObj.cassandraCluster) {ks ->
			def indexCF = itemClass.indexColumnFamily
			def indexKey = KeyHelper.joinRowKey(thisObj.class, itemClass, propName, thisObj)

			def keys = persistence.getColumnRange(ks, indexCF, indexKey, start, finish, options.reversed, max, opts.consistencyLevel)
					.collect{persistence.name(it)}

			def names = columnNames(options)
			def resultClass = options.startAfter ? LinkedList : listClass
			if (names) {
				def rows = persistence.getRowsColumnSlice(ks, itemColumnFamily, keys, names, opts.consistencyLevel)
				result = thisObj.cassandra.mapping.makeResult(keys, rows, options, resultClass)
			}
			else {
				def rows = persistence.getRows(ks, itemColumnFamily, keys, opts.consistencyLevel)
				result = thisObj.cassandra.mapping.makeResult(keys, rows, options, resultClass)
				if (belongsToPropName) {
					result.each {
						it.setProperty(belongsToPropName, thisObj)
					}
				}
			}
		}
		return options.startAfter && result ? result[1..-1] : result
	}

	static getKeysForMappedObject(thisClass, thisId, propName, itemClass, opts=[:])
	{
		def result = []
		def options = OrmHelper.addOptionDefaults(opts, MAX_ROWS, thisClass.cassandraCluster)
		def start = KeyHelper.nullablePrimaryRowKey(options.startAfter ?: options.start)
		def finish = KeyHelper.nullablePrimaryRowKey(options.finish)
		def max = options.startAfter ? options.max + 1 : options.max
		def persistence = thisClass.cassandra.persistence

		thisClass.cassandra.withKeyspace(thisClass.keySpace, thisClass.cassandraCluster) {ks ->
			def indexCF = itemClass.indexColumnFamily
			def indexKey = KeyHelper.joinRowKeyFromId(thisClass, itemClass, propName, thisId)

			result = persistence.getColumnRange(ks, indexCF, indexKey, start, finish, options.reversed, max, opts.consistencyLevel)
					.collect{persistence.name(it)}
		}
		return options.startAfter && result ? result[1..-1] : result
	}

	static countByMappedObject(thisObj, propName, itemClass, opts=[:])
	{
		def result = []
		def options = OrmHelper.addOptionDefaults(opts, MAX_ROWS)
		def persistence = thisObj.cassandra.persistence
		def start = KeyHelper.nullablePrimaryRowKey(options.startAfter ?: options.start)
		def finish = KeyHelper.nullablePrimaryRowKey(options.finish)
		thisObj.cassandra.withKeyspace(thisObj.keySpace, thisObj.cassandraCluster) {ks ->
			def indexCF = itemClass.indexColumnFamily
			def indexKey = KeyHelper.joinRowKey(thisObj.class, itemClass, propName, thisObj)

			result = persistence.countColumnRange(ks, indexCF, indexKey, start, finish, opts.consistencyLevel)
		}
		return options.startAfter && result ? result - 1 : result
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


}
