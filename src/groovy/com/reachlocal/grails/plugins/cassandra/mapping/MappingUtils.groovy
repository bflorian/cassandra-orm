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

/**
 * @author: Bob Florian
 */
class MappingUtils extends CounterUtils
{
	static getCounters(
			Class clazz,
			List counterDefs,
			Map whereFilter,
			byPropNames,
			start, finish, sort, reversed, grain, timeZone, fill)
	{
		// TODO - combine with rowFilterList
		def multiWhereKeys = []
		whereFilter.each {key, values ->
			if (collection(values).size() > 1) {
				multiWhereKeys << key
			}
		}

		def counterDef = findCounter(counterDefs, whereFilter, collection(byPropNames), multiWhereKeys)
		if (counterDef == null) {
			throw new CassandraMappingException("Counter definition not found, where: ${whereFilter}, groupBy: ${byPropNames}")
		}
		def counterGroupByNames = counterDef.groupBy
		def rowFilterList = expandFilters(counterRowFilter(whereFilter, counterDef))
		def columnFilter = counterColumnFilter(whereFilter, counterDef)

		def value
		def groupByPropNames = byPropNames ? collection(byPropNames) : []
		if (groupByPropNames) {
			def i = 0
			def indexes = []

			if (counterDef.isDateIndex) {
				if (groupByPropNames.contains(counterDef.dateIndexProp)) {
					groupByPropNames.remove(counterDef.dateIndexProp)
					value = getDateCounterColumns(clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, sort)
					indexes << i
				}
				else {
					value = getDateCounterColumnsForTotals (clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish)
				}
				i = 1
			}
			else {
				value = getCounterColumns(clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, reversed)
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
 			value = groupBy(value, indexes)
		}
		else {
			if (counterDef.isDateIndex)  {
				value = getDateCounterColumns(clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, sort)
			}
			else {
				value = getCounterColumns(clazz, rowFilterList, multiWhereKeys, columnFilter, counterDef, start, finish, reversed)
			}
		}
		if (grain) {
			value = rollUpCounterDates(value, UTC_HOUR_FORMAT, grain, timeZone, fill, sort)
		}
		return value
	}

	static getCountersForTotals(
			Class clazz,
			List counterDefs,
			Map whereFilter,
			byPropNames, start, finish)
	{
		def counterDef = findCounter(counterDefs, whereFilter, collection(byPropNames))
		def rowFilterList = expandFilters(counterRowFilter(whereFilter, counterDef))
		def columnFilter = counterColumnFilter(whereFilter, counterDef)
		def cols = getDateCounterColumnsForTotals (clazz, rowFilterList, [], columnFilter, counterDef, start, finish)
		return mapTotal(cols)
	}

	static updateCounterColumns(Class clazz, Map counterDef, m, oldObj, thisObj)
	{
		def whereKeys = counterDef.findBy
		def groupKeys = collection(counterDef.groupBy)
		def counterColumnFamily = clazz.counterColumnFamily
		def cassandra = clazz.cassandra

		if (counterDef.isDateIndex) {
			if (oldObj) {
				def oldColName = counterColumnName(groupKeys, oldObj, UTC_HOUR_FORMAT)
				def gKeys = groupKeys
				def ocrk = counterRowKey(whereKeys, gKeys, oldObj)
				if (oldColName && ocrk) {
/*
					// all hours row
					cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)
*/
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

			if (thisObj) {
				def colName = counterColumnName(groupKeys, thisObj, UTC_HOUR_FORMAT)
				def gKeys = groupKeys
				def crk = counterRowKey(whereKeys, gKeys, thisObj)
				if (colName && crk) {
	/*
					// all hours row (to be deprecated)
					cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)
	*/
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
		}
		else {
			if (oldObj) {
				def oldColName = counterColumnName(groupKeys, oldObj, UTC_HOUR_FORMAT)
				def ocrk = counterRowKey(whereKeys, groupKeys, oldObj)
				if (oldColName && ocrk) {
					cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, ocrk, oldColName, -1)
				}
			}
			if (thisObj) {
				def colName = counterColumnName(groupKeys, thisObj, UTC_HOUR_FORMAT)
				def crk = counterRowKey(whereKeys, groupKeys, thisObj)
				if (colName && crk) {
					cassandra.persistence.incrementCounterColumn(m, counterColumnFamily, crk, colName)
				}
			}
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

	static findCounterOld(counterList, filterList, byList)
	{
		def filter1 = filterList[0]
		for (counter in counterList) {
			def findBy = counter.findBy ?: [] //TODO - right?
			def groupBy = collection(counter.groupBy)
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
			if (findByNames.contains(name)) {
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
			if (!findByNames.contains(name)) {
				result[name] = collection(values).collect{it.toString()}
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
			def counterGroupPropNames = collection(counter.groupBy)
			def queryFilterPropsRemaining = queryFilterPropNames.toSet()
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


}
