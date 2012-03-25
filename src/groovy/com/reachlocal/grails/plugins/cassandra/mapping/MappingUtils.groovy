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

import java.text.DecimalFormat

/**
 * @author: Bob Florian
 */
class MappingUtils 
{
	static final int MAX_ROWS = 1000
	
	static final Set SLICE_OPTIONS = ["column", "columns", "rawColumn", "rawColumns"]

	static String stringValue(String s)
	{
		'"' + s.replaceAll('"','\\"') + '"'
	}

	static String stringValue(s)
	{
		String.valueOf(s)
	}

	static updateBelongsToProperty(thisObj, item)
	{
		def entry = item.belongsTo.find{it.value == thisObj.class}
		item.setProperty(entry.key, thisObj)
	}

	static Map addOptionDefaults(options, defaultCount)
	{
		Map result = [
				reversed : options.reversed ? true : false,
				max : options.max ?: defaultCount,
				start: options.start,
				finish: options.finish
		]
		SLICE_OPTIONS.each {
			if (options[it]) {
				result[it] = options[it]
			}
		}
		return result
	}

	static String propertyFromMethodName(name)
	{
		return name[0].toLowerCase() + (name.size() > 1 ? name[1..-1] : '')
	}

	static String methodForPropertyName(prefix, propertyName)
	{
		return "${prefix}${propertyName[0].toUpperCase()}${propertyName.size() > 1 ? propertyName[1..-1] : ''}"
	}

	static String expressionFromMethodName(name, args)
	{
		def exps = name.replaceAll('([a-z,0-9])And([A-Z])','$1,$2').split(",")
		exps = exps.collect{it[0].toLowerCase() + (it.size() > 0 ? it[1..-1] : '')}
		expressionFromPropertyList(exps, args)
	}

	static String expressionFromPropertyList(exps, args)
	{
		def expression = new StringBuilder()
		def op = ""
		exps.eachWithIndex {it, index ->
			expression << op
			expression << "${it} = '${args[index]}'"
			op = " and "
		}
		return expression.toString()
	}

	static List preparedExpressionListFromPropertyList(columnFamily, exps, args)
	{
		def result = []
		exps.eachWithIndex {it, index ->
			result << columnFamily.newIndexClause().whereColumn(it).equals().value(args[index])
		}
		return result
	}

	static List propertyListFromMethodName(name)
	{
		def exps = name.replaceAll('([a-z,0-9])And([A-Z])','$1,$2').split(",")
		exps = exps.collect{it[0].toLowerCase() + (it.size() > 0 ? it[1..-1] : '')}
	}

	static collection(Collection value)
	{
		return value
	}

	static collection(value)
	{
		 return [value]
	}
	
	static String makeKey(name, value){
		"${name}\u00ff${value ?: ''}".toString()
	}

	static String makeComposite(list)
	{
		list.join("__")
	}

	static joinRowKey(fromClass, toClass, propName, object)
	{
		def fromClassName = fromClass.name.split("\\.")[-1]
		return makeKey("${fromClassName}.${propName}", object.id)
	}

	static primaryKeyIndexRowKey()
	{
		makeKey("this","")
	}

	static indexRowKey(propName, args)
	{
		def propNames = propertyListFromMethodName(propName)
		def i = 0
		def valuePart = makeComposite(propNames.collect{primaryRowKey(args[i++])})
		def namePart = makeComposite(propNames)
		return makeKey("this.${namePart}", valuePart)
	}

	static objectIndexRowKey(String propName, Map map)
	{
		def value = map[propName]
		return makeKey("this.${propName}", primaryRowKey(value))
	}

	static objectIndexRowKey(String propName, Object bean)
	{
		def value = bean.getProperty(propName)
		return makeKey("this.${propName}", primaryRowKey(value))
	}

	static objectIndexRowKey(List propNames, Map map)
	{
		def valuePart = makeComposite(propNames.collect{primaryRowKey(map[it])})
		def namePart = makeComposite(propNames)
		return makeKey("this.${namePart}", valuePart)
	}

	static objectIndexRowKey(List propNames, Object bean)
	{
		def valuePart = makeComposite(propNames.collect{primaryRowKey(bean.getProperty(it))})
		def namePart = makeComposite(propNames)
		return makeKey("this.${namePart}", valuePart)
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

	static convertToKey(String id)
	{
		return id
	}

	static convertToKey(object)
	{
		return object.id
	}

	static getFromHasMany(thisObj, propName, options=[:])
	{
		getByMappedObject(thisObj, propName, thisObj.hasMany[propName], options)
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

	static primaryRowKey(List objs)
	{
		makeComposite(objs.collect{primaryRowKey(it)})
	}

	static primaryRowKey(String id)
	{
		return id
	}

	static primaryRowKey(Number id)
	{
		id.toString()
	}

	static primaryRowKey(obj)
	{
		if (isMappedObject(obj)) {
			return obj.id
		}
		else {
			return obj?.toString()
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

	static getByMappedObject(thisObj, propName, itemClass, opts=[:])
	{
		def result = []
		def options = addOptionDefaults(opts, MAX_ROWS)
		def itemColumnFamily = itemClass.columnFamily
		def persistence = thisObj.cassandra.persistence
		thisObj.cassandra.withKeyspace(thisObj.keySpace) {ks ->
			def indexCF = itemClass.indexColumnFamily
			def indexKey = joinRowKey(thisObj.class, itemClass, propName, thisObj)

			def keys = persistence.getColumnRange(ks, indexCF, indexKey, options.start, options.finish, options.reversed, options.max)
					.collect{persistence.name(it)}

			def names = columnNames(options)
			if (names) {
				def rows = persistence.getRowsColumnSlice(ks, itemColumnFamily, keys, names)
				result = thisObj.cassandra.mapping.makeResult(keys, rows, options)
			}
			else {
				def rows = persistence.getRows(ks, itemColumnFamily, keys)
				result = thisObj.cassandra.mapping.makeResult(keys, rows, options)
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
