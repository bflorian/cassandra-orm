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

import org.codehaus.groovy.grails.commons.ConfigurationHolder

/**
 * @author: Bob Florian
 */
class ClassMethods extends MappingUtils
{
	static final int MAX_ROWS = 1000

	static void addDynamicOrmMethods(clazz, ctx) throws CassandraMappingException
	{
		if (!clazz.metaClass.hasMetaProperty('cassandraMapping')) {
			throw new CassandraMappingException("cassandraMapping not specified")
		} else if (!safeGetStaticProperty(clazz, 'cassandraMapping')?.primaryKey &&
				!safeGetStaticProperty(clazz, 'cassandraMapping')?.unindexedPrimaryKey) {
			throw new CassandraMappingException("cassandraMapping.primaryKey not specified")
		}

		// cassandra
		clazz.metaClass.'static'.getCassandra = { ctx.getBean("cassandraOrmService") }

		// keySpace
		clazz.metaClass.'static'.getKeySpace = {
			if (clazz.metaClass.hasMetaProperty('cassandraMapping') && cassandraMapping?.keySpace) {
				return cassandraMapping?.keySpace
			}
			else {
				return ConfigurationHolder.config.cassandra.keySpace
			}
		}

		// columnFamilyName
		clazz.metaClass.'static'.getColumnFamilyName = {
			if (clazz.metaClass.hasMetaProperty('cassandraMapping') && cassandraMapping?.columnFamily) {
				cassandraMapping?.columnFamily
			}
			else {
				clazz.name.split("\\.")[-1]
			}
		}

		// columnFamily
		clazz.metaClass.'static'.getColumnFamily = {
			def cf = cassandraMapping.columnFamily_object
			if (cf == null) {
				cf = cassandra.persistence.columnFamily(columnFamilyName)
				cassandraMapping.columnFamily_object = cf
			}
			return cf
		}

		// indexColumnFamily
		clazz.metaClass.'static'.getIndexColumnFamily = {
			def cf = cassandraMapping.indexColumnFamily_object
			if (cf == null) {
				cf = cassandra.persistence.columnFamily("${columnFamilyName}_IDX".toString())
				cassandraMapping.indexColumnFamily_object = cf
			}
			return cf
		}

		// counterColumnFamily
		clazz.metaClass.'static'.getCounterColumnFamily = {
			def cf = cassandraMapping.counterColumnFamily_object
			if (cf == null) {
				cf = cassandra.persistence.columnFamily("${columnFamilyName}_CTR".toString())
				cassandraMapping.counterColumnFamily_object = cf
			}
			return cf
		}

		// belongsToClass(clazz2)
		clazz.metaClass.'static'.belongsToClass = { clazz2 ->
			if (clazz.metaClass.hasMetaProperty("belongsTo")) {
			    return belongsTo.find{it.value == clazz2} != null
			}
			return false
		}

		// TODO - combine with above?
		// belongsToClass(clazz2)
		clazz.metaClass.'static'.belongsToPropName = { clazz2 ->
			if (clazz.metaClass.hasMetaProperty("belongsTo")) {
				return belongsTo.find{it.value == clazz2}?.key
			}
			return null
		}

		// get(id, options?)
		clazz.metaClass.'static'.get = {id, opts=[:] ->
			def result = null
			def rowKey = primaryRowKey(id)
			cassandra.withKeyspace(keySpace) {ks ->
				def data = cassandra.persistence.getRow(ks, columnFamily, rowKey)
				result = cassandra.mapping.newObject(data)
			}
			return result
		}

		// list(opts?)
		// list(max: max_rows)
		// list(start: id1, max: max_rows)
		// list(start: id1, finish: id1)
		// list(start: id1, finish: id1, max: max_rows)
		// list(start: id1, finish: id1, reversed: true)
		// list(start: id1, finish: id1, reversed: true, max: max_rows)
		clazz.metaClass.'static'.list = {opts=[:] ->

			def options = addOptionDefaults(opts, MAX_ROWS)
			cassandra.withKeyspace(keySpace) {ks ->
				def columns = cassandra.persistence.getColumnRange(
						ks,
						indexColumnFamily,
						primaryKeyIndexRowKey(),
						options.start,
						options.finish,
						options.reversed,
						options.max)

				def keys = columns.collect{cassandra.persistence.name(it)}
				def rows = cassandra.persistence.getRows(ks, columnFamily, keys)
				def result = cassandra.mapping.makeResult(keys, rows, options)
				return result
			}
		}

		// findAllWhere(params, opts?)
		// findAllWhere(state: 'MD')
		// findAllWhere(phone: '301-555-1212')
		// findAllWhere(phone: ['301-555-1212','410-555-1234'])
		// findAllWhere(scope:'DP', scopeId:'text-account-1', eventType:'Radar')
		// findAllWhere(scope:'DP', scopeId:'text-account-1', eventType:'Radar', subType:['Review','Mention'])
		// findAllWhere(scope:'DP', scopeId:'text-account-1', categories: 'My Business', eventType:'Radar', subType:['Review','Mention'])
		clazz.metaClass.'static'.findAllWhere = {params, opts=[:] ->

			def filterList = expandFilters(params)
			def index = findIndex(cassandraMapping.explicitIndexes, filterList)
			if (index) {
				return queryByExplicitIndex(clazz, filterList, index, opts)
			}
			else if (cassandraMapping.secondaryIndexes) {
				// TODO - handle secondary indexes
			}
			else {
				throw new CassandraMappingException("No index found for specified arguments")
			}
		}

		// findWhere(params, opts?)
		clazz.metaClass.'static'.findWhere = {params, opts=[:] ->
			def options = opts.clone()
			options.max = 1

			def result = null
			def filterList = expandFilters(params)
			def index = findIndex(cassandraMapping.explicitIndexes, filterList)
			if (index) {
				result = queryByExplicitIndex(clazz, filterList, index, options)
			}
			else if (cassandraMapping.secondaryIndexes) {
				// TODO - handle secondary indexes
			}
			else {
				throw new CassandraMappingException("No index found for specified arguments")
			}
			return result ? result.toList()[0] : null
		}

		// findBy...(value)
		// findBy...And...(value1, value2)
		// findBy...And...And...(value1, value2, value3) ...
		//
		// findAllBy...(value)
		// findAllBy...(value, rowCount)
		// findAllBy...And...(value1, value2, rowCount)
		// findAllBy...And...And...(value1, value2, value3, rowCount) ...
		clazz.metaClass.'static'.methodMissing = {String name, args ->
			def single = false
			def count = false
			def str = null
			def result = null
			def opts = (args[-1] instanceof Map) ? args[-1].clone() : [:]
			if (name.startsWith("findAllBy") && name.size() > 9 && args.size() > 0) {
				str = name - "findAllBy"
			}
			else if (name.startsWith("findBy") && name.size() > 6 && args.size() > 0) {
				str = name - "findBy"
				opts.max = 1
				single = true
			}
			else if (name.startsWith("countBy") && name.size() > 7 && args.size() > 0) {
				str = name - "countBy"
				opts.max = 1
				count = true
			}
			if (str) {
				def propertyList = propertyListFromMethodName(str)
				def params = [:]
				propertyList.eachWithIndex {it, i ->
					params[it] = args[i]
				}
				def filterList = expandFilters(params)
				def index = findIndex(cassandraMapping.explicitIndexes, filterList)
				if (index) {
					if (count) {
						result = countByExplicitIndex(clazz, filterList, index, opts)
					}
					else {
						result = queryByExplicitIndex(clazz, filterList, index, opts)
					}
				}
				else {
					// find by query expression
					def options = addOptionDefaults(opts, MAX_ROWS)
					cassandra.withKeyspace(keySpace) {ks ->
						def properties = [:]
						propertyList.eachWithIndex {it, i ->
							properties[it] = args[i]
						}
						if (count) {
							result = cassandra.persistence.countRowsWithEqualityIndex(ks, columnFamily, properties)
						}
						else {
							def rows = cassandra.persistence.getRowsWithEqualityIndex(ks, columnFamily, properties, options.max)
							result = cassandra.mapping.makeResult(rows, options)
						}
					}
				}
				return single ? (result ? result.toList()[0] : null) : result
			}
			else {
				throw new MissingPropertyException(name, clazz)
			}
		}
	}
}
