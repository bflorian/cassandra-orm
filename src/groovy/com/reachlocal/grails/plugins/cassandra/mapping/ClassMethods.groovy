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

/**
 * @author: Bob Florian
 */
class ClassMethods extends MappingUtils
{
	static void addDynamicOrmMethods(clazz, ctx) throws CassandraMappingException
	{
		// check completeness
		if (!clazz.metaClass.hasMetaProperty('cassandraMapping')) {
			throw new CassandraMappingException("cassandraMapping not specified")
		} else if (!safeGetStaticProperty(clazz, 'cassandraMapping')?.primaryKey &&
				!safeGetStaticProperty(clazz, 'cassandraMapping')?.unindexedPrimaryKey) {
			throw new CassandraMappingException("cassandraMapping.primaryKey not specified")
		}

		// cassandra
		clazz.metaClass.'static'.getCassandra = {
			ctx.getBean("cassandraOrmService")
		}

		// cassandra client used by ORM
		clazz.metaClass.'static'.getCassandraClient = {
			clazz.cassandra.client
		}

		// set options mapping properties
		if (!clazz.cassandraMapping.timeToLive) {
			clazz.cassandraMapping.timeToLive = [:]
		}
		if (!clazz.cassandraMapping.cluster) {
			clazz.cassandraMapping.cluster = "standard"
		}
		if (!clazz.cassandraMapping.keySpace) {
			clazz.cassandraMapping.keySpace = clazz.cassandra.defaultKeyspaceName(clazz.cassandraMapping.cluster)
		}
		if (!clazz.cassandraMapping.columnFamily) {
			clazz.cassandraMapping.columnFamily = clazz.simpleName
		}
		clazz.cassandraMapping.columnFamily_object = clazz.cassandra.persistence.columnFamily(clazz.cassandraMapping.columnFamily)
		clazz.cassandraMapping.indexColumnFamily_object = clazz.cassandra.persistence.columnFamily("${clazz.cassandraMapping.columnFamily}_IDX".toString())
		clazz.cassandraMapping.counterColumnFamily_object = clazz.cassandra.persistence.columnFamily("${clazz.cassandraMapping.columnFamily}_CTR".toString())

		// initialize counter types
		clazz.cassandraMapping.counters?.eachWithIndex {ctr, index ->
			if (ctr.groupBy) {
				ctr.groupBy = OrmHelper.collection(ctr.groupBy)
				def prop = clazz.metaClass.getMetaProperty(ctr.groupBy[0])
				if (prop) {
					ctr.isDateIndex = prop.type.isAssignableFrom(Date)
					if (ctr.isDateIndex) {
						ctr.dateIndexProp = ctr.groupBy[0]
					}
				}
				else {
					throw new CassandraMappingException("The groupBy property '${ctr.groupBy[0]}' is missing from counter ${index+1}")
				}
			}
			else {
				throw new CassandraMappingException("The groupBy property is missing from counter ${index+1}")
			}
		}

		// cassandraCluster
		clazz.metaClass.'static'.getCassandraCluster = {
			return cassandraMapping.cluster
		}

		// keySpace
		clazz.metaClass.'static'.getKeySpace = {
			return cassandraMapping.keySpace
		}

		// columnFamilyName
		clazz.metaClass.'static'.getColumnFamilyName = {
			return cassandraMapping.columnFamily
		}

		// columnFamily
		clazz.metaClass.'static'.getColumnFamily = {
			return cassandraMapping.columnFamily_object
		}

		// indexColumnFamily
		clazz.metaClass.'static'.getIndexColumnFamily = {
			return cassandraMapping.indexColumnFamily_object
		}

		// counterColumnFamily
		clazz.metaClass.'static'.getCounterColumnFamily = {
			return cassandraMapping.counterColumnFamily_object
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
			def rowKey = KeyHelper.primaryRowKey(id)
			def cluster = opts.cluster ?: cassandraCluster
			cassandra.withKeyspace(keySpace, cluster) {ks ->
				def data = cassandra.persistence.getRow(ks, columnFamily, rowKey, opts.consistencyLevel)
				result = cassandra.mapping.newObject(data, clazz, opts.cluster)
			}
			return result
		}

		// get(id, options?)
		clazz.metaClass.'static'.getAll = {ids, opts=[:] ->
			def result = null
			def rowKeys = ids.collect{KeyHelper.primaryRowKey(it)}
			def cluster = opts.cluster ?: cassandraCluster
			cassandra.withKeyspace(keySpace, cluster) {ks ->
				def options = OrmHelper.addOptionDefaults(opts, MAX_ROWS)
				def names = columnNames(options)
				if (names) {
					def rows = cassandra.persistence.getRowsColumnSlice(ks, clazz.columnFamily, rowKeys, names, opts.consistencyLevel)
					result = cassandra.mapping.makeResult(rowKeys, rows, options, clazz)
				}
				else {
					def rows = cassandra.persistence.getRows(ks, clazz.columnFamily, rowKeys, opts.consistencyLevel)
					result = cassandra.mapping.makeResult(rowKeys, rows, options, clazz)
				}
			}
			return result
		}

		// findOrCreate(id, options?)
		clazz.metaClass.'static'.findOrCreate = {id, opts=[:] ->
			def result = get(id, opts)
			if (!result) {
				def names = OrmHelper.collection(cassandraMapping.primaryKey ?: cassandraMapping.unindexedPrimaryKey)
				if (names.size() > 1) {
					throw new CassandraMappingException("findOrCreate() and findOrSave() methods not defined for classes with compound keys")
				}
				else {
					clazz.newInstance()
					result = clazz.newInstance()
					result.setProperty(names[0], id)
					if (opts.cluster) {
						result.setProperty(CLUSTER_PROP, opts.cluster)
					}
				}
			}
			return result
		}

		// findOrCreate(id, options?)
		clazz.metaClass.'static'.findOrSave = {id, opts=[:] ->
			def result = findOrCreate(id, opts)
			result.save()
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

			def options = OrmHelper.addOptionDefaults(opts, MAX_ROWS)
			def cluster = opts.cluster ?: cassandraCluster
			def start = options.startAfter ?: options.start
			def max = options.startAfter ? options.max + 1 : options.max
			cassandra.withKeyspace(keySpace, cluster) {ks ->
				def columns = cassandra.persistence.getColumnRange(
						ks,
						indexColumnFamily,
						KeyHelper.primaryKeyIndexRowKey(),
						start,
						options.finish,
						options.reversed,
						max,
						opts.consistencyLevel
				)

				def keys = columns.collect{cassandra.persistence.name(it)}
				OrmHelper.checkForDefaultRowsInsufficient(opts.max, keys.size())

				def rows = cassandra.persistence.getRows(ks, columnFamily, keys, opts.consistencyLevel)
				def result = cassandra.mapping.makeResult(keys, rows, options, clazz, LinkedList)

                if (options.startAfter && result) {
                    if (result.size() <= 1) {
                            return []
                    } else {
                        return result[1..-1]
                    }
                } else {
                    return result
                }
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
				return queryBySecondaryIndex(clazz, params, opts)
			}
			else {
				throw new CassandraMappingException("No index found for specified arguments")
			}
		}

		// query
		clazz.metaClass.'static'.query = {args ->
			if (args.where) {
				return queryByCql(clazz, args)
			}
			else {
				throw new IllegalArgumentException("The 'where' parameter must be specified")
			}
		}

		// count
		clazz.metaClass.'static'.count = {args ->
			if (!args) {
				// TODO - should we count these if there is an indexed primary key?
				throw new IllegalArgumentException("The 'where' parameter must be specified")
			}
			else if (args.where) {
				return countByCql(clazz, args)
			}
			else {
				throw new IllegalArgumentException("The 'where' parameter must be specified")
			}
		}

		// TODO - merge these methods
		// getCounts(groupBy: 'hour')
		// getCounts(by: 'hour', where: [usid:'xxx'])
		// getCounts(by: ['hour','category'], where: [usid:'xxx'], groupBy: 'category')
		clazz.metaClass.'static'.getCounts = {params ->
			getCounters(
					clazz, cassandraMapping.counters, params.where ?: [:], params.groupBy ?: [],
					params.start, params.finish, params.sort, params.reversed, params.grain,
					params.timeZone, params.dateFormat, params.fill, params.consistencyLevel, params.cluster)
		}

		// getCounts(where: [usid:'xxx'])
		clazz.metaClass.'static'.getCountTotal = {params ->
			if (!params.where) {
				throw new IllegalArgumentException("The 'where' parameter must be specified")
			}
			else {
				getCountersForTotals(clazz, cassandraMapping.counters, params.where, params.groupBy ?: [], params.start, params.finish, params.consistencyLevel, params.cluster)
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
				result = queryBySecondaryIndex(clazz, params, options)
			}
			else {
				throw new CassandraMappingException("No index found for specified arguments")
			}
			return result ? result.toList()[0] : null
		}

		// countWhere(params, opts?)
		clazz.metaClass.'static'.countWhere = {params, opts=[:] ->
			def options = opts.clone()
			options.max = 1

			def result = null
			def filterList = expandFilters(params)
			def index = findIndex(cassandraMapping.explicitIndexes, filterList)
			if (index) {
				result = countByExplicitIndex(clazz, filterList, index, options)
			}
			else if (cassandraMapping.secondaryIndexes) {
				result = countBySecondaryIndex(clazz, params, options)
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
			def opts = (args && args[-1] instanceof Map) ? args[-1].clone() : [:]

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
				def propertyList = OrmHelper.propertyListFromMethodName(str)
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
					def cluster = opts.cluster ?: cassandraCluster
					cassandra.withKeyspace(keySpace, cluster) {ks ->
						def properties = [:]
						propertyList.eachWithIndex {it, i ->
							properties[it] = args[i]
						}
						if (count) {
							result = countBySecondaryIndex(clazz, properties, opts)
						}
						else {
							result = queryBySecondaryIndex(clazz, properties, opts)
						}
					}
				}
				return single ? (result ? result.toList()[0] : null) : result
			}
			else if (name.startsWith("getCounts")) {
				def total = false
				def wherePropList = []
				def groupByPropList = []
				if (name.startsWith("getCountsBy")) {
					str = name - "getCountsBy"
					total = str.endsWith("Total")
					if (total) {
						str = str - "Total"
					}
					else {
						def pos = str.indexOf("GroupBy")
						if (pos > 0) {
							groupByPropList = OrmHelper.propertyListFromMethodName(str[pos+7..-1])
							str = str[0..pos-1]
						}
					}
					wherePropList = OrmHelper.propertyListFromMethodName(str)
				}
				else if (name.startsWith("getCountsGroupBy")) {
					str = name - "getCountsGroupBy"
					total = str.endsWith("Total")
					if (total) {
						str = str - "Total"
					}
					groupByPropList = OrmHelper.propertyListFromMethodName(str)
				}
				else if (name == "getCountsTotal") {
					total = true
				}
				else {
					throw new MissingPropertyException(name, clazz)
				}

				def whereMap = [:]
				wherePropList.eachWithIndex {propName, index ->
					whereMap[propName] = args[index]
				}

				if (total) {
					return getCountersForTotals(clazz, cassandraMapping.counters, whereMap, groupByPropList, opts.start, opts.finish, opts.consistencyLevel, opts.cluster)
				}
				else {
					return getCounters(
							clazz, cassandraMapping.counters, whereMap, groupByPropList,
							opts.start, opts.finish, opts.sort, opts.reversed, opts.grain,
							opts.timeZone, opts.dateFormat, opts.fill, opts.consistencyLevel, opts.cluster)
				}
			}
			else {
				throw new MissingPropertyException(name, clazz)
			}
		}
	}
}
