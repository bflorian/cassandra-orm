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

import org.codehaus.groovy.grails.commons.GrailsClassUtils
import org.apache.commons.beanutils.PropertyUtils

/**
 * @author: Bob Florian
 */
class InstanceMethods extends MappingUtils
{
	static void addDynamicOrmMethods(clazz, ctx)
	{
		// cassandra
		clazz.metaClass.getCassandra = { clazz.cassandra }

		// cassandra client used by ORM
		clazz.metaClass.getCassandraClient = { clazz.cassandraClient }

		// cassandraCluster
		clazz.metaClass.getCassandraCluster = { delegate.getProperty(CLUSTER_PROP) ?: clazz.cassandraCluster }

		// keySpace
		clazz.metaClass.getKeySpace = { clazz.keySpace }

		// columnFamily()
		clazz.metaClass.getColumnFamily = { clazz.columnFamily }

		// indexColumnFamily()
		clazz.metaClass.getIndexColumnFamily = { clazz.indexColumnFamily }

		// counterColumnFamily()
		clazz.metaClass.getCounterColumnFamily = { clazz.counterColumnFamily }

		// cluster property
		clazz.metaClass."${CLUSTER_PROP}" = null

		// cassandraKey
		clazz.metaClass.getId = {
			def thisObj = delegate
			def names = collection(cassandraMapping.primaryKey ?: cassandraMapping.unindexedPrimaryKey)
			def values = names.collect {
				def value = thisObj.getProperty(it)
				primaryRowKey(value)
			}
			def result = makeComposite(values)

			return result
		}

		// save()
		clazz.metaClass.save = {args ->
			def thisObj = delegate
			def ttl = args?.ttl ?: cassandraMapping.timeToLive
			if (args?.cluster) {
				thisObj.setProperty(CLUSTER_PROP, args.cluster)
			}
			def cluster = thisObj.cassandraCluster
			def persistence = cassandra.persistence
			cassandra.withKeyspace(thisObj.keySpace, cluster) {ks ->
				def m = persistence.prepareMutationBatch(ks, args?.consistencyLevel)

				// get the primary row key
				def id
				try {
					id = thisObj.id
				}
				catch (CassandraMappingNullIndexException e) {
					// if primary key is a UUID and its null, set it
					def keyNames = collection(cassandraMapping.primaryKey ?: cassandraMapping.unindexedPrimaryKey)
					def keyClass = keyNames.size() == 1 ? clazz.getDeclaredField(keyNames[0]).type : null
					if (keyClass == UUID) {
						thisObj.setProperty(keyNames[0], UUID.timeUUID())
						id = thisObj.id
					}
					else {
						throw e
					}
				}

				// see if it exists
				def oldObj = args?.nocheck ? null : clazz.get(id, [cluster:cluster])

				// one-to-one relationships
				def keyDeleted = false
				clazz.metaClass.properties.each {property ->

					// need to save object (if cascading) and remove the key column from cassandra if its
					// been set to null (uses dirty bit to know if it should be null since values are
					// lazy evaluated
					def name = property.name
					if (name.endsWith(DIRTY_SUFFIX)) {
						def pName = name - DIRTY_SUFFIX
						def pValue = PropertyUtils.getProperty(thisObj, pName)
						def dValue = thisObj.getProperty(name)
						if (pValue == null) {
							if (dValue != null) {
								// property is null but dirty bit is not null -- remove the key from cassandra
								thisObj.setProperty(name, null)
								def cName = "${pName}${KEY_SUFFIX}".toString()
								persistence.deleteColumn(m, thisObj.columnFamily, thisObj.id, cName)
								keyDeleted = true

								// back links
								if (!(dValue instanceof Boolean)) {
									def backLinkRowKey = oneBackIndexRowKey(dValue.id)
									def backLinkColName = oneBackIndexColumnName(persistence.columnFamilyName(thisObj.columnFamily), pName, id)
									persistence.deleteColumn(m, pValue.indexColumnFamily, backLinkRowKey, backLinkColName, '')
								}
							}
						}
						else {
							// back links
							def backLinkRowKey = oneBackIndexRowKey(pValue.id)
							def backLinkColName = oneBackIndexColumnName(persistence.columnFamilyName(thisObj.columnFamily), pName, id)
							persistence.putColumn(m, pValue.indexColumnFamily, backLinkRowKey, backLinkColName, '')

							// cascade?
							if (args?.cascade) {
								pValue.save(cluster: thisObj.getProperty(CLUSTER_PROP))
							}
						}
					}
				}

				// commit deletion of relationship keys
				if (keyDeleted) {
					persistence.execute(m)
					m = persistence.prepareMutationBatch(ks, args?.consistencyLevel)
				}

				// manage index rows
				def indexRows = [:]
				def oldIndexRows = [:]
				def indexColumnFamily = thisObj.indexColumnFamily

				// primary key index
				if (cassandraMapping.primaryKey) {
					indexRows[primaryKeyIndexRowKey()] = [(thisObj.id):'']
				}

				// explicit indexes
				cassandraMapping.explicitIndexes?.each {propName ->
					if (oldObj) {

						def oldIndexRowKeys = objectIndexRowKeys(propName, oldObj)
						oldIndexRowKeys.each {oldIndexRowKey ->
							oldIndexRows[oldIndexRowKey] = [(oldObj.id):'']
						}
					}

					def indexRowKeys = objectIndexRowKeys(propName, thisObj)
					indexRowKeys.each {indexRowKey ->
						indexRows[indexRowKey] = [(thisObj.id):'']
					}
				}

				oldIndexRows.each {rowKey, col ->
					col.each {colKey, v ->
						persistence.deleteColumn(m, indexColumnFamily, rowKey, colKey)
					}
				}

				// delete old index row keys
				if (oldIndexRows) {
					persistence.execute(m)
					m = persistence.prepareMutationBatch(ks, args?.consistencyLevel)
				}

				// insert this object
				def dataProperties = cassandra.mapping.dataProperties(thisObj)
				persistence.putColumns(m, thisObj.columnFamily, id, dataProperties, ttl)

				// insert new index row keys
				if (indexRows) {
					indexRows.each {rowKey, cols ->
						persistence.putColumns(m, indexColumnFamily, rowKey, cols, ttl)
					}
				}

				// counters
				cassandraMapping.counters?.each {ctr ->
					updateCounterColumns(clazz, ctr, m, oldObj, thisObj)
				}

				persistence.execute(m)
			}
			thisObj
		}

		// insert(properties)
		clazz.metaClass.insert = {properties, timeToLive=null, consistencyLevel=null ->
			def thisObj = delegate
			def ttl = timeToLive ?: cassandraMapping.timeToLive
			def persistence = cassandra.persistence
			cassandra.withKeyspace(thisObj.keySpace, thisObj.cassandraCluster) {ks ->
				def m = persistence.prepareMutationBatch(ks, consistencyLevel)

				// check one-to-one relationship properties
				def keyDeleted = false
				properties.each {name, value ->
					if (value == null) {
						def prop = PropertyUtils.getProperty(thisObj, name)
						if (prop != null && isMappedObject(prop)) {
							def cName = "${pName}${KEY_SUFFIX}".toString()
							persistence.deleteColumn(m, thisObj.columnFamily, thisObj.id, cName)
							keyDeleted = true
						}

					}
				}

				// commit deletion of relationship keys
				if (keyDeleted) {
					persistence.execute(m)
					m = persistence.prepareMutationBatch(ks, consistencyLevel)
				}

				// manage index rows
				def indexRows = [:]
				def oldIndexRows = [:]
				def indexColumnFamily = thisObj.indexColumnFamily

				// remove old explicit indexes
				def propertyNames = properties.keySet()
				cassandraMapping.explicitIndexes?.each {propName ->
					def names = collection(propName)
					if (!Collections.disjoint(propertyNames, names)) {
						def oldIndexRowKeys = objectIndexRowKeys(propName, thisObj)
						oldIndexRowKeys.each {oldIndexRowKey ->
							oldIndexRows[oldIndexRowKey] = [(thisObj.id):'']
						}
					}
				}

				// decrement old counters
				// TODO - filter out those not in properties?
				cassandraMapping.counters?.each {ctr ->
					updateCounterColumns(clazz, ctr, m, thisObj, null)
				}

				// set the new properties
				properties.each {name, value ->
					thisObj.setProperty(name, value)
				}

				// add new explicit indexes
				cassandraMapping.explicitIndexes?.each {propName ->
					def names = collection(propName)
					if (!Collections.disjoint(propertyNames, names)) {
						def indexRowKeys = objectIndexRowKeys(propName, thisObj)
						indexRowKeys.each {indexRowKey ->
							indexRows[indexRowKey] = [(thisObj.id):'']
						}
					}
				}

				// do the deletions
				oldIndexRows.each {rowKey, col ->
					col.each {colKey, v ->
						persistence.deleteColumn(m, indexColumnFamily, rowKey, colKey)
					}
				}

				// delete old index row keys
				if (oldIndexRows) {
					persistence.execute(m)
					m = persistence.prepareMutationBatch(ks, args?.consistencyLevel)
				}

				// insert this object
				def dataProperties = cassandra.mapping.dataProperties(properties)
				persistence.putColumns(m, thisObj.columnFamily, id, dataProperties, ttl)

				// do the additions
				if (indexRows) {
					indexRows.each {rowKey, cols ->
						persistence.putColumns(m, indexColumnFamily, rowKey, cols, ttl)
					}
				}

				// increment new counters
				// TODO - filter out those not in properties?
				cassandraMapping.counters?.each {ctr ->
					updateCounterColumns(clazz, ctr, m, null, thisObj)
				}

				persistence.execute(m)
			}
			thisObj
		}

		// delete()
		clazz.metaClass.delete = {args=[:] ->
			def thisObj = delegate
			def thisObjId = thisObj.id
			def persistence = cassandra.persistence
			def indexColumnFamily = thisObj.indexColumnFamily

			cassandra.withKeyspace(thisObj.keySpace, thisObj.cassandraCluster) {ks ->

				// delete the object
				def m = persistence.prepareMutationBatch(ks, args?.consistencyLevel)
				persistence.deleteRow(m, thisObj.columnFamily, thisObjId)

				// primary index
				if (cassandraMapping.primaryKey) {
					persistence.deleteColumn(m, indexColumnFamily, primaryKeyIndexRowKey(), thisObjId)
				}

				// explicit indexes
				cassandraMapping.explicitIndexes?.each {propName ->
					def indexRowKeys = objectIndexRowKeys(propName, thisObj)
					indexRowKeys.each {indexRowKey ->
						persistence.deleteColumn(m, indexColumnFamily, indexRowKey, thisObjId)
					}
				}

				// hasMany indexes
				def hasManyKey = manyBackIndexRowKey(thisObjId)
				def manyBackLinkRow = persistence.getRow(ks, indexColumnFamily, hasManyKey, args?.consistencyLevel)
				manyBackLinkRow.each {col ->
					def manyIndexRowKey = persistence.name(col)
					persistence.deleteColumn(m, indexColumnFamily, manyIndexRowKey, thisObjId)
				}

				// hasMany back pointers
				persistence.deleteRow(m, indexColumnFamily, hasManyKey)

				// hasOne properties
				def oneBackLinkKey = oneBackIndexRowKey(thisObjId)
				def oneBackLinkRow = persistence.getRow(ks, indexColumnFamily, oneBackLinkKey, args?.consistencyLevel)
				oneBackLinkRow.each {col ->
					def oneIndexArgs = oneBackIndexColumnValues(persistence.name(col))
					def objectRowKey = oneIndexArgs[2]
					def objectColumnFamily = oneIndexArgs[0]
					def objectPropName = "${oneIndexArgs[1]}Id".toString()
					persistence.deleteColumn(m, objectColumnFamily, objectRowKey, objectPropName)
				}

				// hasOne back pointers
				persistence.deleteRow(m, indexColumnFamily, oneBackLinkKey)

				// decrement counters
				cassandraMapping.counters?.each {ctr ->
					updateCounterColumns(clazz, ctr, m, thisObj, null)
				}

				persistence.execute(m)
			}
		}

		// hasMany properties
		if (clazz.metaClass.hasMetaProperty('hasMany')) {

			clazz.hasMany.each {propName, propClass ->

				def getterName = GrailsClassUtils.getGetterName(propName)
				def setterName = GrailsClassUtils.getSetterName(propName)
				def counterFunctionName = "${propName}Count".toString()
				def addToName = methodForPropertyName("addTo", propName)
				def removeFromName = methodForPropertyName("removeFrom", propName)

				// setter
				clazz.metaClass."${setterName}" = {
					PropertyUtils.setProperty(delegate, propName, it)
				}

				// getter
				clazz.metaClass."${getterName}" = {
					def iClass = PropertyUtils.getPropertyType(delegate, propName)
					def items = iClass ? PropertyUtils.getProperty(delegate, propName) : null
					if (items == null) {
						def oClass = iClass && List.isAssignableFrom(iClass) ? LinkedList : LinkedHashSet
						items = getFromHasMany(delegate, propName, [:], oClass)
						if (iClass) {
							PropertyUtils.setProperty(delegate, propName, items)
						}
					}
					return items
				}

				// getter function
				clazz.metaClass."${propName}" = { options ->
					return getFromHasMany(delegate, propName, options)
				}

				// counter function
				clazz.metaClass."${counterFunctionName}" = { options = [:] ->
					return countFromHasMany(delegate, propName, options)
				}

				// addTo...
				clazz.metaClass."${addToName}" = { item, consistencyLevel=null ->
					def thisObj = delegate
					def persistence = cassandra.persistence
					if (thisObj.getProperty(CLUSTER_PROP)) {
						item.setProperty(CLUSTER_PROP, thisObj.getProperty(CLUSTER_PROP))
					}

					// set belongsTo value
					safeGetStaticProperty(item.class, "belongsTo")?.each {name1, clazz1 ->
						if (clazz1 == clazz) {
							item.setProperty(name1, thisObj)
						}
					}

					// save the item
					item.save()

					// null this property so that it is lazy-evaluated the next time
					safeSetProperty(thisObj, propName, null)

					// add the indexes
					cassandra.withKeyspace(delegate.keySpace, delegate.cassandraCluster) {ks ->
						def m = persistence.prepareMutationBatch(ks, consistencyLevel)

						// save join row from this object to the item
						saveJoinRow(cassandra.persistence, m, clazz, thisObj, item.class, item, propName)

						// save the join row from the item to this object, if there is one
						safeGetStaticProperty(item.class, "hasMany")?.each {name1, clazz1 ->
							if (clazz1 == clazz) {
								saveJoinRow(cassandra.persistence, m, item.class, item, clazz, thisObj, name1)
							}
						}

						persistence.execute(m)
					}

					return thisObj
				}

				// removeFrom...
				clazz.metaClass."${removeFromName}" = { item, consistencyLevel=null ->
					def thisObj = delegate
					def persistence = cassandra.persistence
					safeSetProperty(thisObj, propName, null)

					cassandra.withKeyspace(delegate.keySpace, delegate.cassandraCluster) {ks ->
						def m = persistence.prepareMutationBatch(ks, consistencyLevel)

						// remove join row from this object to the item
						removeJoinRow(persistence, m, clazz, thisObj, item.class, item, propName)

						// remove the join row from the item to this object, if there is one
						safeGetStaticProperty(item.class, "hasMany")?.each {name1, clazz1 ->
							if (clazz1 == clazz) {
								def items1 = item.getProperty(name1)
								def listItem1 = items1.find{it.id == thisObj.id}
								items1.remove(listItem1)
								removeJoinRow(persistence, m, item.class, item, clazz, thisObj, name1)
							}
						}

						// null out belongsTo value
						safeGetStaticProperty(item.class, "belongsTo")?.each {name1, clazz1 ->
							if (clazz1 == clazz) {
								item.setProperty(name1, null)
								// TODO - convert to save when we have a way to null out relationships
								// TODO - break out into a util function
								def colKey = "${name1}${KEY_SUFFIX}".toString()
								persistence.deleteColumn(m, item.columnFamily, item.id, colKey)
							}
						}
						persistence.execute(m)
					}
					return thisObj
				}
			}
		}

		// for each property
		clazz.metaClass.properties.each {property ->
			if (property.type.metaClass.hasMetaProperty("cassandraMapping")) {

				def propName = property.name
				def getterName = GrailsClassUtils.getGetterName(propName)
				def setterName = GrailsClassUtils.getSetterName(propName)

				// dirty bit
				clazz.metaClass."${propName}${DIRTY_SUFFIX}" = null

				// setter
				clazz.metaClass."${setterName}" = {
					// TODO - does this need to default to true?
					delegate.setProperty("${propName}${DIRTY_SUFFIX}", PropertyUtils.getProperty(delegate, propName) ?: true)
					PropertyUtils.setProperty(delegate, propName, it)
				}

				// getter
				clazz.metaClass."${getterName}" = {
					def value = PropertyUtils.getProperty(delegate, propName)
					if (value == null) {
						def persistence = cassandra.persistence
						def thisObj = delegate
						def cf = property.type.columnFamily

						// TODO - need to find a way to store this in the object!
						//def id = PropertyUtils.getProperty(delegate, "${propName}${KEY_SUFFIX}")

						cassandra.withKeyspace(keySpace, cassandraCluster) {ks ->
							def consistencyLevel = null //TODO - this OK?
							def colName = "${propName}${KEY_SUFFIX}".toString()
							def cols = persistence.getColumnSlice(ks, columnFamily, thisObj.id, [colName], consistencyLevel)
							def col = persistence.getColumn(cols, colName)
							if (col) {
								def pid = persistence.stringValue(col)
								def data = persistence.getRow(ks, cf, pid, consistencyLevel)
							    value = cassandra.mapping.newObject(data, delegate.getProperty(CLUSTER_PROP))
							}
						}
					}
					PropertyUtils.setProperty(delegate, propName, value)
					return value
				}

				// id getter
				clazz.metaClass."${getterName}Id" = {
					def result = null
					def value = PropertyUtils.getProperty(delegate, propName)
					if (value?.id) {
						result = value.id
					}
					else {
						def persistence = cassandra.persistence
						def thisObj = delegate

						// TODO - need to find a way to store this in the object!
						//def id = PropertyUtils.getProperty(delegate, "${propName}${KEY_SUFFIX}")

						cassandra.withKeyspace(keySpace, cassandraCluster) {ks ->
							def consistencyLevel = null //TODO - this OK?
							def colName = "${propName}${KEY_SUFFIX}".toString()
							def cols = persistence.getColumnSlice(ks, columnFamily, thisObj.id, [colName], consistencyLevel)
							def col = persistence.getColumn(cols, colName)
							if (col) {
								result = persistence.stringValue(col)
							}
						}
					}
					return result
				}
			}
		}

		// Expando properties setter
		clazz.metaClass.propertyMissing = {String name, arg ->
			if (cassandraMapping.expandoMap) {
				def map = delegate.getProperty(cassandraMapping.expandoMap)
				if (map == null) {
					map = [:] //TODO - consider type?
					delegate.setProperty(cassandraMapping.expandoMap, map)
				}
				map[name] = arg
			}
			else {
				throw new MissingPropertyException(name, clazz)
			}
		}

		// Expando properties getter
		clazz.metaClass.propertyMissing = {String name ->
			if (cassandraMapping.expandoMap && name != "transients") {
				def map = delegate.getProperty(cassandraMapping.expandoMap)
				return map ? map[name] : null
			}
			else {
				throw new MissingPropertyException(name, clazz)
			}
		}

		// toString()
		clazz.metaClass.toString = {
			"${delegate.getClass().getName().split('\\.')[-1]}(${delegate.id})"
		}
	}
}
