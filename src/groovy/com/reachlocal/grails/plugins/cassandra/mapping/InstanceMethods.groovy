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

import org.apache.commons.beanutils.PropertyUtils
import org.codehaus.groovy.grails.commons.GrailsClassUtils

import com.reachlocal.grails.plugins.cassandra.utils.CounterHelper
import com.reachlocal.grails.plugins.cassandra.utils.DataMapper
import com.reachlocal.grails.plugins.cassandra.utils.IndexHelper
import com.reachlocal.grails.plugins.cassandra.utils.KeyHelper
import com.reachlocal.grails.plugins.cassandra.utils.NestedHashMap
import com.reachlocal.grails.plugins.cassandra.utils.OrmHelper

/**
 * @author: Bob Florian
 */
class InstanceMethods extends MappingUtils
{
	static Profiler profiler = new Profiler()

	static dumpProfiler()
	{
		profiler.data()
	}

	static dumpProfilerAverages()
	{
		profiler.averages()
	}

	static dumpRemainingIds()
	{
		profiler.remainingIds()
	}

	static void clearProfiler()
	{
		profiler.clear()
	}

	static void addDynamicOrmMethods(clazz, ctx)
	{
		// cassandra
		clazz.metaClass.getCassandra = { clazz.cassandra }

		// cassandra client used by ORM
		clazz.metaClass.getCassandraClient = { clazz.cassandraClient }

		// cassandraCluster
		// TODO - this was causing CPU issues after running for a long time -- find out why
		//clazz.metaClass.getCassandraCluster = { delegate.getProperty(CLUSTER_PROP) ?: clazz.cassandraCluster }
		clazz.metaClass.getCassandraCluster = { clazz.cassandraCluster }

		// keySpace
		clazz.metaClass.getKeySpace = { clazz.keySpace }

		// columnFamily()
		clazz.metaClass.getColumnFamily = { clazz.columnFamily }

		// indexColumnFamily
		clazz.metaClass.getIndexColumnFamily = { clazz.indexColumnFamily }

		// counterColumnFamily
		clazz.metaClass.getCounterColumnFamily = { clazz.counterColumnFamily }

		// backLinkColumnFamily
		clazz.metaClass.getBackLinkColumnFamily = { clazz.backLinkColumnFamily }

		// cluster property
		clazz.metaClass."${CLUSTER_PROP}" = null

		// cassandra row key (alternative)
		clazz.metaClass.getId = {
			return KeyHelper.identKey(delegate, cassandraMapping)
		}

		// mapped property name cache
		clazz.cassandraMapping.mappedProperties = new LinkedHashSet()

		// cassandra row key
		clazz.metaClass.ident = {
			return KeyHelper.identKey(delegate, cassandraMapping)
		}

		// traverseRelationships
		clazz.metaClass.getTraverseRelationships = {
			return new Traverser(object: delegate, args: null)
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
					def keyNames = OrmHelper.collection(cassandraMapping.primaryKey ?: cassandraMapping.unindexedPrimaryKey)
					def keyClass = keyNames.size() == 1 ? clazz.getDeclaredField(keyNames[0]).type : null

					if (keyClass == UUID) {
						def uuid = UUID.timeUUID()
						thisObj.setProperty(keyNames[0], uuid)
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
				DataMapper.dirtyPropertyNames(thisObj).each {name ->

					// need to save object (if cascading) and remove the key column from cassandra if its
					// been set to null (uses dirty bit to know if it should be null since values are
					// lazy evaluated
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
							// TODO - UUID - move to separate column family
							if (!(dValue instanceof Boolean)) {
								def backLinkRowKey = KeyHelper.oneBackIndexRowKey(dValue.id)
								def backLinkColName = KeyHelper.oneBackIndexColumnName(persistence.columnFamilyName(thisObj.columnFamily), pName, id)
								persistence.deleteColumn(m, pValue.backLinkColumnFamily, backLinkRowKey, backLinkColName, '')
							}
						}
					}
					else {
						// back links
						// TODO - UUID -
						def backLinkRowKey = KeyHelper.oneBackIndexRowKey(pValue.id)
						def backLinkColName = KeyHelper.oneBackIndexColumnName(persistence.columnFamilyName(thisObj.columnFamily), pName, id)
						persistence.putColumn(m, pValue.backLinkColumnFamily, backLinkRowKey, backLinkColName, '')

						// cascade?
						if (args?.cascade) {
							pValue.save(cluster: thisObj.getProperty(CLUSTER_PROP))
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
					//indexRows[KeyHelper.primaryKeyIndexRowKey()] = [(thisObj.id):'']
					indexRows[KeyHelper.primaryKeyIndexRowKey()] = [(id):'']
				}

				// explicit indexes
				IndexHelper.updateAllExplicitIndexes(oldObj, thisObj, cassandraMapping, oldIndexRows, indexRows)

				//TODO - put this into the above?
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
				CounterHelper.updateAllCounterColumns(persistence, counterColumnFamily, cassandraMapping.counters, m, oldObj, thisObj)

				persistence.execute(m)
			}
			thisObj
		}

		// saveTimed()
		clazz.metaClass.saveTimed = {args ->
			profiler.increment("Iterations", 1L)

			// TIMER
			def t0 = System.nanoTime()

			def thisObj = delegate
			def t1 = System.nanoTime()
			profiler.increment("Save - Header/Delegate", t1-t0)
			t0 = t1

			def ttl = args?.ttl ?: cassandraMapping.timeToLive
			t1 = System.nanoTime()
			profiler.increment("Save - Header/TTL", t1-t0)
			t0 = t1

			// TODO - removed ability to pass in cluster because of CPU issues
			if (args?.cluster) {
				thisObj.setProperty(CLUSTER_PROP, args.cluster)
			}
			t1 = System.nanoTime()
			profiler.increment("Save - Header/Cluster Set", t1-t0)
			t0 = t1

			def cluster = cassandraMapping.cluster
			t1 = System.nanoTime()
			profiler.increment("Save - Header/Cluster Get", t1-t0)
			t0 = t1

			def persistence = cassandra.persistence
			t1 = System.nanoTime()
			profiler.increment("Save - Header/Persistence Get", t1-t0)
			t0 = t1

			cassandra.withKeyspace(thisObj.keySpace, cluster) {ks ->
				def m = persistence.prepareMutationBatch(ks, args?.consistencyLevel)

				// TIMER
				t1 = System.nanoTime()
				profiler.increment("Save - Prepare Mutation", t1-t0)
				t0 = t1

				// get the primary row key
				def id
				try {
					id = thisObj.ident()

					t1 = System.nanoTime()
					profiler.increment("Save - Primary Key 1", t1-t0)
					t0 = t1
				}
				catch (CassandraMappingNullIndexException e) {
					// if primary key is a UUID and its null, set it

					t1 = System.nanoTime()
					profiler.increment("Save - Primary Key 2", t1-t0)
					t0 = t1

					def keyNames = OrmHelper.collection(cassandraMapping.primaryKey ?: cassandraMapping.unindexedPrimaryKey)
					def keyClass = keyNames.size() == 1 ? clazz.getDeclaredField(keyNames[0]).type : null

					t1 = System.nanoTime()
					profiler.increment("Save - Primary Key 2/Check Class", t1-t0)
					t0 = t1

					if (keyClass == UUID) {
						def uuid = UUID.timeUUID()

						t1 = System.nanoTime()
						profiler.increment("Save - Primary Key 2/UUID", t1-t0)
						t0 = t1

						thisObj.setProperty(keyNames[0], uuid)
						id = thisObj.id
					}
					else {
						throw e
					}
					t1 = System.nanoTime()
					profiler.increment("Save - Primary Key 2/Remainder", t1-t0)
					t0 = t1
				}

				// see if it exists
				def oldObj = args?.nocheck ? null : clazz.get(id, [cluster:cluster])

				// TIMER
				t1 = System.nanoTime()
				profiler.initialIncrement("Save - Old Object", t1-t0, id)
				t0 = t1

				// one-to-one relationships
				def keyDeleted = false
				DataMapper.dirtyPropertyNames(thisObj).each {name ->

					// need to save object (if cascading) and remove the key column from cassandra if its
					// been set to null (uses dirty bit to know if it should be null since values are
					// lazy evaluated
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
							// TODO - UUID - move to separate column family
							if (!(dValue instanceof Boolean)) {
								def backLinkRowKey = KeyHelper.oneBackIndexRowKey(dValue.id)
								def backLinkColName = KeyHelper.oneBackIndexColumnName(persistence.columnFamilyName(thisObj.columnFamily), pName, id)
								persistence.deleteColumn(m, pValue.backLinkColumnFamily, backLinkRowKey, backLinkColName, '')
							}
						}
					}
					else {
						// back links
						// TODO - UUID - move to separate column family
						def backLinkRowKey = KeyHelper.oneBackIndexRowKey(pValue.id)
						def backLinkColName = KeyHelper.oneBackIndexColumnName(persistence.columnFamilyName(thisObj.columnFamily), pName, id)
						persistence.putColumn(m, pValue.backLinkColumnFamily, backLinkRowKey, backLinkColName, '')

						// cascade?
						if (args?.cascade) {
							pValue.save(cluster: thisObj.getProperty(CLUSTER_PROP))
						}
					}
				}

				// TIMER
				t1 = System.nanoTime()
				profiler.increment("Save - 1:1 Relationships", t1-t0)
				t0 = t1

				// commit deletion of relationship keys
				if (keyDeleted) {
					persistence.execute(m)
					m = persistence.prepareMutationBatch(ks, args?.consistencyLevel)
				}

				// TIMER
				t1 = System.nanoTime()
				profiler.increment("Save - Delete Old Keys", t1-t0)
				t0 = t1

				// manage index rows
				def indexRows = [:]
				def oldIndexRows = [:]
				def indexColumnFamily = thisObj.indexColumnFamily

				// primary key index
				if (cassandraMapping.primaryKey) {
					//indexRows[KeyHelper.primaryKeyIndexRowKey()] = [(thisObj.id):'']
					indexRows[KeyHelper.primaryKeyIndexRowKey()] = [(id):'']
				}

				// TIMER
				t1 = System.nanoTime()
				profiler.increment("Save - Primary Key Index", t1-t0)
				t0 = t1


				// explicit indexes
				IndexHelper.updateAllExplicitIndexes(oldObj, thisObj, cassandraMapping, oldIndexRows, indexRows)

				//TODO - put this into the above?
				oldIndexRows.each {rowKey, col ->
					col.each {colKey, v ->
						persistence.deleteColumn(m, indexColumnFamily, rowKey, colKey)
					}
				}

				// TIMER
				t1 = System.nanoTime()
				profiler.increment("Save - Explicit Index Generation", t1-t0)
				t0 = t1


				// delete old index row keys
				if (oldIndexRows) {
					persistence.execute(m)
					m = persistence.prepareMutationBatch(ks, args?.consistencyLevel)
				}

				// TIMER
				t1 = System.nanoTime()
				profiler.increment("Save - Explicit Delete Old", t1-t0)
				t0 = t1

				// insert this object
				def dataProperties = cassandra.mapping.dataProperties(thisObj)

				t1 = System.nanoTime()
				profiler.increment("Save - Convert Properties", t1-t0)
				t0 = t1

				persistence.putColumns(m, thisObj.columnFamily, id, dataProperties, ttl)

				// TIMER
				t1 = System.nanoTime()
				profiler.increment("Save - Save Object", t1-t0)
				t0 = t1


				// insert new index row keys
				if (indexRows) {
					indexRows.each {rowKey, cols ->
						persistence.putColumns(m, indexColumnFamily, rowKey, cols, ttl)
					}
				}

				// TIMER
				t1 = System.nanoTime()
				profiler.increment("Save - Explicit Index Setup", t1-t0)
				t0 = t1

				// counters
				CounterHelper.updateAllCounterColumns(persistence, counterColumnFamily, cassandraMapping.counters, m, oldObj, thisObj)

				// TIMER
				t1 = System.nanoTime()
				profiler.increment("Save - Counter Setup", t1-t0)
				t0 = t1

				persistence.execute(m)

				// TIMER
				t1 = System.nanoTime()
				profiler.finalIncrement("Save - Cassandra Save", t1-t0, id)
				t0 = t1
			}
			profiler.increment("Exits", 1L)
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
						try {
							def prop = PropertyUtils.getProperty(thisObj, name)
							if (prop != null && OrmHelper.isMappedObject(prop)) {
								def cName = "${pName}${KEY_SUFFIX}".toString()
								persistence.deleteColumn(m, thisObj.columnFamily, thisObj.id, cName)
								keyDeleted = true
							}
						}
						catch (NoSuchMethodException e) {
							// Skip it, if there is an expando
							if (!cassandraMapping.expandoMap) {
								throw e
							}
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
					def names = OrmHelper.collection(propName)
					if (!Collections.disjoint(propertyNames, names)) {
						def oldIndexRowKeys = KeyHelper.objectIndexRowKeys(propName, thisObj)
						oldIndexRowKeys.each {oldIndexRowKey ->
							oldIndexRows[oldIndexRowKey] = [(thisObj.id):'']
						}
					}
				}

				// decrement old counters
				// TODO - filter out those not in properties?
				CounterHelper.updateAllCounterColumns(persistence, counterColumnFamily, cassandraMapping.counters, m, thisObj, null)

				// set the new properties
				properties.each {name, value ->
					thisObj.setProperty(name, value)
				}

				// add new explicit indexes
				cassandraMapping.explicitIndexes?.each {propName ->
					def names = OrmHelper.collection(propName)
					if (!Collections.disjoint(propertyNames, names)) {
						def indexRowKeys = KeyHelper.objectIndexRowKeys(propName, thisObj)
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
					m = persistence.prepareMutationBatch(ks, consistencyLevel)
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
				CounterHelper.updateAllCounterColumns(persistence, counterColumnFamily, cassandraMapping.counters, m, null, thisObj)


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
			def maxCascade = args.max ?: MAX_ROWS
			def cascadedDeletes = []

			cassandra.withKeyspace(thisObj.keySpace, thisObj.cassandraCluster) {ks ->

				// delete the object
				def m = persistence.prepareMutationBatch(ks, args?.consistencyLevel)
				if (args.cascade) {

					// has one
					clazz.metaClass.properties.each {property ->
						def name = property.name
						if (name.endsWith(DIRTY_SUFFIX)) {
							def pName = name - DIRTY_SUFFIX
							def pValue = PropertyUtils.getProperty(thisObj, pName)
							if (pValue && pValue.belongsToClass(clazz)) {
								PropertyUtils.setProperty(pValue, pValue.belongsToPropName(clazz), null)
								cascadedDeletes << pValue
							}
						}
					}

					// has many
					if (clazz.metaClass.hasMetaProperty('hasMany')) {
						clazz.hasMany.each {propName, propClass ->
							def items = thisObj.invokeMethod(propName,[max: maxCascade])
							if (items) {
								if (items?.size() < maxCascade) {
									def pName = propClass.belongsToPropName(clazz)
									if (propClass.belongsToClass(clazz)) {
										items.each {
											OrmHelper.safeSetProperty(it, pName, null)
										}
										cascadedDeletes.addAll(items)
									}
									else {
										// indexes that don't belong-to, clean up indexes and back-pointers
										removeAllJoins(persistence, m, clazz, thisObj, propClass, items, propName)
									}
								}
								else {
									throw new CassandraMappingException("Cascaded delete failed because '${propName}' property potentially has more than ${maxCascade} values. Specify a larger max value.")
								}
							}
						}
					}
				}
				else {
					// not cascaded, but need to clean up indexes and back-pointers
					if (clazz.metaClass.hasMetaProperty('hasMany')) {
						clazz.hasMany.each {propName, propClass ->
							def items = thisObj.invokeMethod(propName,[max: maxCascade])
							if (items) {
								if (items?.size() < maxCascade) {
									// clean up indexes and back-pointers
									removeAllJoins(persistence, m, clazz, thisObj, propClass, items, propName)
								}
								else {
									throw new CassandraMappingException("Cascaded delete failed because '${propName}' property potentially has more than ${maxCascade} values. Specify a larger max value.")
								}
							}
						}
					}
				}

				// delete the object
				persistence.deleteRow(m, thisObj.columnFamily, thisObjId)

				// primary index
				if (cassandraMapping.primaryKey) {
					persistence.deleteColumn(m, indexColumnFamily, KeyHelper.primaryKeyIndexRowKey(), thisObjId)
				}

				// explicit indexes
				cassandraMapping.explicitIndexes?.each {propName ->
					def indexRowKeys = KeyHelper.objectIndexRowKeys(propName, thisObj)
					indexRowKeys.each {indexRowKey ->
						persistence.deleteColumn(m, indexColumnFamily, indexRowKey, thisObjId)
					}
				}

				// hasMany indexes
				def hasManyKey = KeyHelper.manyBackIndexRowKey(thisObjId)
				def manyBackLinkRow = persistence.getRow(ks, indexColumnFamily, hasManyKey, args?.consistencyLevel)
				manyBackLinkRow.each {col ->
					def manyIndexRowKey = persistence.name(col)
					persistence.deleteColumn(m, indexColumnFamily, manyIndexRowKey, thisObjId)
				}

				// hasMany back pointers
				persistence.deleteRow(m, indexColumnFamily, hasManyKey)

				// hasOne properties
				def oneBackLinkKey = KeyHelper.oneBackIndexRowKey(thisObjId)
				def oneBackLinkRow = persistence.getRow(ks, indexColumnFamily, oneBackLinkKey, args?.consistencyLevel)
				oneBackLinkRow.each {col ->
					def oneIndexArgs = KeyHelper.oneBackIndexColumnValues(persistence.name(col))
					def objectRowKey = oneIndexArgs[2]
					def objectColumnFamily = oneIndexArgs[0]
					def objectPropName = "${oneIndexArgs[1]}Id".toString()
					persistence.deleteColumn(m, objectColumnFamily, objectRowKey, objectPropName)
				}

				// hasOne back pointers
				persistence.deleteRow(m, indexColumnFamily, oneBackLinkKey)

				// decrement counters
				CounterHelper.updateAllCounterColumns(persistence, counterColumnFamily, cassandraMapping.counters, m, thisObj, null)

				persistence.execute(m)
			}

			cascadedDeletes.each {
				it.delete(cascade: true)
			}
		}

		// hasMany properties
		if (clazz.metaClass.hasMetaProperty('hasMany')) {

			clazz.hasMany.each {propName, propClass ->

				def getterName = GrailsClassUtils.getGetterName(propName)
				def setterName = GrailsClassUtils.getSetterName(propName)
				def counterFunctionName = "${propName}Count".toString()
				def addToName = OrmHelper.methodForPropertyName("addTo", propName)
				def removeFromName = OrmHelper.methodForPropertyName("removeFrom", propName)

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
							if (items.size() < MAX_ROWS) {
								PropertyUtils.setProperty(delegate, propName, items)
							}
							else {
								throw new CassandraMappingException("Query failed because '${propName}' property potentialy has more than default of ${MAX_ROWS} values, Use ${propName}(max:nnnn) function instead.")
							}
						}
					}
					return items
				}

				// getter function
				clazz.metaClass."${propName}" = { options ->
					def iClass = PropertyUtils.getPropertyType(delegate, propName)
					def oClass = iClass && List.isAssignableFrom(iClass) ? LinkedList : LinkedHashSet
					return getFromHasMany(delegate, propName, options, oClass)
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
					OrmHelper.safeSetProperty(thisObj, propName, null)

					// add the indexes
					cassandra.withKeyspace(delegate.keySpace, delegate.cassandraCluster) {ks ->
						def m = persistence.prepareMutationBatch(ks, consistencyLevel)

						// save join row from this object to the item
						saveJoinRow(cassandra.persistence, m, clazz, thisObj, item.class, item, propName)

						persistence.execute(m)
					}
					return item
				}

				// removeFrom...
				clazz.metaClass."${removeFromName}" = { item, consistencyLevel=null ->
					def thisObj = delegate
					def persistence = cassandra.persistence
					OrmHelper.safeSetProperty(thisObj, propName, null)

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
					return item
				}
			}
		}

		// for each property
		clazz.metaClass.properties.each {property ->
			if (property.type.metaClass.hasMetaProperty("cassandraMapping")) {

				def propName = property.name
				def getterName = GrailsClassUtils.getGetterName(propName)
				def setterName = GrailsClassUtils.getSetterName(propName)

				// save set of mapped properties.
				clazz.cassandraMapping.mappedProperties << propName

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
						def itemClass = property.type
						def cf = property.type.columnFamily

						// TODO - need to find a way to store this in the object!
						//def id = PropertyUtils.getProperty(delegate, "${propName}${KEY_SUFFIX}")

						cassandra.withKeyspace(keySpace, cassandraCluster) {ks ->
							def consistencyLevel = null //TODO - this OK?
							def colName = "${propName}${KEY_SUFFIX}".toString()
							def cols = persistence.getColumnSlice(ks, columnFamily, thisObj.id, [colName], consistencyLevel)
							def col = persistence.getColumn(cols, colName)
							if (col) {
								// TODO - UUID - needs to accomodate UUID values
								def pType = columnFamilyDataType(colName)
								def pid = pType in ["UUID","TimeUUID"] ? persistence.uuidValue(col) : persistence.stringValue(col)
								def data = persistence.getRow(ks, cf, pid, consistencyLevel)
							    value = cassandra.mapping.newObject(data, itemClass, delegate.getProperty(CLUSTER_PROP))
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
