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
	static final KEY_SUFFIX = "_key"
	static final DIRTY_SUFFIX = "_dirty"
	static final int MAX_ROWS = 1000

	static void addDynamicOrmMethods(clazz, ctx)
	{
		// cassandra
		clazz.metaClass.getCassandra = { clazz.cassandra }

		// keySpace
		clazz.metaClass.getKeySpace = { clazz.keySpace }

		// columnFamily()
		clazz.metaClass.getColumnFamily = { clazz.columnFamily }

		// indexColumnFamily()
		clazz.metaClass.getIndexColumnFamily = { clazz.indexColumnFamily }

		// cassandraKey
		clazz.metaClass.getId = {
			def thisObj = delegate
			def result = makeComposite(collection(cassandraMapping.primaryKey).collect {
				def value = thisObj.getProperty(it)
				primaryRowKey(value)
			})

			return result
		}

		// save()
		clazz.metaClass.save = {args ->
			def thisObj = delegate
			cassandra.withKeyspace(thisObj.keySpace) {ks ->
				def m = cassandra.persistence.prepareMutationBatch(ks)

				// see if it exists
				def id = thisObj.id
				def oldObj = clazz.get(id)

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
								cassandra.persistence.deleteColumn(m, thisObj.columnFamily, thisObj.id, cName)
								keyDeleted = true
							}
						}
						else {
							if (args?.cascade) {
								pValue.save()
							}
						}
					}
				}

				// commit deletion of relationship keys
				if (keyDeleted) {
					cassandra.persistence.execute(m)
					m = cassandra.persistence.prepareMutationBatch(ks)
				}

				// insert this object
				def dataProperties = cassandra.mapping.dataProperties(thisObj)
				cassandra.persistence.putColumns(m, thisObj.columnFamily, id, dataProperties)

				// explicit indexes
				def indexRows = [:]
				def oldIndexRows = [:]
				def indexColumnFamily = thisObj.indexColumnFamily

				if (!cassandraMapping.noPrimaryKeyIndex) {
					indexRows[primaryKeyIndexRowKey()] = [(thisObj.id):'']
				}

				cassandraMapping.explicitIndexes?.each {propName ->
					if (oldObj) {

						// TODO - skip if null?
						def oldIndexRowKey = objectIndexRowKey(propName, oldObj)
						oldIndexRows[oldIndexRowKey] = [(oldObj.id):'']
					}

					// TODO - skip if null?
					def indexRowKey = objectIndexRowKey(propName, thisObj)
					indexRows[indexRowKey] = [(thisObj.id):'']
				}
				oldIndexRows.each {rowKey, col ->
					col.each {colKey, v ->
						cassandra.persistence.deleteColumn(m, indexColumnFamily, rowKey, colKey)
					}
				}
				if (indexRows) {
					indexRows.each {rowKey, cols ->
						cassandra.persistence.putColumns(m, indexColumnFamily, rowKey, cols)
					}
				}
				cassandra.persistence.execute(m)
			}

				// has many cascade
			if (args?.cascade && clazz.metaClass.hasMetaProperty('hasMany')) {
				hasMany.each {propName, propClass ->

					def items = thisObj.getProperty(propName)
					items?.each {item ->

						thisObj.cassandra.withKeyspace(thisObj.keySpace) {ks ->
							def m = cassandra.persistence.prepareMutationBatch(ks)

							saveJoinRow(cassandra.persistence, m, clazz, thisObj, propClass, item, propName)

							safeGetStaticProperty(item.class, "hasMany")?.each {name1, clazz1 ->
								if (clazz1 == clazz) {
									saveJoinRow(cassandra.persistence, m, propClass, item, clazz, thisObj, name1)
								}
							}
							safeGetStaticProperty(item.class, "belongsTo")?.each {name1, clazz1 ->
								if (clazz1 == clazz) {
									item.setProperty(name1, thisObj)
								}
							}
							cassandra.persistence.execute(m)
						}
						// TODO - how to cascade and not have recursive loop when many-to-many
						//item.save(cascade:true)
						item.save()
					}
				}
			}
		}

		// delete()
		clazz.metaClass.delete = {
			def thisObj = delegate
			cassandra.withKeyspace(thisObj.keySpace) {ks ->
				def m = cassandra.persistence.prepareMutationBatch(ks)
				cassandra.persistence.deleteRow(m, thisObj.columnFamily, thisObj.id)

				if (clazz.metaClass.hasMetaProperty('hasMany')) {
					hasMany.each {propName, propClass ->
						def joinColumnFamily = propClass.indexColumnFamily
						cassandra.persistence.deleteRow(m, joinColumnFamily, thisObj.id)
						if (propClass.belongsToClass(thisObj.class)) {
							def items = thisObj.getProperty(propName)
							items?.each {item ->
								cassandra.persistence.deleteRow(m, item.columnFamily, item.id)
							}
						}
					}
				}
				cassandra.persistence.execute(m)
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
					def items = PropertyUtils.getProperty(delegate, propName)
					if (items == null) {
						items = getFromHasMany(delegate, propName)
						PropertyUtils.setProperty(delegate, propName, items)
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
				clazz.metaClass."${addToName}" = { item ->
					def thisObj = delegate
					def items = PropertyUtils.getProperty(thisObj, propName)?: []
					items << item
					PropertyUtils.setProperty(thisObj, propName, items)

					cassandra.withKeyspace(delegate.keySpace) {ks ->
						def m = cassandra.persistence.prepareMutationBatch(ks)

						// save join row from this object to the item
						saveJoinRow(cassandra.persistence, m, clazz, thisObj, item.class, item, propName)

						// save the join row from the item to this object, if there is one
						safeGetStaticProperty(item.class, "hasMany")?.each {name1, clazz1 ->
							if (clazz1 == clazz) {
								saveJoinRow(cassandra.persistence, m, item.class, item, clazz, thisObj, name1)
							}
						}

						// set belongsTo value
						safeGetStaticProperty(item.class, "belongsTo")?.each {name1, clazz1 ->
							if (clazz1 == clazz) {
								item.setProperty(name1, thisObj)
							}
						}
						cassandra.persistence.execute(m)
					}
					item.save()
				}

				// removeFrom...
				clazz.metaClass."${removeFromName}" = { item ->
					def thisObj = delegate
					def items = PropertyUtils.getProperty(thisObj, propName)?: []
					def listItem = items.find{it.id == item.id}
					def persistence = cassandra.persistence
					if (listItem) {
						items.remove(listItem)
						PropertyUtils.setProperty(thisObj, propName, items)

						cassandra.withKeyspace(delegate.keySpace) {ks ->
							def m = persistence.prepareMutationBatch(ks)

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
					}
					else {
						// TODO - throw exception if specified item not in list?
					}
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
					PropertyUtils.setProperty(delegate, propName, it)
					delegate.setProperty("${propName}${DIRTY_SUFFIX}", true)
				}

				// getter
				clazz.metaClass."${getterName}" = {
					def value = PropertyUtils.getProperty(delegate, propName)
					if (value == null) {
						def thisObj = delegate
						def cf = property.type.columnFamily
						// TODO - need to find a way to store this in the object!
						//def id = PropertyUtils.getProperty(delegate, "${propName}${KEY_SUFFIX}")
						cassandra.withKeyspace(keySpace) {ks ->
							def colName = "${propName}${KEY_SUFFIX}".toString()
							def cols = cassandra.persistence.getColumnSlice(ks, columnFamily, thisObj.id, [colName])
							def col = cassandra.persistence.getColumn(cols, colName)
							if (col) {
								def pid = cassandra.persistence.stringValue(col)
								def data = cassandra.persistence.getRow(ks, cf, pid)
							    value = cassandra.mapping.newObject(data)
							}
						}
					}
					PropertyUtils.setProperty(delegate, propName, value)
					return value
				}
			}
		}

		// toString()
		clazz.metaClass.toString = {
			"${delegate.class.name.split('\\.')[-1]}(${delegate.id})"
		}
	}

}
