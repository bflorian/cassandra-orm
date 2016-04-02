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

import com.reachlocal.grails.plugins.cassandra.utils.DataMapper
import com.reachlocal.grails.plugins.cassandra.utils.KeyHelper
import com.reachlocal.grails.plugins.cassandra.utils.OrmHelper
import com.reachlocal.grails.plugins.cassandra.utils.UtcDate
import groovy.util.logging.Commons

/**
 * @author: Bob Florian
 */

@Commons
class DataMapping extends MappingUtils
{
	def persistence

	def dataProperties(data)
	{
		if (data instanceof Map) {
			return DataMapper.dataProperties((Map<String, Object>)data)
		}
		else {
			def cassandraMapping = data.getClass().cassandraMapping
			List<String> transients = (List<String>)OrmHelper.safeGetProperty(data, 'transients', List, [])
			Map<String, Class> hasMany = (Map<String, Class>)OrmHelper.safeGetProperty(data, 'hasMany', Map, [:])
			String expandoMapName = cassandraMapping.expandoMap
			Collection<String> mappedProperties = cassandraMapping.mappedProperties
			return DataMapper.dataProperties(data, transients, hasMany, expandoMapName, mappedProperties)
		}
	}

	def makeResult(rows, Map options, Class clazz, Class collectionClass=LinkedHashSet)
	{
		def result = collectionClass.newInstance()
		if (options.columns) {
			rows.each{
				result << newColumnMap(it.columns, options.columns)
			}
		}
		else if (options.column) {
			rows.each {
				result << it.columns.getColumnByName(options.column)
			}
		}
		else {
			rows.each {
				result << newObject(it.key, it.columns, clazz, options.cluster)
			}
		}
		return result
	}

	def makeResult(keys, Object rows, Map options, Class clazz, Class collectionClass=LinkedHashSet)
	{
		def result = collectionClass.newInstance()
		if (options.columns) {
			keys.each {
				def row = persistence.getRow(rows, it)
				if (row.columns) {
					result << newColumnMap(row, options.columns)
				}
				else {
					log.error("Row not found for key '$it' of class $clazz")
				}
			}
		}
		else if (options.rawColumns) {
			keys.each {
				def row = persistence.getRow(rows, it)
				if (row.columns) {
					result << newRawColumnMap(row, options.rawColumns)
				}
				else {
					log.error("Row not found for key '$it' of class $clazz")
				}
			}
		}
		else if (options.column) {
			keys.each {
				def row = persistence.getRow(rows, it)
				if (row.columns) {
					result << persistence.stringValue(persistence.getColumn(row, options.column))
				}
				else {
					log.error("Row not found for key '$it' of class $clazz")
				}
			}
		}
		else if (options.rawColumn) {
			keys.each {
				def row = persistence.getRow(rows, it)
				if (row.columns) {
					result << persistence.byteArrayValue(persistence.getColumn(row, options.rawColumn))
				}
				else {
					log.error("Row not found for key '$it' of class $clazz")
				}
			}
		}
		else {
			keys.each {
				def row = persistence.getRow(rows, it)
				if (row?.columns) {
					result << newObject(it, row, clazz, options.cluster)
				}
				else {
					log.error("Row not found for key '$it' of class $clazz")
				}
			}
		}
		return result
	}

	def newColumnMap(cols, names)
	{
		def map = [:]
		names.each {
			map[it] = persistence.stringValue(persistence.getColumn(cols, it))
		}
		return map
	}

	def newRawColumnMap(cols, names)
	{
		def map = [:]
		names.each {
			map[it] = persistence.byteArrayValue(persistence.getColumn(cols, it))
		}
		return map
	}

	def newObject(key, cols, asClass, cluster=null)
	{
		def obj = null

		if (cols) {
			// Unneeded since we now get the class from the method signatures
			// Might be needed again if we ever support subclasses
			//def className = persistence.stringValue(persistence.getColumn(cols, CLASS_NAME_KEY))
			//def asClass = Class.forName(className, false, DataMapping.class.classLoader)
			obj = asClass.newInstance()

			def keyPropertyName = KeyHelper.identKeyProperty(asClass.cassandraMapping) // null for composite keys
			if (keyPropertyName) {
				def metaProperty = obj.metaClass.getMetaProperty(keyPropertyName)
				if (metaProperty) {
					metaProperty.setProperty(obj, key)
				}
			}

			def expandoMapName = asClass.cassandraMapping.expandoMap
			def expandoMap = null
			def expandoMapType = String
			if (expandoMapName) {
				expandoMap = obj.getProperty(expandoMapName)
				if (expandoMap == null) {
					expandoMap = [:] //TODO - map type?
					obj.setProperty(expandoMapName, expandoMap)
				}
			}

			cols.each() {col ->
				def name = col.name
				def metaProperty = obj.metaClass.getMetaProperty(name)
				if (metaProperty) {
					if (metaProperty.setter && !metaProperty.setter.isStatic()) {
						metaProperty.setProperty(obj, objectProperty(metaProperty.type, col))
					}
				}
				else if (expandoMap != null && name != CLASS_NAME_KEY && !(name.endsWith(KEY_SUFFIX) && isMappedProperty(asClass, name[0..(-KEY_SUFFIX.size()-1)]))) {
					expandoMap[name] = objectProperty(expandoMapType, col)
				}
			}
		}

		if (cluster && obj) {
			obj.setProperty(CLUSTER_PROP, cluster)
		}
		return obj
	}

	def objectProperty(Class clazz, column)
	{
		switch (clazz) {
			case Integer:
				return new Integer(persistence.stringValue(column))
			case Long:
				return new Long(persistence.stringValue(column))
			case Float:
				return new Float(persistence.stringValue(column))
			case Double:
				return new Double(persistence.stringValue(column))
			case BigDecimal:
				return new BigDecimal(persistence.stringValue(column))
			case BigInteger:
				return new BigInteger(persistence.stringValue(column))
			case Boolean:
				return column.booleanValue
			case Date:
				return UtcDate.isoFormatter().parse(persistence.stringValue(column))
			case String:
				return persistence.stringValue(column)
			case UUID:
				//return UUID.fromString(persistence.stringValue(column))
				return persistence.uuidValue(column)
			case byte[]:
				return persistence.byteArrayValue(column)
			default:
				if (Collection.isAssignableFrom(clazz) || Map.isAssignableFrom(clazz)) {
					return mapper.readValue(persistence.stringValue(column), clazz)
				}
				else if (clazz.isEnum()) {
					def value = persistence.stringValue(column)
					return clazz.values().find{it.toString() == value}
				}
		}
	}

	static protected final CLASS_NAME_KEY = DataMapper.CLASS_NAME_KEY
}
