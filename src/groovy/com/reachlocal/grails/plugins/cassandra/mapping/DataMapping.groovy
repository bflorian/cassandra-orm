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

import grails.converters.JSON
import java.nio.ByteBuffer

/**
 * @author: Bob Florian
 */
class DataMapping 
{
	static final CLASS_NAME_KEY = '_class_name_'
	static final KEY_SUFFIX = InstanceMethods.KEY_SUFFIX
	static final DIRTY_SUFFIX = InstanceMethods.DIRTY_SUFFIX
	static final GLOBAL_TRANSIENTS = ["class","id","cassandra","indexColumnFamily","columnFamily","metaClass","keySpace"] as Set

	def persistence

	def dataProperties(data)
	{
		def map = [:]
		if (data instanceof Map) {
			data.each() {name, value ->
				if (value != null) {
					map[name] = dataProperty(value)
				}
			}
		}
		else {
			def transients = MappingUtils.safeGetProperty(data, 'transients', List, [])
			def hasMany = MappingUtils.safeGetProperty(data, 'hasMany', Map, [:])
			def clazz = data.getClass()
			map[CLASS_NAME_KEY] = clazz.getName()
			data.metaClass.properties.each() {
				if (!it.name.endsWith(DIRTY_SUFFIX)) {

					def prop = data.getProperty(it.name)
					if (prop != null &&
							it.getter &&
							!it.getter.isStatic() &&
							!transients.contains(it.name) &&
							!GLOBAL_TRANSIENTS.contains(it.name) &&
							!hasMany[it.name])
					{
						println it.name
						if (MappingUtils.isMappedClass(prop.class)) {
							map["${it.name}${KEY_SUFFIX}"] = prop.id
						}
						else {
							def value = dataProperty(prop)
							if (value != null) {
								map[it.name] = value
							}
						}
					}
				}
			}
		}
		return map
	}

	def makeResult(rows, options)
	{
		if (options.columns) {
			rows.collect{newColumnMap(it.columns, options.columns)}
		}
		else if (options.column) {
			rows.collect{it.columns.getColumnByName(options.column)}
		}
		else {
			rows.collect{newObject(it.columns)}
		}
	}

	def makeResult(keys, rows, options)
	{
		if (options.columns) {
			keys.collect{newColumnMap(persistence.getRow(rows, it), options.columns)}
		}
		else if (options.rawColumns) {
			keys.collect{newRawColumnMap(persistence.getRow(rows, it), options.rawColumns)}
		}
		else if (options.column) {
			keys.collect{persistence.stringValue(persistence.getColumn(persistence.getRow(rows, it), options.column))}
		}
		else if (options.rawColumn) {
			keys.collect{persistence.byteArrayValue(persistence.getColumn(persistence.getRow(rows, it), options.rawColumn))}
		}
		else {
			keys.collect{newObject(persistence.getRow(rows, it))}
		}
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

	def newObject(cols)
	{
		def obj = null
		if (cols) {
			def className = persistence.stringValue(persistence.getColumn(cols, CLASS_NAME_KEY))
			def asClass = Class.forName(className, false, DataMapping.class.classLoader)
			obj = asClass.newInstance()

			cols.each() {col ->
				def name = col.name
				def metaProperty = obj.metaClass.getMetaProperty(name)
				if (metaProperty) {
					if (metaProperty.setter && !metaProperty.setter.isStatic()) {
						metaProperty.setProperty(obj, objectProperty(metaProperty.type, col))
					}
				}
			}
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
				return new Date(new Long(persistence.stringValue(column)))
			case String:
				return persistence.stringValue(column)
			case UUID:
				return UUID.fromString(persistence.stringValue(column))
			case byte[]:
				return persistence.byteArrayValue(column)
			default:
				if (Collection.isAssignableFrom(clazz) || Map.isAssignableFrom(clazz)) {
					return JSON.parse(persistence.stringValue(column))
				}
				else if (clazz.isEnum()) {
					clazz.values().each() {
						if (it.toString() == persistence.stringValue(column)) {
							return persistence.stringValue(column)
						}
					}
					return null
				}
		}
	}

	def dataProperty(Date value)
	{
		return value.time.toString()
	}

	def dataProperty(Integer value)
	{
		return value.toString()
	}

	def dataProperty(Long value)
	{
		return value.toString()
	}

	def dataProperty(Double value)
	{
		return value.toString()
	}

	def dataProperty(BigDecimal value)
	{
		return value.toString()
	}

	def dataProperty(BigInteger value)
	{
		return value.toString()
	}

	def dataProperty(UUID value)
	{
		return value.toString()
	}

	def dataProperty(Collection value)
	{
		return value.encodeAsJSON()
	}

	def dataProperty(Map value)
	{
		return value.encodeAsJSON()
	}

	def dataProperty(String value)
	{
		return value
	}

	def dataProperty(byte[] value)
	{
		return value

	}

	def dataProperty(ByteBuffer value)
	{
		return value
	}

	def dataProperty(Class value)
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
