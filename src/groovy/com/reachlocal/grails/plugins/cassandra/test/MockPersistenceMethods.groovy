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

package com.reachlocal.grails.plugins.cassandra.test

/**
 * @author: Bob Florian
 */
class MockPersistenceMethods
{
	def CLASSES = [
	        MockUser_CFO: 'com.reachlocal.grails.plugins.cassandra.test.orm.User',
			UserGroup_CFO: 'com.reachlocal.grails.plugins.cassandra.test.orm.UserGroup'
	]

	def columnFamily(String name)
	{
		log "columnFamily", name
		"${name}_CFO".toString()
	}

	def getRow(Object client, Object columnFamily, Object rowKey)
	{
		log "getRow", columnFamily, rowKey
		[[name: '_class_name_', value: CLASSES[columnFamily]],[name:'prop1', value: 'propVal1'],[name:'prop2', value: 'propVal2']]
		//[('_class_name_'):'com.reachlocal.grails.plugins.cassandra.test.orm.User', name: 'Sally', city: 'Olney']
	}

	def getRows(Object client, Object columnFamily, Collection rowKeys)
	{
		log "getRows", columnFamily, rowKeys
		[
				col1: [[name: '_class_name_', value: CLASSES[columnFamily]],[name:'name', value: 'Sally'],[name:'city', value: 'Olney']],
				col2: [[name: '_class_name_', value: CLASSES[columnFamily]],[name:'name', value: 'Sue'],[name:'city', value: 'Ellicott City']]
		]
	}

	def getRowsColumnSlice(Object client, Object columnFamily, Collection rowKeys, Collection columnNames)
	{
		log "getRowsColumnSlice", columnFamily, rowKeys, columnNames
		def rows = [
				col1: [[name: '_class_name_', value: CLASSES[columnFamily]],[name:'name', value: 'Sally'],[name:'city', value: 'Olney']],
				col2: [[name: '_class_name_', value: CLASSES[columnFamily]],[name:'name', value: 'Sue'],[name:'city', value: 'Ellicott City']]
		]
		def result = [:]
		rows.each {key, row ->
			def values = []
			columnNames.each {name ->
				values << [name: name, value: row.find{it.name == name}?.value]
			}
			result[key] = values
		}
		return result
	}

	def getRowsWithEqualityIndex(client, columnFamily, properties, max)
	{
		log "getRowsWithEqualityIndex", columnFamily, properties, max
		[]
	}

	def countRowsWithEqualityIndex(client, columnFamily, properties)
	{
		log "countRowsWithEqualityIndex", columnFamily, properties
		0
	}

	def getColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, Boolean reversed, Integer max)
	{
		log "getColumnRange", columnFamily, rowKey, start, finish, reversed, max
		[[name: 'col1', value: 'val1'],[name: 'col2', value: 'val2']]
	}

	def countColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish)
	{
		log "countColumnRange", columnFamily, rowKey, start, finish
		0
	}

	def getColumnSlice(Object client, Object columnFamily, Object rowKey, Collection columnNames)
	{
		log "getColumnSlice", columnFamily, rowKey, columnNames
		def map = [('_class_name_'):'com.reachlocal.grails.plugins.cassandra.test.orm.User', name: 'Sally', city: 'Olney', userGroup_key: 'group1-zzzz-zzzz']
		def result = []
		columnNames.each {
			def v = map[it]
			if (v) {
				result << [name: it, value:  v]
			}
		}
		result
	}

	def getColumn(Object client, Object columnFamily, Object rowKey, Object columnName)
	{
		log "getColumnSlice", columnFamily, rowKey, columnName
		[('_class_name_'):'com.reachlocal.grails.plugins.cassandra.test.orm.User', name: 'Sally', city: 'Olney', userGroup_key: 'group1-zzzz-zzzz'][columnName]
	}
	
	def prepareMutationBatch(client)
	{
		log "prepareMutationBatch"
		"mutation"
	}

	void deleteColumn(mutationBatch, columnFamily, rowKey, columnName)
	{
		log "deleteColumn", columnFamily, rowKey, columnName
	}

	void putColumn(mutationBatch, columnFamily, rowKey, name, value)
	{
		log "putColumn", columnFamily, rowKey, name, value
	}

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap)
	{
		log "putColumns", columnFamily, rowKey, columnMap
	}

	void deleteRow(mutationBatch, columnFamily, rowKey)
	{
		log "deleteRow", columnFamily, rowKey
	}

	def execute(mutationBatch)
	{
		log "execute"
	}

	def getRow(rows, key)
	{
		rows[key]
	}

	def getColumn(row, name)
	{
		row.find{it.name == name}
	}

	def name(column)
	{
		column.name
	}

	String stringValue(column)
	{
		column.value
	}

	byte[] byteArrayValue(column)
	{
		def result = column.value?.bytes
		result
	}

	void log(String method, Object... args)
	{
		def argStr = args.collect{it.toString()}.join(", ")
		def s = "$method(${argStr})"
		//println s
		calls << s
	}
	
	void print(out=System.out)
	{
		calls.eachWithIndex {it, index ->
			out.println "${index}: ${it}"
		}
	}

	void printClear(out=System.out)
	{
		print out
		clear()
	}
	
	def calls = []
	
	void clear()
	{
		calls = []
	}
}
