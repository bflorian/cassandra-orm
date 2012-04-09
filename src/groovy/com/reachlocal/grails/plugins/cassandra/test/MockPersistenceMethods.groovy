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

import org.codehaus.groovy.grails.plugins.codecs.MD5Codec

/**
 * @author: Bob Florian
 */
class MockPersistenceMethods
{
	MockPersistenceDataStructure data = new MockPersistenceDataStructure()

	def columnFamily(String name)
	{
		logOp "columnFamily", name
		"${name}_CFO".toString()
	}

	def getRow(Object client, Object columnFamily, Object rowKey)
	{
		logOp "getRow", columnFamily, rowKey
		//[[name: '_class_name_', value: CLASSES[columnFamily]],[name:'prop1', value: 'propVal1'],[name:'prop2', value: 'propVal2']]
		//[('_class_name_'):'com.reachlocal.grails.plugins.cassandra.test.orm.User', name: 'Sally', city: 'Olney']
		data.getRowColumns(columnFamily, rowKey)
	}

	def getRows(Object client, Object columnFamily, Collection rowKeys)
	{
		logOp "getRows", columnFamily, rowKeys
		//[
		//		col1: [[name: '_class_name_', value: CLASSES[columnFamily]],[name:'name', value: 'Sally'],[name:'city', value: 'Olney'],[name:'uuid', value: 'user-xxxx-1']],
		//		col2: [[name: '_class_name_', value: CLASSES[columnFamily]],[name:'name', value: 'Sue'],[name:'city', value: 'Ellicott City'],[name:'uuid', value: 'user-xxxx-2']]
		//]
		data.multigetRowColumns(columnFamily, rowKeys)
	}

	def getRowsColumnSlice(Object client, Object columnFamily, Collection rowKeys, Collection columnNames)
	{
		logOp "getRowsColumnSlice", columnFamily, rowKeys, columnNames
		//def rows = [
		//		col1: [[name: '_class_name_', value: CLASSES[columnFamily]],[name:'name', value: 'Sally'],[name:'city', value: 'Olney'],[name:'uuid', value: 'user-xxxx-1']],
		//		col2: [[name: '_class_name_', value: CLASSES[columnFamily]],[name:'name', value: 'Sue'],[name:'city', value: 'Ellicott City'],[name:'uuid', value: 'user-xxxx-2']]
		//]
		def rows = data.multigetRowColumns(columnFamily, rowKeys)
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
		logOp "getRowsWithEqualityIndex", columnFamily, properties, max
		// TODO ??
		[]
	}

	def countRowsWithEqualityIndex(client, columnFamily, properties)
	{
		logOp "countRowsWithEqualityIndex", columnFamily, properties
		// TODO ??
		0
	}

	def getColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, Boolean reversed, Integer max)
	{
		logOp "getColumnRange", columnFamily, rowKey, start, finish, reversed, max
		//[[name: 'col1', value: 'val1'],[name: 'col2', value: 'val2']]
		data.getColumnRange(columnFamily, rowKey, start, finish, reversed, max)
	}

	def countColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish)
	{
		logOp "countColumnRange", columnFamily, rowKey, start, finish
		data.getColumnRange(columnFamily, rowKey, start, finish, false, Integer.MAX_VALUE).size()
	}

	def getColumnSlice(Object client, Object columnFamily, Object rowKey, Collection columnNames)
	{
		logOp "getColumnSlice", columnFamily, rowKey, columnNames
		//def map = [('_class_name_'):'com.reachlocal.grails.plugins.cassandra.test.orm.User', name: 'Sally', city: 'Olney', userGroup_key: 'group1-zzzz-zzzz']
		//def result = []
		//columnNames.each {
		//	def v = map[it]
		//	if (v) {
		//		result << [name: it, value:  v]
		//	}
		//}
		data.getColumnSlice(columnFamily, rowKey, columnNames)

	}

	def getColumn(Object client, Object columnFamily, Object rowKey, Object columnName)
	{
		logOp "getColumnSlice", columnFamily, rowKey, columnName
		//[('_class_name_'):'com.reachlocal.grails.plugins.cassandra.test.orm.User', name: 'Sally', city: 'Olney', userGroup_key: 'group1-zzzz-zzzz'][columnName]
		def list = data.getColumnSlice(columnFamily, rowKey, [columnName])
		list ? list[0] : null
	}
	
	def prepareMutationBatch(client)
	{
		logOp "prepareMutationBatch"
		"mutation"
	}

	void deleteColumn(mutationBatch, columnFamily, rowKey, columnName)
	{
		logOp "deleteColumn", columnFamily, rowKey, columnName
		data.deleteColumn(columnFamily, rowKey, columnName)
	}

	void putColumn(mutationBatch, columnFamily, rowKey, name, value)
	{
		logOp "putColumn", columnFamily, rowKey, name, value
		data.putColumn(columnFamily, rowKey, name, value)
	}

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap)
	{
		logOp "putColumns", columnFamily, rowKey, columnMap
		data.putColumns(columnFamily, rowKey, columnMap)
	}

	void deleteRow(mutationBatch, columnFamily, rowKey)
	{
		logOp "deleteRow", columnFamily, rowKey
		data.deleteRow(columnFamily, rowKey)
	}

	def execute(mutationBatch)
	{
		logOp "execute"
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

	void logOp(String msg)
	{
		calls << msg
	}

	void logOp(String method, Object... args)
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
		data.print(out)
		//out.println MD5Codec.encode(calls.collect{it.toString()}.join("\n"))
		clear()
	}
	
	def calls = []
	
	void clear()
	{
		calls = []
	}
}
