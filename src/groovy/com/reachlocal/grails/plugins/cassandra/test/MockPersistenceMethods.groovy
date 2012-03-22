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

import com.reachlocal.grails.plugins.cassandra.OrmPersistenceMethods

/**
 * @author: Bob Florian
 */
class MockPersistenceMethods implements OrmPersistenceMethods
{
	def columnFamily(String name)
	{
		log "columnFamily", name
		"${name}_CFO".toString()
	}

	def getRow(Object client, Object columnFamily, Object rowKey)
	{
		log "getRow", columnFamily, rowKey
		[[name: '_class_name_', value:'com.reachlocal.grails.plugins.cassandra.test.orm.User'],[name:'prop1', value: 'propVal1'],[name:'prop2', value: 'propVal2']]
		//[('_class_name_'):'com.reachlocal.grails.plugins.cassandra.test.orm.User', prop1: 'propVal1', prop2: 'propVal2']
	}

	def getRows(Object client, Object columnFamily, Collection rowKeys)
	{
		log "getRows", columnFamily, rowKeys
		[
				col1: [[name: '_class_name_', value:'com.reachlocal.grails.plugins.cassandra.test.orm.User'],[name:'name', value: 'Sally'],[name:'city', value: 'Olney']],
				col2: [[name: '_class_name_', value:'com.reachlocal.grails.plugins.cassandra.test.orm.User'],[name:'name', value: 'Sue'],[name:'city', value: 'Ellicott City']]
		]
	}

	def getRowsColumnSlice(Object client, Object columnFamily, Collection rowKeys, Collection columnNames)
	{
		log "getRowsColumnSlice", columnFamily, rowKeys, columnNames
		[]
	}

	def getRowsWithEqualityIndex(client, columnFamily, properties, max)
	{
		log "getRowsWithEqualityIndex", columnFamily, properties, max
		[]
	}

	def getColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, Boolean reversed, Integer max)
	{
		log "getColumnRange", columnFamily, rowKey, start, finish, reversed, max
		[[name: 'col1', value: 'val1'],[name: 'col2', value: 'val2']]
	}

	def getColumnSlice(Object client, Object columnFamily, Object rowKey, Collection columnNames)
	{
		log "getColumnSlice", columnFamily, rowKey, columnNames
		[]
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
		column.value?.bytes
	}

	void log(String method, Object... args)
	{
		def argStr = args.collect{it.toString()}.join(", ")
		calls << "$method(${argStr})"
	}
	
	void print(out=System.out)
	{
		calls.each {
			out.println it
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
