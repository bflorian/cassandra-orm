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

	def getRow(Object client, Object columnFamily, Object rowKey, Object consistencyLevel)
	{
		logOp "getRow", columnFamily, rowKey
		data.getRowColumns(columnFamily, rowKey)
	}

	def getRows(Object client, Object columnFamily, Collection rowKeys, Object consistencyLevel)
	{
		logOp "getRows", columnFamily, rowKeys, consistencyLevel
		data.multigetRowColumns(columnFamily, rowKeys)
	}

	def getRowsColumnSlice(Object client, Object columnFamily, Collection rowKeys, Collection columnNames, Object consistencyLevel)
	{
		logOp "getRowsColumnSlice", columnFamily, rowKeys, columnNames, consistencyLevel
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

	def getRowsColumnRange(Object client, Object columnFamily, Collection rowKeys, Object start, Object finish, Boolean reversed, Integer max, Object consistencyLevel)
	{
		logOp "getRowsColumnRange", columnFamily, rowKeys, start, finish, reversed, max, consistencyLevel
		data.multigetRowColumnRange(columnFamily, rowKeys, start, finish, reversed, max)
	}

	def getRowsWithEqualityIndex(client, columnFamily, properties, max, Object consistencyLevel)
	{
		logOp "getRowsWithEqualityIndex", columnFamily, properties, max
		// TODO ??
		[]
	}

	def countRowsWithEqualityIndex(client, columnFamily, properties, Object consistencyLevel)
	{
		logOp "countRowsWithEqualityIndex", columnFamily, properties, consistencyLevel
		// TODO ??
		0
	}

	def getColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, Boolean reversed, Integer max, Object consistencyLevel)
	{
		logOp "getColumnRange", columnFamily, rowKey, start, finish, reversed, max, consistencyLevel
		data.getColumnRange(columnFamily, rowKey, start, finish, reversed, max)
	}

	def countColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, Object consistencyLevel)
	{
		logOp "countColumnRange", columnFamily, rowKey, start, finish, consistencyLevel
		data.getColumnRange(columnFamily, rowKey, start, finish, false, Integer.MAX_VALUE).size()
	}

	def getColumnSlice(Object client, Object columnFamily, Object rowKey, Collection columnNames, Object consistencyLevel)
	{
		logOp "getColumnSlice", columnFamily, rowKey, columnNames
		data.getColumnSlice(columnFamily, rowKey, columnNames)

	}

	def getColumn(Object client, Object columnFamily, Object rowKey, Object columnName, Object consistencyLevel)
	{
		logOp "getColumnSlice", columnFamily, rowKey, columnName, consistencyLevel
		def list = data.getColumnSlice(columnFamily, rowKey, [columnName])
		list ? list[0] : null
	}
	
	def prepareMutationBatch(client, Object consistencyLevel)
	{
		logOp "prepareMutationBatch", consistencyLevel
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

	void putColumn(mutationBatch, columnFamily, rowKey, name, value, ttl)
	{
		logOp "putColumn", columnFamily, rowKey, name, value, ttl
		data.putColumn(columnFamily, rowKey, name, value, ttl)
	}

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap)
	{
		logOp "putColumns", columnFamily, rowKey, columnMap
		data.putColumns(columnFamily, rowKey, columnMap)
	}

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap, ttlMap)
	{
		logOp "putColumns", columnFamily, rowKey, columnMap, ttlMap
		data.putColumns(columnFamily, rowKey, columnMap, ttlMap)
	}

	void incrementCounterColumn(mutationBatch, columnFamily, rowKey, name, value=1)
	{
		logOp "incrementCounterColumn", columnFamily, rowKey, name, value
		data.incrementCounterColumn(columnFamily, rowKey, name, value)
	}

	void incrementCounterColumns(mutationBatch, columnFamily, rowKey, columnMap)
	{
		logOp "incrementCounterColumns", columnFamily, rowKey, columnMap
		data.incrementCounterColumns(columnFamily, rowKey, columnMap)
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

	def getColumns(row)
	{
		row.value
	}

	def getColumn(row, name)
	{
		row.find{it.name == name}
	}

	def getColumnByIndex(row, index)
	{
		row[index]
	}

	def name(column)
	{
		column.name
	}

	String stringValue(column)
	{
		column.value
	}

	Long longValue(column)
	{
		column.value as Long
	}

	byte[] byteArrayValue(column)
	{
		def result = column.value?.bytes
		result
	}

	void logOp(String msg)
	{
		calls <<  [method: msg, args: [], message: msg]
	}

	void logOp(String method, Object... args)
	{
		def argStr = args.collect{it.toString()}.join(", ")
		def s = "$method(${argStr})"
		calls << [method: method, args: args, message: s]
	}
	
	void print(out=System.out)
	{
		calls.eachWithIndex {it, index ->
			out.println "${index}: ${it.message}"
		}
	}

	void printClear(out=System.out)
	{
		print out
		data.print(out)
		clear()
	}
	
	def calls = []
	
	void clear()
	{
		calls = []
	}

	def getFirstCall()
	{
		calls[0]
	}

	def getLastCall()
	{
		calls[-1]
	}

	def getCallHash()
	{
		MD5Codec.encode(calls.collect{it.toString()}.join("\n"))
	}
}
