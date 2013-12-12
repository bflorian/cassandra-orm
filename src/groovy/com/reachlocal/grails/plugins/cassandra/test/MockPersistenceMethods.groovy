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
import com.reachlocal.grails.plugins.cassandra.mapping.PersistenceProvider

/**
 * @author: Bob Florian
 */
class MockPersistenceMethods implements PersistenceProvider
{
	Boolean dumpMutations = false
	MockPersistenceDataStructure data = new MockPersistenceDataStructure()

	void setCluster(cluster)
	{
		data.currentCluster = cluster
	}

	def columnTypes(Object client, String name)
	{
		[:]
	}

	def columnFamily(Object client, String name)
	{
		logOp "columnFamily", name
		"${name}_CFO".toString()
	}

	def columnFamilyName(columnFamily)
	{
		columnFamily[0..-4]
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
		logOp "getRowsWithEqualityIndex", columnFamily, properties, max, consistencyLevel
		// TODO ??
		[]
	}

	def countRowsWithEqualityIndex(client, columnFamily, properties, Object consistencyLevel)
	{
		logOp "countRowsWithEqualityIndex", columnFamily, properties, consistencyLevel
		// TODO ??
		0
	}

	def getRowsWithCqlWhereClause(client, columnFamily, clause, max, consistencyLevel)
	{
		logOp "getRowsWithCqlWhereClause", columnFamily, clause, max, consistencyLevel
		// TODO ??
		[]
	}

	def getRowsColumnSliceWithCqlWhereClause(client, columnFamily, clause, max, columns, consistencyLevel)
	{
		logOp "getRowsColumnSliceWithCqlWhereClause", columnFamily, clause, max, columns, consistencyLevel
		// TODO ??
		[]
	}

	def countRowsWithCqlWhereClause(client, columnFamily, clause, consistencyLevel)
	{
		logOp "countRowsWithCqlWhereClause", columnFamily, properties, consistencyLevel
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

		if (dumpMutations) {
			mutationBatch = []
		}
	}

	void deleteColumn(mutationBatch, columnFamily, rowKey, columnName)
	{
		logOp "deleteColumn", columnFamily, rowKey, columnName
		data.deleteColumn(columnFamily, rowKey, columnName)

		if (dumpMutations) {
			mutationBatch << "mutationBatch.withRow('$columnFamily', '$rowKey').deleteColumn('$columnName')"
		}
	}

	void putColumn(mutationBatch, columnFamily, rowKey, name, value)
	{
		logOp "putColumn", columnFamily, rowKey, name, value
		data.putColumn(columnFamily, rowKey, name, value)

		if (dumpMutations) {
			mutationBatch << "mutationBatch.withRow('$columnFamily', '$rowKey').putColumn('$name', '$value')"
		}
	}

	void putColumn(mutationBatch, columnFamily, rowKey, name, value, ttl)
	{
		logOp "putColumn", columnFamily, rowKey, name, value, ttl
		data.putColumn(columnFamily, rowKey, name, value, ttl)

		if (dumpMutations) {
			mutationBatch << "mutationBatch.withRow('$columnFamily', '$rowKey').putColumn('$name', '$value', $ttl)"
		}
	}

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap)
	{
		logOp "putColumns", columnFamily, rowKey, columnMap
		data.putColumns(columnFamily, rowKey, columnMap)

		if (dumpMutations) {
			def sb = new StringBuilder("mutationBatch.withRow('$columnFamily', '$rowKey')")
			columnMap.each {k,v ->
				if (v != null) {
					sb << "\n      .putColumn('$k', '$v')"
				}
				else {
					sb << "\n      .deleteColumn('$k')"
				}
			}
			mutationBatch << sb.toString()
		}
	}

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap, ttlMap)
	{
		logOp "putColumns", columnFamily, rowKey, columnMap, ttlMap
		data.putColumns(columnFamily, rowKey, columnMap, ttlMap)

		if (dumpMutations) {
			def sb = new StringBuilder("mutationBatch.withRow('$columnFamily', '$rowKey')")
			if (ttlMap instanceof Number) {
				columnMap.each {k, v ->
					if (v != null) {
						sb << "\n      .putColumn('$k', '$v', $ttlMap)"
					}
					else {
						sb << "\n      .deleteColumn('$k')"
					}
				}
			}
			else {
				columnMap.each {k, v ->
					if (v != null) {
						sb << "\n      .putColumn('$k', '$v', ${ttlMap[k]})"
					}
					else {
						sb << "\n      .deleteColumn('$k')"
					}
				}
			}
			mutationBatch << sb.toString()
		}
	}

	void incrementCounterColumn(Object mutationBatch, Object columnFamily, Object rowKey, Object name)
	{
		incrementCounterColumn(mutationBatch, columnFamily, rowKey, name, 1)
	}

	void incrementCounterColumn(Object mutationBatch, Object columnFamily, Object rowKey, Object name, Long value)
	{
		logOp "incrementCounterColumn", columnFamily, rowKey, name, value
		data.incrementCounterColumn(columnFamily, rowKey, name, value)

		if (dumpMutations) {
			mutationBatch << "mutationBatch.withRow('$columnFamily', '$rowKey').incrementCounterColumn('$name', $value)"
		}
	}

	void incrementCounterColumns(mutationBatch, columnFamily, rowKey, columnMap)
	{
		logOp "incrementCounterColumns", columnFamily, rowKey, columnMap
		data.incrementCounterColumns(columnFamily, rowKey, columnMap)

		def sb = new StringBuilder("mutationBatch.withRow('$columnFamily', '$rowKey')")
		columnMap.each {k,v ->
			sb << "\n      .incrementCounterColumn('$k', $v)"
		}
		if (dumpMutations) {
			mutationBatch << sb.toString()
		}
	}

	void deleteRow(mutationBatch, columnFamily, rowKey)
	{
		logOp "deleteRow", columnFamily, rowKey
		data.deleteRow(columnFamily, rowKey)
	}

	def execute(mutationBatch)
	{
		logOp "execute"

		if (dumpMutations) {
			println "def mutationBatch = ks.prepareMutationBatch()"
			mutationBatch.each {
				println it
			}
			println "mutationBatch.execute()"
		}

	}

	def getRow(rows, key)
	{
		rows[key]
	}

    Iterable getColumns(row)
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
		def argStr = args.collect{"\"${it}\""}.join(", ")
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

	def mutationBatch
}
