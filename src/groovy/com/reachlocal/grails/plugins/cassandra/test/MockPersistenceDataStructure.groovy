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
class MockPersistenceDataStructure
{
	def data = [:]

	def getRowColumns(columnFamily, rowKey)
	{
		data.get(columnFamily)?.get(rowKey)?.collect{k,v -> [name: k, value: v]}
	}

	def multigetRowColumns(columnFamily, rowKeys)
	{
		def result = [:]
		rowKeys.each {rowKey ->
			result[rowKey] = getRowColumns(columnFamily, rowKey)
		}
		result
	}

	def multigetRowColumnRange(columnFamily, rowKeys, start, finish, reversed, max)
	{
		def result = [:]
		rowKeys.each {rowKey ->
			result[rowKey] = getColumnRange(columnFamily, rowKey, start, finish, reversed, max)
		}
		result
	}

	def getColumnRange(columnFamily, rowKey, start, finish, reversed, max)
	{
		def result = []
		def started = !start
		def finished = false
		def row = data.get(columnFamily)?.get(rowKey)
		if (row) {
			if (reversed) {
				def row2 = [:]
				row.collect{it.key}.sort().reverse().each {k ->
					row2[k] = row[k]
				}
				row = row2
			}
			else {
				def row2 = [:]
				row.collect{it.key}.sort().each {k ->
					row2[k] = row[k]
				}
				row = row2
			}
			row.each {k,v ->
				if (!started) {
					if ((reversed && k <= start) || (!reversed && k >= start)) {
						started = true
					}
				}

				if (started && !finished && result.size() < max) {
					result << [name:  k, value:  v]
				}

				if (finish && !finished) {
					if ((reversed && k < finish) || (!reversed && k > finish)) {
						finished = true
					}
				}
			}
		}
		return result
	}

	def getColumnSlice(columnFamily, rowKey, columnNames)
	{
		def result = []
		def row = data.get(columnFamily)?.get(rowKey)
		if (row) {
			row.each {k,v ->
				if (columnNames.contains(k)) {
					result << [name:  k, value:  v]
				}
			}
		}
		return result

	}

	void putColumn(columnFamily, rowKey, columnName, columnValue)
	{
		def cf = data[columnFamily] ?: [:]
		def row = cf[rowKey] ?: [:]
		row[columnName] = columnValue
		cf[rowKey] = row
		data[columnFamily] = cf
	}

	void putColumns(columnFamily, rowKey, columnMap)
	{
		def cf = data[columnFamily] ?: [:]
		def row = cf[rowKey] ?: [:]
		columnMap.each {k,v ->
			row[k] = v
		}
		cf[rowKey] = row
		data[columnFamily] = cf
	}

	void incrementCounterColumn(columnFamily, rowKey, columnName, value=1)
	{
		def cf = data[columnFamily] ?: [:]
		def row = cf[rowKey] ?: [:]
		row[columnName] = (row[columnName] ?: 0) + value
		cf[rowKey] = row
		data[columnFamily] = cf
	}

	void incrementCounterColumns(columnFamily, rowKey, columnMap)
	{
		def cf = data[columnFamily] ?: [:]
		def row = cf[rowKey] ?: [:]
		columnMap.each {k,v ->
			row[k] = (row[k] ?: 0) + v
		}
		cf[rowKey] = row
		data[columnFamily] = cf
	}

	def deleteRow(columnFamily, rowKey)
	{
		data.get(columnFamily)?.remove(rowKey)
	}

	def deleteColumn(columnFamily, rowKey, columnName)
	{
		data.get(columnFamily)?.get(rowKey)?.remove(columnName)
	}

	void print(out)
	{
		data.each {cfk, cfv ->
			println "${cfk} =>"
			cfv.each {rowk, rowv ->
				println "    ${rowk} => ${rowv}"
			}
		}
	}
}
