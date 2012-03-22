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

package com.reachlocal.grails.plugins.cassandra

/**
 * @author: Bob Florian
 */
interface OrmPersistenceMethods
{
	def columnFamily(String name);

	def getRow(Object client, Object columnFamily, Object rowKey);

	def getRows(Object client, Object columnFamily, Collection rowKeys);

	def getRowsColumnSlice(Object client, Object columnFamily, Collection rowKeys, Collection columnNames);

	def getRowsWithEqualityIndex(client, columnFamily, properties, max);

	def getColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, Boolean reversed, Integer max);

	def getColumnSlice(Object client, Object columnFamily, Object rowKey, Collection columnNames);

	def getColumn(Object client, Object columnFamily, Object rowKey, Object columnName);

	def prepareMutationBatch(client);

	void deleteColumn(mutationBatch, columnFamily, rowKey, columnName);

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap);

	void deleteRow(mutationBatch, columnFamily, rowKey);

	def execute(mutationBatch);

	def getRow(rows, key);

	def getColumn(row, name);

	def name(column);

	String stringValue(column);

	byte[] byteArrayValue(column);
}

