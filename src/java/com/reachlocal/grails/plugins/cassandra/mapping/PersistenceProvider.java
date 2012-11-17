package com.reachlocal.grails.plugins.cassandra.mapping;

import java.util.Collection;

/**
 * @author: Bob Florian
 */
public interface PersistenceProvider
{
	/*
	void setCluster(cluster);

	Object columnFamily(String name);

	String columnFamilyName(columnFamily);

	Object getRow(Object client, Object columnFamily, Object rowKey, Object consistencyLevel);

	Object getRows(Object client, Object columnFamily, Collection rowKeys, Object consistencyLevel);

	Object getRowsColumnSlice(Object client, Object columnFamily, Collection rowKeys, Collection columnNames, Object consistencyLevel);

	Object getRowsColumnRange(Object client, Object columnFamily, Collection rowKeys, Object start, Object finish, Boolean reversed, Integer max, Object consistencyLevel);

	Object getRowsWithEqualityIndex(client, columnFamily, properties, max, Object consistencyLevel);

	Object countRowsWithEqualityIndex(client, columnFamily, properties, Object consistencyLevel);

	Object getRowsWithCqlWhereClause(client, columnFamily, clause, max, consistencyLevel);

	Object getRowsColumnSliceWithCqlWhereClause(client, columnFamily, clause, max, columns, consistencyLevel);

	Object countRowsWithCqlWhereClause(client, columnFamily, clause, consistencyLevel);

	Object getColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, Boolean reversed, Integer max, Object consistencyLevel);

	Object countColumnRange(Object client, Object columnFamily, Object rowKey, Object start, Object finish, Object consistencyLevel);

	Object getColumnSlice(Object client, Object columnFamily, Object rowKey, Collection columnNames, Object consistencyLevel);

	Object getColumn(Object client, Object columnFamily, Object rowKey, Object columnName, Object consistencyLevel);

	Object prepareMutationBatch(client, Object consistencyLevel);

	void deleteColumn(mutationBatch, columnFamily, rowKey, columnName);

	void putColumn(mutationBatch, columnFamily, rowKey, name, value);

	void putColumn(mutationBatch, columnFamily, rowKey, name, value, ttl);

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap);

	void putColumns(mutationBatch, columnFamily, rowKey, columnMap, ttlMap);
    */
	void incrementCounterColumn(Object mutationBatch, Object columnFamily, Object rowKey, Object name);

	void incrementCounterColumn(Object mutationBatch, Object columnFamily, Object rowKey, Object name, Long value);

	/*
	void incrementCounterColumns(mutationBatch, columnFamily, rowKey, columnMap);

	void deleteRow(mutationBatch, columnFamily, rowKey);

	void execute(mutationBatch);

	Object getRow(rows, key);

	Object getColumns(row);

	Object getColumn(row, name);

	Object getColumnByIndex(row, index);

	Object name(column);

	String stringValue(column);

	Long longValue(column);

	byte[] byteArrayValue(column);
	*/

}
