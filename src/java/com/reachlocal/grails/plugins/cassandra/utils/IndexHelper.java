package com.reachlocal.grails.plugins.cassandra.utils;

import groovy.lang.GroovyObject;

import java.io.IOException;
import java.util.*;

/**
 * @author: Bob Florian
 */
public class IndexHelper
{
	public static void updateAllExplicitIndexes(
			GroovyObject oldObj,
			GroovyObject thisObj,
			Map<String, Object> cassandraMapping,
			Map<Object,Object> oldIndexRows,
			Map<Object,Object> indexRows) throws IOException
	{
		Object explicitIndexes = cassandraMapping.get("explicitIndexes");
		if (explicitIndexes instanceof List) {

			List explicitIndexList = (List)explicitIndexes;
			for (Object propName: explicitIndexList) {
				if (oldObj != null) {
					Map<Object,Object> oldIndexCol = new LinkedHashMap<Object,Object>();
					oldIndexCol.put(oldObj.getProperty("id"), "");
					List<String> oldIndexRowKeys = KeyHelper.objectIndexRowKeys(propName, oldObj);
					for (String oldIndexRowKey: oldIndexRowKeys) {
						oldIndexRows.put(oldIndexRowKey, oldIndexCol);
					}
				}

				Map<Object,Object> indexCol = new LinkedHashMap<Object,Object>();
				indexCol.put(thisObj.getProperty("id"), "");
				List<String> indexRowKeys = KeyHelper.objectIndexRowKeys(propName, thisObj);
				for (String indexRowKey: indexRowKeys) {
					indexRows.put(indexRowKey, indexCol);
				}
			}
		}
		/*
		//oldIndexRows.each {rowKey, col ->
		for (Map.Entry entry: oldIndexRows.entrySet()) {
			//col.each {colKey, v ->
			for (Map.Entry col: entry.value) {
				persistence.deleteColumn(m, indexColumnFamily, rowKey, colKey)
			}
		}*/
	}
}
