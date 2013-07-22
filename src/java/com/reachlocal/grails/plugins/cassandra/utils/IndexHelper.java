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

			Object thisObjectId = thisObj.getProperty("id");
			Object oldObjectId = null;
			if (oldObj != null) {
				oldObjectId = oldObj.getProperty("id");
			}
			List explicitIndexList = (List)explicitIndexes;
			for (Object propName: explicitIndexList) {
				if (oldObj != null) {
					Map<Object,Object> oldIndexCol = new LinkedHashMap<Object,Object>();
					oldIndexCol.put(oldObjectId, "");
					List<String> oldIndexRowKeys = KeyHelper.objectIndexRowKeys(propName, oldObj);
					for (String oldIndexRowKey: oldIndexRowKeys) {
						oldIndexRows.put(oldIndexRowKey, oldIndexCol);
					}
				}

				Map<Object,Object> indexCol = new LinkedHashMap<Object,Object>();
				indexCol.put(thisObjectId, "");
				List<String> indexRowKeys = KeyHelper.objectIndexRowKeys(propName, thisObj);
				for (String indexRowKey: indexRowKeys) {
					indexRows.put(indexRowKey, indexCol);
				}
			}
		}
	}
}
