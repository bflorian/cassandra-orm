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

package com.reachlocal.grails.plugins.cassandra.utils;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import groovy.lang.GroovyObject;
import groovy.lang.MetaProperty;
import org.apache.commons.beanutils.PropertyUtils;
import org.codehaus.jackson.map.ObjectMapper;

public class DataMapper
{
	public static Map<String, Object> dataProperties(Map<String, Object> data) throws IOException
	{
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		for (Map.Entry<String, Object> entry: data.entrySet()) {
			map.put(entry.getKey(), dataProperty(entry.getValue()));
		}
		return map;
	}

	public static Map<String, Object> dataProperties(
			GroovyObject data,
			List<String> transients,
			Map<String, Class> hasMany,
			String expandoMapName
	)
			throws IOException
	{
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		Class clazz = data.getClass();
		if (expandoMapName != null) {
			transients.add(expandoMapName);
		}

		map.put(CLASS_NAME_KEY, clazz.getName());

		for (PropertyDescriptor pd: PropertyUtils.getPropertyDescriptors(data)) {
		//for (MetaProperty pd: data.getMetaClass().getProperties()) {
			String name = pd.getName();
			if (!transients.contains(name) && !GLOBAL_TRANSIENTS.contains(name) && hasMany.get(name) == null) {
				Object prop = data.getProperty(name);
				if (prop != null) {
					if (OrmHelper.isMappedObject(prop)) {   // TODO new is mapped
						GroovyObject g = (GroovyObject)prop;
						String idName = name + "Id";
						map.put(idName, g.getProperty("id"));
					}
					else {
						Object value = dataProperty(prop);
						if (value != null) {
							map.put(name, value);
						}
					}
				}
			}
		}

		if (expandoMapName != null) {
			Map<String, Object> expandoMap = (Map<String, Object>)data.getProperty(expandoMapName);
			if (expandoMap != null) {
				for (Map.Entry<String, Object> entry: expandoMap.entrySet()) {
					if (entry.getValue() != null) {
						map.put(entry.getKey(), entry.getValue());
					}
				}
			}
		}

		return map;
	}

	public static List<String> propertyNames(GroovyObject object)
	{
		List<MetaProperty> props = object.getMetaClass().getProperties();
		List<String> result = new ArrayList<String>(props.size());
		for (MetaProperty pd: props) {
			result.add(pd.getName());
		}
		return result;
	}

	public static List<String> dirtyPropertyNames(GroovyObject object)
	{
		List<MetaProperty> props = object.getMetaClass().getProperties();
		List<String> result = new ArrayList<String>();
		for (MetaProperty pd: props) {
			if (pd.getName().endsWith(DIRTY_SUFFIX)) {
				result.add(pd.getName());
			}
		}
		return result;
	}

	public static Object dataProperty(Object value) throws IOException
	{
		if (value == null) {
			return null;
		}

		Class clazz = value.getClass();
		if (value instanceof String) {
			return value;
		}
		else if (value instanceof Collection || value instanceof Map) {
			return new ObjectMapper().writeValueAsString(value);
		}
		else if (value instanceof Date) {
			return ISO_TS.format(value);
		}
		else if (value instanceof Boolean || value instanceof byte[] || value instanceof ByteBuffer) {
			return value;
		}
		else {
			return value.toString();
		}
	}

	static final String DIRTY_SUFFIX = "_dirty";
	static final String CLUSTER_PROP = "_cassandra_cluster_";
	static final String CLASS_NAME_KEY = "_class_name_";
	static HashSet<String> GLOBAL_TRANSIENTS = new LinkedHashSet<String>();
	//["class","id","cassandra","indexColumnFamily","columnFamily","counterColumnFamily","metaClass","keySpace","cassandraCluster",DIRTY_SUFFIX,CLUSTER_PROP] as Set
	static DateFormat ISO_TS = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	static {
		ISO_TS.setTimeZone(TimeZone.getTimeZone("GMT"));

		GLOBAL_TRANSIENTS.add("class");
		GLOBAL_TRANSIENTS.add("id");
		GLOBAL_TRANSIENTS.add("cassandra");
		GLOBAL_TRANSIENTS.add("indexColumnFamily");
		GLOBAL_TRANSIENTS.add("columnFamily");
		GLOBAL_TRANSIENTS.add("counterColumnFamily");
		GLOBAL_TRANSIENTS.add("metaClass");
		GLOBAL_TRANSIENTS.add("keySpace");
		GLOBAL_TRANSIENTS.add("cassandraCluster");
		GLOBAL_TRANSIENTS.add(DIRTY_SUFFIX);
		GLOBAL_TRANSIENTS.add(CLUSTER_PROP);

	}
}
