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
			Object value = entry.getValue();
			map.put(entry.getKey(), dataProperty(entry.getValue()));
		}
		return map;
	}

	public static Map<String, Object> dataProperties(
			GroovyObject data,
			List<String> transients,
			Map<String, Class> hasMany,
			String expandoMapName,
			Collection mappedProperties
	)
			throws IOException
	{
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		Class clazz = data.getClass();
		if (expandoMapName != null) {
			transients.add(expandoMapName);
		}

		// Unneeded since we now get the class from the method signatures
		// Might be needed again if we ever support subclasses
		//map.put(CLASS_NAME_KEY, clazz.getName());

		for (PropertyDescriptor pd: PropertyUtils.getPropertyDescriptors(data)) {
			String name = pd.getName();
			if (!transients.contains(name) && !GLOBAL_TRANSIENTS.contains(name) && hasMany.get(name) == null) {
				Object prop = data.getProperty(name);
				if (prop == null) {
				    if (mappedProperties.contains(name)) {
						map.put(name + "Id", null);
					}
					else {
						map.put(name, null);
					}
				}
				else {
					//if (OrmHelper.isMappedObject(prop)) {   // TODO new is mapped
					if (mappedProperties.contains(name)) {
						GroovyObject g = (GroovyObject)prop;
						String idName = name + "Id";
						map.put(idName, g.getProperty("id"));
					}
					else {
						Object value = dataProperty(prop);
						map.put(name, value);
					}
				}
			}
		}

		if (expandoMapName != null) {
			Map<String, Object> expandoMap = (Map<String, Object>)data.getProperty(expandoMapName);
			if (expandoMap != null) {
				for (Map.Entry<String, Object> entry: expandoMap.entrySet()) {
					map.put(entry.getKey(), entry.getValue());
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
		else if (value instanceof String) {
			return value;
		}
		else if (value instanceof UUID) {
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

	// Unused now except for backward compatibility since we now get the class from the method signatures
	// Might be needed again if we ever support subclasses
	public static final String CLASS_NAME_KEY = "_class_name_";

	public static final String DIRTY_SUFFIX = "_dirty";
	public static final String CLUSTER_PROP = "_cassandra_cluster_";
	static HashSet<String> GLOBAL_TRANSIENTS = new LinkedHashSet<String>();
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
