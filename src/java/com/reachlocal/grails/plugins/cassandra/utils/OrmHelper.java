package com.reachlocal.grails.plugins.cassandra.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import com.reachlocal.grails.plugins.cassandra.mapping.CassandraMappingException;
import groovy.lang.GroovyObject;
import groovy.lang.MissingMethodException;
import groovy.lang.MissingPropertyException;
import org.apache.commons.beanutils.PropertyUtils;

import java.util.List;

/**
 * @author: Bob Florian
 */
public class OrmHelper
{
	public static final String END_CHAR = "\u00ff";
	public static final int MAX_ROWS = 5000;
	public static final String CLASS_NAME_KEY = "_class_name_";
	public static final String KEY_SUFFIX = "Id";
	public static final String DIRTY_SUFFIX = "_dirty";
	public static final String CLUSTER_PROP = "_cassandra_cluster_";

	public static String methodForPropertyName(String prefix, String propertyName)
	{
		String result = prefix + propertyName.substring(0,1);
		if (propertyName.length() > 1) {
			return prefix + propertyName.substring(0,1).toUpperCase() + propertyName.substring(1);
		}
		else {
			return prefix + propertyName.toUpperCase();
		}
	}

	public static List propertyListFromMethodName(String name)
	{
		String[] exps = name.replaceAll("([a-z,0-9])And([A-Z])","$1,$2").split(",");
		List<String> result = new ArrayList<String>(exps.length);
		for (String it: exps) {
			if (it.length() > 1) {
				result.add(it.substring(0, 1).toLowerCase() + it.substring(1));
			}
			else {
				result.add(it.toLowerCase());
			}
		}
		return result;
	}

	public static String propertyNameFromClassName(String name)
	{
		return name.substring(0,1).toLowerCase() + name.substring(1);
	}

	public static Collection collection(Object value)
	{
		// TODO handle arrays?
		if (value instanceof Collection) {
			return (Collection)value;
		}
		else {
			List<Object> list = new ArrayList<Object>(1);
			list.add(value);
			return list;
		}
	}

	public static List<String> stringList(Object value)
	{
		// TODO handle arrays?
		// Do we want one that assumes a collection of strings?
		List<String> list;
		if (value instanceof Collection) {
			Collection c = (Collection)value;
			list = new ArrayList<String>((c).size());
			for (Object it: c) {
				list.add(String.valueOf(it));
			}
		}
		else if (value != null) {
			list = new ArrayList<String>(1);
			list.add(String.valueOf(value));
		}
		else {
			list = new ArrayList<String>();
		}
		return list;
	}

	public static boolean containsElement(Collection col1, Collection col2)
	{
		for (Object obj: col1) {
			if (col2.contains(obj)) {
				return true;
			}
		}
		return false;
	}
/*
	public static boolean isMappedClass(Class clazz) {
		return clazz.metaClass.hasMetaProperty("cassandraMapping")
	}
*/
	public static boolean isMappedObject(GroovyObject object) {
		// TODO - doesn't test for static!
		//return object ? object.getClass().metaClass.hasMetaProperty("cassandraMapping") : false
		try {
			Object value = object.getProperty("cassandraMapping");
			return value != null && value instanceof Map;
		}
		catch (MissingPropertyException e) {
			return false;
		}
	}
/*
	public static boolean isMappedProperty(Class clazz, String name) {
		try {
			def propClass = clazz.getDeclaredField(name)?.type // TODO - support superclasses?
			return propClass ? isMappedClass(propClass) : false
		}
		catch (NoSuchFieldException e) {
			return false
		}
	}

	public static safeGetStaticProperty(clazz, name)
	{
		if (clazz.metaClass.hasMetaProperty(name)) {
			return clazz.metaClass.getMetaProperty(name).getProperty()
		}
		return null
	}
*/
	public static Object safeGetProperty(GroovyObject data, String name, Class clazz, Object defaultValue)
	{
		Object value;
		try {
			value = data.getProperty(name);
			if (!clazz.isInstance(value)) {
				value = defaultValue;
			}
		}
		catch (MissingPropertyException e) {
			value = defaultValue;
		}
		catch (MissingMethodException e) {
			value = defaultValue;
		}
		return value;
	}

	public static void safeSetProperty(Object object, String name, Object value) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException
	{
		if (PropertyUtils.getPropertyType(object, name) != null) {
			PropertyUtils.setProperty(object, name, value);
		}
	}

	public static Map addOptionDefaults(Map<String, Object>options, Integer defaultCount)
	{
		return addOptionDefaults(options, defaultCount, null);
	}

	public static Map addOptionDefaults(Map<String, Object>options, Integer defaultCount, String cluster)
	{
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		result.putAll(options);

		Object reversed = options.get("reversed");
		if (reversed == null) {
			reversed = false;
		}
		result.put("reversed", reversed);

		Object max = options.get("max");
		if (max == null) {
			max = defaultCount;
		}
		result.put("max", max);

		Object clstr = options.get("cluster");
		if (clstr == null && cluster != null) {
			result.put("cluster", cluster);
		}
		return result;
	}

	public static Map<String, Object> sort(Map<String, Object> map)
	{
		Map<String, Object> sorted = new LinkedHashMap<String, Object>();
		List<String> keys = new ArrayList<String>(map.size());
		keys.addAll(map.keySet());
		Collections.sort(keys);
		for (String key: keys) {
			sorted.put(key, map.get(key));
		}
		return sorted;
	}

	public static List<List<String>> expandNestedArray(List params)
	{
		List<List<String>> result = new ArrayList<List<String>>(params.size());

		Integer len = 1;
		List<Integer> lengths = new ArrayList<Integer>(params.size());

		for (Object value: params) {
			if (value instanceof List) {
				len = len * ((List)value).size();
			}
			else if (value != null && value.getClass().isArray()) {
				len = len * ((Object[])value).length;
			}
			lengths.add(len);
		}

		for (int i=0; i < len; i++) {
			result.add(new ArrayList<String>());
		}

		int pindex = 0;
		for (Object value: params) {
			if (value instanceof List) {
				List valueList = (List)value;
				int index = 0;
				for (List<String> item: result) {
					int i = (index * lengths.get(pindex) / len) % valueList.size();
					item.add(String.valueOf(valueList.get(i)));
				}
			}
			else if (value != null && value.getClass().isArray()) {
				String[] valueList = (String[])value;
				int index = 0;
				for (List<String> item: result) {
					int i = (index * lengths.get(pindex) / len) % valueList.length;
					item.add(valueList[i]);
				}
			}
			else {
				for (List<String> item: result) {
					item.add(String.valueOf(value));
				}
			}
			pindex++;
		}
		return result;
	}

	public static void checkForDefaultRowsInsufficient(Integer max, Integer count) throws CassandraMappingException
	{
		if (max == null && count == MAX_ROWS) {
			throw new CassandraMappingException("Query failed because default row limit of ${MAX_ROWS} is potentially insuficient. Specify an explicit max option.");
		}
	}
}
