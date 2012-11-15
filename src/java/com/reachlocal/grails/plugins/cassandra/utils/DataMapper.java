package com.reachlocal.grails.plugins.cassandra.utils;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import com.reachlocal.grails.plugins.cassandra.mapping.KeyUtils;
import com.reachlocal.grails.plugins.cassandra.mapping.MappingUtils;
import groovy.lang.GroovyObject;
import groovy.lang.MetaBeanProperty;
import groovy.lang.MetaMethod;
import groovy.lang.MetaProperty;
import org.apache.commons.beanutils.PropertyUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class DataMapper
{
	public static Map<String, Object> dataProperties(Map<String, Object> data)
	{
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		for (Map.Entry<String, Object> entry: data.entrySet()) {
			map.put(entry.getKey(), entry.getValue());
		}
		return map;
	}

	public static Map<String, Object> dataProperties(
			GroovyObject data,
			List<String> transients,
			Map<String, Class> hasMany,
			String expandoMapName
	)
			throws JsonMappingException, JsonGenerationException, IOException
	{
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		Class clazz = data.getClass();
		if (expandoMapName != null) {
			transients.add(expandoMapName);
		}

		map.put(CLASS_NAME_KEY, clazz.getName());

		for (PropertyDescriptor pd: PropertyUtils.getPropertyDescriptors(data)) {
			String name = pd.getName();
			if (!transients.contains(name) && !GLOBAL_TRANSIENTS.contains(name) && hasMany.get(name) == null) {
				Object prop = data.getProperty(name);
				if (prop != null) {
					if (!MappingUtils.isMappedObject(prop)) {
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

	public static List<String> propertyNames(Object object)
	{
		PropertyDescriptor[] props = PropertyUtils.getPropertyDescriptors(object);
		List<String> result = new ArrayList<String>(props.length);
		for (PropertyDescriptor pd: props) {
			result.add(pd.getName());
		}
		return result;
	}

	public static List<String> dirtyPropertyNames(Object object)
	{
		PropertyDescriptor[] props = PropertyUtils.getPropertyDescriptors(object);
		List<String> result = new ArrayList<String>(props.length);
		for (PropertyDescriptor pd: props) {
			if (pd.getName().endsWith(DIRTY_SUFFIX)) {
				result.add(pd.getName());
			}
		}
		return result;
	}

	public static Object dataProperty(Object value) throws JsonMappingException, JsonGenerationException, IOException
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
