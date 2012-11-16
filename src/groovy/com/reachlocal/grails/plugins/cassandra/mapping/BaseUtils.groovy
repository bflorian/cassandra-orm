package com.reachlocal.grails.plugins.cassandra.mapping

import org.apache.commons.beanutils.PropertyUtils
import org.codehaus.jackson.map.ObjectMapper
import java.text.DecimalFormat

/**
 * @author: Bob Florian
 */
class BaseUtils 
{
	static protected final END_CHAR = "\u00ff"
	static protected final int MAX_ROWS = 5000
	static protected final INDEX_OPTIONS = ["start","startAfter","finish","keys"]
	static protected final OBJECT_OPTIONS = ["column","columns", "rawColumn", "rawColumns"]
	static protected final ALL_OPTIONS = INDEX_OPTIONS + OBJECT_OPTIONS  + ["cluster"]
	static protected final CLASS_NAME_KEY = '_class_name_'
	static protected final KEY_SUFFIX = "Id"
	static protected final DIRTY_SUFFIX = "_dirty"
	static protected final CLUSTER_PROP = "_cassandra_cluster_"
	static protected final GLOBAL_TRANSIENTS = ["class","id","cassandra","indexColumnFamily","columnFamily","counterColumnFamily","metaClass","keySpace","cassandraCluster",DIRTY_SUFFIX,CLUSTER_PROP] as Set

	static String methodForPropertyName(prefix, propertyName)
	{
		return "${prefix}${propertyName[0].toUpperCase()}${propertyName.size() > 1 ? propertyName[1..-1] : ''}"
	}

	static List propertyListFromMethodName(name)
	{
		def exps = name.replaceAll('([a-z,0-9])And([A-Z])','$1,$2').split(",")
		exps = exps.collect{it[0].toLowerCase() + (it.size() > 0 ? it[1..-1] : '')}
	}

	static String propertyNameFromClassName(name)
	{
		name[0].toLowerCase() + name[1..-1]
	}

	static collection(Collection value)
	{
		return value
	}

	static collection(value)
	{
		return [value]
	}

	static containsElement(Collection col1, Collection col2)
	{
		for (obj in col1) {
			if (col2.contains(obj)) {
				return true
			}
		}
		return false
	}

	static boolean isMappedClass(clazz) {
		return clazz.metaClass.hasMetaProperty("cassandraMapping")
	}

	static boolean isMappedObject(object) {
		return object ? object.getClass().metaClass.hasMetaProperty("cassandraMapping") : false
	}

	static boolean isMappedProperty(Class clazz, String name) {
		try {
			def propClass = clazz.getDeclaredField(name)?.type // TODO - support superclasses?
			return propClass ? isMappedClass(propClass) : false
		}
		catch (NoSuchFieldException e) {
			return false
		}
	}

	static safeGetStaticProperty(clazz, name)
	{
		if (clazz.metaClass.hasMetaProperty(name)) {
			return clazz.metaClass.getMetaProperty(name).getProperty()
		}
		return null
	}

	static safeGetProperty(data, name, clazz, defaultValue)
	{
		def value
		try {
			value = clazz.isInstance(data.getProperty(name)) ? data.getProperty(name) : defaultValue
		}
		catch (MissingPropertyException e) {
			value = defaultValue
		}
		catch (MissingMethodException e) {
			value = defaultValue
		}
		return value
	}

	static safeSetProperty(object, name, value)
	{
		if (PropertyUtils.getPropertyType(object, name)) {
			PropertyUtils.setProperty(object, name, value)
		}
	}

	static Map addOptionDefaults(options, defaultCount, cluster=null)
	{
		Map result = [
				reversed : options.reversed ? true : false,
				max : options.max ?: defaultCount,
				cluster: cluster
		]
		ALL_OPTIONS.each {
			if (options[it]) {
				result[it] = options[it]
			}
		}
		return result
	}

	static sort(map)
	{
		def sorted = [:]
		map.collect{it}.sort{it.key}.each {
			sorted[it.key] = it.value
		}
		return sorted;
	}

	static expandNestedArray(params)
	{
		def result = []
		def len = 1
		def lengths = []
		params.each {value ->
			if (value instanceof Collection || value.getClass().isArray()) {
				len = len * value.size()
			}
			lengths << len
		}
		for (i in 1..len) {
			result << []
		}

		params.eachWithIndex {value, pindex ->
			if (value instanceof Collection || value.getClass().isArray()) {
				result.eachWithIndex {item, index ->
					def i = (index * lengths[pindex] / len).toInteger() % value.size()
					item << value[i]
				}
			}
			else {
				result.each {item ->
					item << value
				}
			}
		}
		return result
	}

	static checkForDefaultRowsInsufficient(max, count)
	{
		if (max == null && count == MAX_ROWS) {
			throw new CassandraMappingException("Query failed because default row limit of ${MAX_ROWS} is potentially insuficient. Specify an explicit max option.")
		}
	}

	static mapper = new ObjectMapper()
}
