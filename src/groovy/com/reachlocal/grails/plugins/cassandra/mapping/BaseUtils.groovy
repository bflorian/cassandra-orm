package com.reachlocal.grails.plugins.cassandra.mapping

import com.reachlocal.grails.plugins.cassandra.utils.HashCounter
import java.text.DateFormat
import com.reachlocal.grails.plugins.cassandra.utils.DateHelper
import org.apache.commons.beanutils.PropertyUtils

/**
 * @author: Bob Florian
 */
class BaseUtils 
{
	static protected final int MAX_ROWS = 2000
	static protected final INDEX_OPTIONS = ["start","finish","keys"]
	static protected final OBJECT_OPTIONS = ["column","columns", "rawColumn", "rawColumns"]
	static protected final ALL_OPTIONS = INDEX_OPTIONS + OBJECT_OPTIONS
	static protected final CLASS_NAME_KEY = '_class_name_'
	static protected final GLOBAL_TRANSIENTS = ["class","id","cassandra","indexColumnFamily","columnFamily","metaClass","keySpace"] as Set
	static protected final KEY_SUFFIX = "_key"
	static protected final DIRTY_SUFFIX = "_dirty"

	static String stringValue(String s)
	{
		'"' + s.replaceAll('"','\\"') + '"'
	}

	static String stringValue(s)
	{
		String.valueOf(s)
	}

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

	static mapTotal(Map map)
	{
		def total = 0
		map.each {key, value ->
			total += mapTotal(value)
		}
		return total
	}

	static mapTotal(number) {
		return number
	}

	static groupBy(Map map, int level)
	{
		def result = new HashCounter()
		processGroupByItem(map, [], level, result)
		return result
	}

	static void processGroupByItem(Map item, List keys, int groupLevel, HashCounter result)
	{
		item.each {key, value ->
			processGroupByItem(value, keys + key, groupLevel, result)
		}
	}

	static void processGroupByItem(Number item, List keys, int groupLevel, HashCounter result)
	{
		result.increment(keys[groupLevel], item)
	}

	static boolean isMappedClass(clazz) {
		return clazz.metaClass.hasMetaProperty("cassandraMapping")
	}

	static boolean isMappedObject(object) {
		return object ? object.class.metaClass.hasMetaProperty("cassandraMapping") : false
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

	static Map addOptionDefaults(options, defaultCount)
	{
		Map result = [
				reversed : options.reversed ? true : false,
				max : options.max ?: defaultCount,
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

}
