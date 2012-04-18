package com.reachlocal.grails.plugins.cassandra.mapping

import com.reachlocal.grails.plugins.cassandra.utils.HashCounter
import java.text.DateFormat
import com.reachlocal.grails.plugins.cassandra.utils.DateHelper

/**
 * @author: Bob Florian
 */
class BaseUtils 
{
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

	static rollUpCounterDates(Map map, DateFormat fromFormat, DateFormat toFormat)
	{
		return DateHelper.rollUpCounterDates(map, fromFormat, toFormat)
	}
}
