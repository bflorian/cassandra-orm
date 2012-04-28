package com.reachlocal.grails.plugins.cassandra.mapping

import java.text.DateFormat
import java.nio.ByteBuffer

/**
 * @author: Bob Florian
 */
class KeyUtils extends BaseUtils
{
	static String makeComposite(list)
	{
		list.join("__")
	}

	static List parseComposite(String value)
	{
		value.split("__")
	}

	static joinRowKey(fromClass, toClass, propName, object)
	{
		def fromClassName = fromClass.name.split("\\.")[-1]
		"${fromClassName}?${propName}=${URLEncoder.encode(object.id)}".toString()
	}

	static primaryKeyIndexRowKey()
	{
		"this"
	}

	static counterRowKey(List whereKeys, List groupKeys, Map map)
	{
		def key = objectIndexRowKey(whereKeys, map)
		key ? "${key}#${makeComposite(groupKeys)}".toString() : null
	}

	static counterRowKey(List whereKeys, List groupKeys, Object bean)
	{
		def key = objectIndexRowKey(whereKeys, bean)
		key ? "${key}#${makeComposite(groupKeys)}".toString() : null
	}

	static makeGroupKeyList(keys, dateSuffix)
	{
		def result = keys.clone()
		result[0] = "${keys[0]}[${dateSuffix}]"
		return result
	}

	static objectIndexRowKey(String propName, Map map)
	{
		try {
			return indexRowKey(propName, map[propName])
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static objectIndexRowKey(String propName, Object bean)
	{
		try {
			return indexRowKey(propName, bean.getProperty(propName))
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static objectIndexRowKey(List propNames, Map map)
	{
		try {
			return indexRowKey(propNames.collect{[it, map[it]]})
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static objectIndexRowKey(List propNames, Object bean)
	{
		try {
			return indexRowKey(propNames.collect{[it, bean.getProperty(it)]})
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static indexRowKey(String name, value)
	{
		try {
			"this?${name}=${URLEncoder.encode(primaryRowKey(value))}".toString()
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static indexRowKey(List pairs)
	{
		try {
			def sep = "?"
			def sb = new StringBuilder("this")
			pairs.each {
				sb << sep
				sb << it[0]
				sb << '='
				sb << URLEncoder.encode(primaryRowKey(it[1]))
				sep = "&"
			}
			return sb.toString()
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}
	static counterColumnKey(List items, DateFormat dateFormat) throws CassandraMappingNullIndexException
	{
		if (items) {
			makeComposite(items.collect{counterColumnKey(it, dateFormat)})
		}
		else {
			throw new CassandraMappingNullIndexException("Counter column keys cannot bean null or blank")
		}
	}

	static counterColumnKey(Date date, DateFormat dateFormat)
	{
		if (date) {
			dateFormat.format(date)
		}
		else {
			throw new CassandraMappingNullIndexException("Counter column keys cannot bean null or blank")
		}
	}

	static counterColumnKey(String str, DateFormat dateFormat)
	{
		if (str) {
			return str
		}
		else {
			throw new CassandraMappingNullIndexException("Counter column keys cannot bean null or blank")
		}
	}

	static counterColumnKey(obj, DateFormat dateFormat) throws CassandraMappingNullIndexException
	{
		primaryRowKey(obj)
	}

	static primaryRowKey(List objs)
	{
		makeComposite(objs.collect{primaryRowKey(it)})
	}

	static primaryRowKey(String id)
	{
		id
	}

	static primaryRowKey(Number id)
	{
		id.toString()
	}

	static primaryRowKey(Boolean id)
	{
		id.toString()
	}

	static primaryRowKey(UUID id)
	{
		dataProperty(id)
	}

	static primaryRowKey(obj) throws CassandraMappingNullIndexException
	{
		if (obj == null) {
			throw new CassandraMappingNullIndexException("Primary keys and indexed properties cannot have null values")
		}
		else if (isMappedObject(obj)) {
			return obj.id
		}
		else {
			return dataProperty(obj)
		}
	}
	static dataProperty(Date value)
	{
		return value.time.toString()
	}

	static dataProperty(Integer value)
	{
		return value.toString()
	}

	static dataProperty(Long value)
	{
		return value.toString()
	}

	static dataProperty(Double value)
	{
		return value.toString()
	}

	static dataProperty(BigDecimal value)
	{
		return value.toString()
	}

	static dataProperty(BigInteger value)
	{
		return value.toString()
	}

	static dataProperty(UUID value)
	{
		return value.toString()
	}

	static dataProperty(Collection value)
	{
		return value.encodeAsJSON()
	}

	static dataProperty(Map value)
	{
		return value.encodeAsJSON()
	}

	static dataProperty(Boolean value)
	{
		return value
	}

	static dataProperty(String value)
	{
		return value
	}

	static dataProperty(byte[] value)
	{
		return value

	}

	static dataProperty(ByteBuffer value)
	{
		return value
	}

	static dataProperty(Class value)
	{
		return null
	}

	def dataProperty(value)
	{
		if (value?.getClass()?.isEnum()) {
			value.toString()
		}
		else {
			null
		}
	}
}
