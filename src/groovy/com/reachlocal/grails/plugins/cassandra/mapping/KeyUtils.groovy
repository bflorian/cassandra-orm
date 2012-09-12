package com.reachlocal.grails.plugins.cassandra.mapping

import java.text.DateFormat
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.text.DecimalFormat

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
		//def fromClassName = fromClass.name.split("\\.")[-1]
		//"${fromClassName}?${propName}=${URLEncoder.encode(object.id)}".toString()
		joinRowKeyFromId(fromClass, toClass, propName, object.id)
	}

	static joinRowKeyFromId(fromClass, toClass, propName, objectId)
	{
		def fromClassName = fromClass.name.split("\\.")[-1]
		"${fromClassName}?${propName}=${URLEncoder.encode(objectId)}".toString()
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

	static objectIndexRowKey(List propNames, Map map)
	{
		try {
			return indexRowKey(propNames.collect{[it, map[it]]})
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

	static objectIndexRowKey(List propNames, Object bean)
	{
		try {
			return indexRowKey(propNames.collect{[it, bean.getProperty(it)]})
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}


	static Collection objectIndexRowKeys(String propName, Object bean)
	{
		try {
			def value = bean.getProperty(propName)
			if (value instanceof Collection) {
				def result = []
				value.each {
					result << indexRowKey(propName, it)
				}
				return result
			}
			else if (value != null) {
				return [indexRowKey(propName, value)]
			}
			else {
				return []
			}
		}
		catch (CassandraMappingNullIndexException e) {
			return null
		}
	}

	static Collection objectIndexRowKeys(List propNames, Object bean)
	{
		try {
			def valueList = propNames.collect{bean.getProperty(it)}
			def result = []
			def v2 = expandNestedArray(valueList)
			v2.each {values ->
				def pairs = []
				propNames.eachWithIndex {name, index ->
					pairs << [name, values[index]]
				}
				def key = indexRowKey(pairs)
				if (key) {
					result << key
				}
			}
			return result
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

	static manyBackIndexRowKey(objectId)
	{
		"this#${objectId}".toString()
	}

	static oneBackIndexRowKey(objectId)
	{
		"this@${objectId}".toString()
	}

	static oneBackIndexColumnName(columnFamily, propertyName, objectKey)
	{
		"${objectKey}${END_CHAR}${propertyName}${END_CHAR}${columnFamily}".toString()
	}

	static oneBackIndexColumnValues(String name)
	{
		name.split(END_CHAR).reverse()
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

	static nullablePrimaryRowKey(obj)
	{
		return obj == null ? null : primaryRowKey(obj)
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
		if (id.version() == 1) {
			def time = id.time
			def ts = time < 0L  ? INT_KEY_FMT2.format(time) : INT_KEY_FMT1.format(time)
			return "${ts}_${id}".toString()
		}
		else {
			return id.toString()
		}
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
			try {
				return dataProperty(obj)
			}
			catch (IllegalArgumentException e) {
				// TODO - why do we get this for enums in counters and not in simple properties?
				obj.toString()
			}
		}
	}
	static dataProperty(Date value)
	{
		return ISO_TS.format(value)
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
		mapper.writeValueAsString(value)
	}

	static dataProperty(Map value)
	{
		mapper.writeValueAsString(value)
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

	static final INT_KEY_FMT1 = new DecimalFormat("000000000000000")
	static final INT_KEY_FMT2 = new DecimalFormat("00000000000000")

	static protected ISO_TS = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
	static {
		ISO_TS.setTimeZone(TimeZone.getTimeZone("GMT"))
	}
}
