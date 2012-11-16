package com.reachlocal.grails.plugins.cassandra.utils;

import com.reachlocal.grails.plugins.cassandra.mapping.CassandraMappingNullIndexException;
import com.reachlocal.grails.plugins.cassandra.uuid.UuidDynamicMethods;
import groovy.lang.GroovyObject;
import org.codehaus.jackson.JsonGenerationException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author: Bob Florian
 */
public class KeyHelper
{
	static String makeComposite(List<String> list)
	{
		int len = list.size();
		if (len == 1) {
			return list.get(0);
		}
		else {
			StringBuilder sb = new StringBuilder(list.get(0));
			for (int i=1; i < len; i++) {
				sb.append("__");
				sb.append(list.get(i));
			}
			return sb.toString();
		}
	}

	static String[] parseComposite(String value)
	{
		return value.split("__");
	}

	static String joinRowKey(Class fromClass, Class toClass, String propName, GroovyObject object) throws UnsupportedEncodingException
	{
		//def fromClassName = fromClass.name.split("\\.")[-1]
		//"${fromClassName}?${propName}=${URLEncoder.encode(object.id)}".toString()
		return joinRowKeyFromId(fromClass, toClass, propName, String.valueOf(object.getProperty("id")));
	}

	static String joinRowKeyFromId(Class fromClass, Class toClass, String propName, String objectId) throws UnsupportedEncodingException
	{
		String fromClassName = fromClass.getSimpleName();
		return fromClassName + "?" + propName + "=" + URLEncoder.encode(objectId, ENC);
	}

	static String primaryKeyIndexRowKey()
	{
		return "this";
	}

	static String counterRowKey(List whereKeys, List<String> groupKeys, Map map) throws IOException
	{
		String key = objectIndexRowKey(whereKeys, map);
		return key != null ? key + "#" + makeComposite(groupKeys) : null;
	}

	static String counterRowKey(List whereKeys, List<String> groupKeys, GroovyObject bean) throws IOException
	{
		String key = objectIndexRowKey(whereKeys, bean);
		return key != null ? key + "#" + makeComposite(groupKeys) : null;
	}

	static List<String> makeGroupKeyList(List<String>keys, String dateSuffix)
	{
		List<String> result = new ArrayList<String>(keys.size());
		result.add(0, keys.get(0) + "[" + dateSuffix + "]");
		return result;
	}

	static String objectIndexRowKey(String propName, Map map) throws IOException
	{
		try {
			return indexRowKey(propName, map.get(propName));
		}
		catch (CassandraMappingNullIndexException e) {
			return null;
		}
	}

	static String objectIndexRowKey(List propNames, Map map) throws IOException
	{
		try {
			List<List<Object>> items = new ArrayList<List<Object>>();
			for (Object it: propNames) {
				List<Object> tuple = new ArrayList<Object>(2);
				tuple.add(it);
				tuple.add(map.get(it));
				items.add(tuple);
			}
			return indexRowKey(items);
		}
		catch (CassandraMappingNullIndexException e) {
			return null;
		}
	}

	static String objectIndexRowKey(String propName, GroovyObject bean) throws IOException
	{
		try {
			return indexRowKey(propName, bean.getProperty(propName));
		}
		catch (CassandraMappingNullIndexException e) {
			return null;
		}
	}

	static String objectIndexRowKey(List<String> propNames, GroovyObject bean) throws IOException
	{
		try {
			List<List<Object>> items = new ArrayList<List<Object>>(propNames.size());
			for (String it: propNames) {
				List<Object> tuple = new ArrayList<Object>(2);
				tuple.add(it);
				tuple.add(bean.getProperty(it));
				items.add(tuple);
			}
			return indexRowKey(items);
		}
		catch (CassandraMappingNullIndexException e) {
			return null;
		}
	}


	static Collection objectIndexRowKeys(String propName, GroovyObject bean) throws IOException
	{
		try {
			Object value = bean.getProperty(propName);
			if (value instanceof Collection) {
				Collection c = (Collection)value;
				List<String> result = new ArrayList<String>();
				for (Object it: c) {
					result.add(indexRowKey(propName, it));
				}
				return result;
			}
			else if (value != null) {
				List<String> result = new ArrayList<String>();
				result.add(indexRowKey(propName, value));
				return result;
			}
			else {
				return new ArrayList<String>();
			}
		}
		catch (CassandraMappingNullIndexException e) {
			return null;
		}
	}

	static Collection<String> objectIndexRowKeys(List<String> propNames, GroovyObject bean) throws IOException
	{
		try {
			List<Object> valueList = new ArrayList<Object>(propNames.size());
			for (String it: propNames) {
				valueList.add(bean.getProperty(it));
			}
			List<String> result = new ArrayList<String>(valueList.size());
			List<List<Object>> v2 = OrmHelper.expandNestedArray(valueList);
			for (List values: v2) {
				List<List<Object>> pairs = new ArrayList<List<Object>>();
				int index = 0;
				for (String name: propNames) {
					List<Object> tuple = new ArrayList<Object>(2);
					tuple.add(name);
					tuple.add(values.get(index));
					pairs.add(tuple);
					index++;
				}
				String key = indexRowKey(pairs);
				if (key != null) {
					result.add(key);
				}
			}
			return result;
		}
		catch (CassandraMappingNullIndexException e) {
			return null;
		}
	}

	static String indexRowKey(String name, Object value) throws CassandraMappingNullIndexException, IOException
	{
		try {
			//"this?${name}=${URLEncoder.encode(primaryRowKey(value))}".toString()
			return "this?" + name + "=" + URLEncoder.encode(primaryRowKey(value), ENC);
		}
		catch (CassandraMappingNullIndexException e) {
			return null;
		}
	}

	static String manyBackIndexRowKey(String objectId)
	{
		return "this#" + objectId;
	}

	static String oneBackIndexRowKey(String objectId)
	{
		return "this@" + objectId;
	}

	static String oneBackIndexColumnName(String columnFamily, String propertyName, String objectKey)
	{
		//"${objectKey}${END_CHAR}${propertyName}${END_CHAR}${columnFamily}".toString()
		return objectKey + END_CHAR + propertyName + END_CHAR + columnFamily;
	}

	static List<String> oneBackIndexColumnValues(String name)
	{
		String[] array = name.split(END_CHAR);
		List<String> result = new ArrayList<String>(array.length);
		for (int i=array.length-1; i >= 0; i--) {
			result.add(array[i]);
		}
		return result;
	}

	static String indexRowKey(List<List<Object>> pairs) throws CassandraMappingNullIndexException, IOException
	{
		try {
			String sep = "?";
			StringBuilder sb = new StringBuilder("this");
			for (List<Object> it: pairs) {
				sb.append(sep);
				sb.append(it.get(0));
				sb.append("=");
				sb.append(URLEncoder.encode(primaryRowKey(it.get(1)), ENC));
				sep = "&";
			}
			return sb.toString();
		}
		catch (CassandraMappingNullIndexException e) {
			return null;
		}
	}
	static String counterColumnKey(List items, DateFormat dateFormat) throws CassandraMappingNullIndexException, IOException
	{
		if (items != null && items.size() > 0) {
			List<String> list = new ArrayList<String>(items.size());
			for (Object it: items) {
				list.add(counterColumnKey(it, dateFormat));
			}
			return makeComposite(list);

		}
		else {
			throw new CassandraMappingNullIndexException("Counter column keys cannot bean null or blank");
		}
	}

	static String counterColumnKey(Date date, DateFormat dateFormat) throws CassandraMappingNullIndexException
	{
		if (date != null) {
			return dateFormat.format(date);
		}
		else {
			throw new CassandraMappingNullIndexException("Counter column keys cannot bean null or blank");
		}
	}

	static String counterColumnKey(String str, DateFormat dateFormat) throws CassandraMappingNullIndexException
	{
		if (str != null && str.length() > 0) {
			return str;
		}
		else {
			throw new CassandraMappingNullIndexException("Counter column keys cannot bean null or blank");
		}
	}

	static String counterColumnKey(Object obj, DateFormat dateFormat) throws CassandraMappingNullIndexException, IOException
	{
		return primaryRowKey(obj);  //TODO - dateFormat???
	}

	static String nullablePrimaryRowKey(Object obj) throws CassandraMappingNullIndexException, IOException
	{
		return obj == null ? null : primaryRowKey(obj);
	}

	static String primaryRowKey(Object obj) throws CassandraMappingNullIndexException, IOException
	{
		if (obj instanceof String) {
			return (String)obj;
		}
		else if (obj instanceof UUID) {
			UUID id = (UUID)obj;
			if (id.version() == 1) {
				long time = UuidDynamicMethods.time(id);
				String ts = time < 0L  ? INT_KEY_FMT2.format(time) : INT_KEY_FMT1.format(time);
				return ts + "_" + id.toString();
			}
			else {
				return id.toString();
			}

		}
		else if (obj instanceof GroovyObject) {
			GroovyObject g = (GroovyObject)obj;
			if (OrmHelper.isMappedObject(g)) {
				return (String)g.getProperty("id");
			}
		}
		else if (obj == null) {
			throw new CassandraMappingNullIndexException("Primary keys and indexed properties cannot have null values");
		}
		else if (obj instanceof Collection) {
			Collection c = (Collection)obj;
			List<String> items = new ArrayList<String>(c.size());
			for (Object it: c) {
				items.add(primaryRowKey(it));
			}
			return makeComposite(items);
		}
		else if (obj instanceof Number || obj instanceof Boolean) {
			return obj.toString();
		}

		try {
			return String.valueOf(DataMapper.dataProperty(obj));
		}
		catch (IllegalArgumentException e) {
			// TODO - why do we get this for enums in counters and not in simple properties?
			return obj.toString();
		}
	}

	static final String ENC = "UTF-8";
	static final String END_CHAR = "\u00ff";

	static final DecimalFormat INT_KEY_FMT1 = new DecimalFormat("000000000000000");
	static final DecimalFormat INT_KEY_FMT2 = new DecimalFormat("00000000000000");

	static DateFormat ISO_TS = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	static {
		ISO_TS.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
}
