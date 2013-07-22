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

package com.reachlocal.grails.plugins.cassandra.test

import org.junit.Test
import static org.junit.Assert.*
import com.reachlocal.grails.plugins.cassandra.utils.KeyHelper
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroup
import com.reachlocal.grails.plugins.cassandra.test.orm.User
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroupMeeting
import com.reachlocal.grails.plugins.cassandra.uuid.UuidDynamicMethods
import com.reachlocal.grails.plugins.cassandra.test.orm.Color
import java.text.SimpleDateFormat

/**
 * @author: Bob Florian
 */
class KeyHelperTests 
{
	@Test
	void test_makeComposite()
	{
		assertEquals "01234", KeyHelper.makeComposite(["01234"])
		assertEquals "01234__5678", KeyHelper.makeComposite(["01234","5678"])
		assertEquals "01234__5678__abcd", KeyHelper.makeComposite(["01234","5678","abcd"])
	}

	@Test
	void test_parseComposite()
	{
		assertEquals "01234", KeyHelper.parseComposite("01234")[0]
		assertEquals "01234", KeyHelper.parseComposite("01234__5678")[0]
		assertEquals "5678", KeyHelper.parseComposite("01234__5678")[1]
		assertEquals "abcd", KeyHelper.parseComposite("01234__5678__abcd")[2]
	}

	@Test
	void test_joinRowKey()
	{
		def mockUserGroup = new Expando(id: "UG_0001")
		assertEquals "UserGroup?meetings=UG_0001", KeyHelper.joinRowKey(UserGroup, UserGroupMeeting, "meetings", mockUserGroup)
	}

	@Test
	void test_joinRowKeyFromId()
	{
		assertEquals "UserGroup?users=00001", KeyHelper.joinRowKeyFromId(UserGroup, User, "users", "00001")
	}

	@Test
	void test_primaryKeyIndexRowKey()
	{
		assertEquals "this", KeyHelper.primaryKeyIndexRowKey()
	}

	// public static String counterRowKey(List whereKeys, List<String> groupKeys, Map map) throws IOException
	@Test
	void test_counterRowKey_Map()
	{
		assertEquals "this?siteId=SITE01#refType__refName__refKeyword",
				KeyHelper.counterRowKey(["siteId"], ["refType","refName","refKeyword"], [siteId:"SITE01"])
	}

	// public static String counterRowKey(List whereKeys, List<String> groupKeys, GroovyObject bean) throws IOException
	@Test
	void test_counterRowKey_Bean()
	{
		def mockObject = new Expando(siteId: "SITE_0001")
		assertEquals "this?siteId=SITE_0001#refType__refName__refKeyword",
				KeyHelper.counterRowKey(["siteId"], ["refType","refName","refKeyword"], mockObject)

	}

	// public static List<String> makeGroupKeyList(List<String>keys, String dateSuffix)
	@Test
	void test_makeGroupKeyList()
	{
		def result = KeyHelper.makeGroupKeyList(["occurTime","refType","refName"], "yyyy-MM-dd")
		println result
		assertEquals 3, result.size()
		assertEquals "occurTime[yyyy-MM-dd]", result[0]
		assertEquals "refType", result[1]
		assertEquals "refName", result[2]
	}

	// public static String objectIndexRowKey(String propName, Map map) throws IOException
	@Test
	void test_objectIndexRowKey_NameMap()
	{
		def result = KeyHelper.objectIndexRowKey("color", [color: "Blue"])
		println result
		assertEquals "this?color=Blue", result
	}

	// public static String objectIndexRowKey(List propNames, Map map) throws IOException
	@Test
	void test_objectIndexRowKey_ListMap()
	{
		def result = KeyHelper.objectIndexRowKey(["color","charm","spin"], [color: "Blue", charm: "Yes", spin: "Up&Left", foo: "Bar"])
		println result
		assertEquals "this?color=Blue&charm=Yes&spin=Up%26Left", result
	}

	// public static String objectIndexRowKey(List propNames, Map map) throws IOException
	@Test
	void test_objectIndexRowKey_ListMap_Empty()
	{
		def result = KeyHelper.objectIndexRowKey([], [color: "Blue", charm: "Yes", spin: "Up&Left", foo: "Bar"])
		println result
		assertEquals "this", result
	}

	// public static String objectIndexRowKey(String propName, GroovyObject bean) throws IOException
	@Test
	void test_objectIndexRowKey_NameBean()
	{
		def mockObject = new Expando(color: "Blue/Green")

		def result = KeyHelper.objectIndexRowKey("color", mockObject)
		println result
		assertEquals "this?color=Blue%2FGreen", result
	}

	// public static String objectIndexRowKey(List<String> propNames, GroovyObject bean) throws IOException
	@Test
	void test_objectIndexRowKey_ListBean()
	{
		def mockObject = new Expando(color: "Blue", charm: "Yes", spin: "Up&Left", foo: "Bar")

		def result = KeyHelper.objectIndexRowKey(["color","charm","spin"], mockObject)
		println result
		assertEquals "this?color=Blue&charm=Yes&spin=Up%26Left", result
	}


	// public static Collection objectIndexRowKeys(String propName, GroovyObject bean) throws IOException
	@Test
	void test_objectIndexRowKeys_NameBean()
	{
		def mockObject = new Expando(color: "Blue/Green")

		def result = KeyHelper.objectIndexRowKeys("color", mockObject)
		println result
		assertEquals 1, result.size()
		assertEquals "this?color=Blue%2FGreen", result[0]
	}

	// public static Collection<String> objectIndexRowKeys(List<String> propNames, GroovyObject bean) throws IOException
	@Test
	void test_objectIndexRowKeys_ListBean()
	{
		def mockObject = new Expando(color: "Blue", charm: "Yes", spin: "Up&Left", foo: "Bar")

		def result = KeyHelper.objectIndexRowKeys(["color","charm","spin"], mockObject)
		println result
		assertEquals 1, result.size()
		assertEquals "this?color=Blue&charm=Yes&spin=Up%26Left", result[0]
	}

	// public static String indexRowKey(String name, Object value) throws CassandraMappingNullIndexException, IOException
	@Test
	void test_indexRowKey_Name()
	{
		def uuid = UuidDynamicMethods.timeUUID(Date.parse("yyyy-MM-dd HH:mm z","2012-11-15 12:00 GMT").time)

	    assertEquals "this?uuid=001352980800000_${uuid}".toString(), KeyHelper.indexRowKey("uuid", uuid)
		assertEquals "this?age=35", KeyHelper.indexRowKey("age", 35)
		assertEquals "this?date=001352980800000", KeyHelper.indexRowKey("date", Date.parse("yyyy-MM-dd HH:mm z","2012-11-15 12:00 GMT"))
		assertEquals "this?name=Joe", KeyHelper.indexRowKey("name", "Joe")
	}

	// public static String manyBackIndexRowKey(String objectId)
	@Test
	void test_manyBackIndexRowKey()
	{
		assertEquals "this#001352980800000_fb3969b0-2f1b-11e2-816c-001c42000009", KeyHelper.manyBackIndexRowKey("001352980800000_fb3969b0-2f1b-11e2-816c-001c42000009")
	}

	// public static String oneBackIndexRowKey(String objectId)
	@Test
	void test_oneBackIndexRowKey()
	{
		assertEquals "this@001352980800000_fb3969b0-2f1b-11e2-816c-001c42000009", KeyHelper.oneBackIndexRowKey("001352980800000_fb3969b0-2f1b-11e2-816c-001c42000009")
	}

	// public static String oneBackIndexColumnName(String columnFamily, String propertyName, String objectKey)
	@Test
	void test_oneBackIndexColumnName()
	{
		def result = KeyHelper.oneBackIndexColumnName("Visit", "refType", "001352980800000_fb3969b0-2f1b-11e2-816c-001c42000009")
		println result
		assertEquals "001352980800000_fb3969b0-2f1b-11e2-816c-001c42000009\u00ffrefType\u00ffVisit", result
	}

	// public static List<String> oneBackIndexColumnValues(String name)
	@Test
	void test_oneBackIndexColumnValues()
	{
		def result = KeyHelper.oneBackIndexColumnValues("001352980800000_fb3969b0-2f1b-11e2-816c-001c42000009\u00ffrefType\u00ffVisit")
		println result

		assertEquals 3, result.size()
		assertEquals "Visit", result[0]
		assertEquals "refType", result[1]
		assertEquals "001352980800000_fb3969b0-2f1b-11e2-816c-001c42000009", result[2]
	}

	// public static String indexRowKey(List<List<Object>> pairs) throws CassandraMappingNullIndexException, IOException
	@Test
	void test_indexRowKey_List()
	{
		def result = KeyHelper.indexRowKey([["name","Joe"],["rank","Captain"],["serialNumber",1000001]])
		println result
		assertEquals "this?name=Joe&rank=Captain&serialNumber=1000001", result
	}

	// public static String counterColumnKey(List items, DateFormat dateFormat) throws CassandraMappingNullIndexException, IOException
	@Test
	void test_counterColumnKey_List()
	{
		def dateFormat = new SimpleDateFormat("yyyy-MM-dd")
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
		def date = Date.parse("yyyy-MM-dd HH:mm z","2012-11-15 12:00 GMT")
		def result = KeyHelper.counterColumnKey([date, "Search", "Google"], dateFormat)
		println result
		assertEquals "2012-11-15__Search__Google", result
	}

	// public static String counterColumnKey(Date date, DateFormat dateFormat) throws CassandraMappingNullIndexException
	@Test
	void test_counterColumnKey_Date()
	{
		def dateFormat = new SimpleDateFormat("yyyy-MM-dd")
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
		def date = Date.parse("yyyy-MM-dd HH:mm z","2012-11-15 12:00 GMT")
		def result = KeyHelper.counterColumnKey(date, dateFormat)
		println result
		assertEquals "2012-11-15", result
	}

	// public static String counterColumnKey(String str, DateFormat dateFormat) throws CassandraMappingNullIndexException
	@Test
	void test_counterColumnKey_String()
	{
		def dateFormat = new SimpleDateFormat("yyyy-MM-dd")
		assertEquals "SomeString", KeyHelper.counterColumnKey("SomeString", dateFormat)
	}

	// public static String counterColumnKey(Object obj, DateFormat dateFormat) throws CassandraMappingNullIndexException, IOException
	@Test
	void test_counterColumnKey_Object()
	{
		def dateFormat = new SimpleDateFormat("yyyy-MM-dd")
		def uuid = UuidDynamicMethods.timeUUID(Date.parse("yyyy-MM-dd HH:mm z","2012-11-15 12:00 GMT").time)
		assertEquals "001352980800000_${uuid}".toString(), KeyHelper.counterColumnKey(uuid, dateFormat)
	}

	// public static String nullablePrimaryRowKey(Object obj) throws CassandraMappingNullIndexException, IOException
	@Test
	void test_nullablePrimaryRowKey()
	{
		assertEquals "SomeString", KeyHelper.nullablePrimaryRowKey("SomeString")
		assertNull KeyHelper.nullablePrimaryRowKey(null)
	}

	// public static String primaryRowKey(Object obj) throws CassandraMappingNullIndexException, IOException
	@Test
	void test_primaryRowKey()
	{
		def mockObject = new Expando(id: "X15", color: "Blue", charm: "Yes", spin: "Up&Left", foo: "Bar", cassandraMapping: [primaryKey:"color"])
		def uuid = UuidDynamicMethods.timeUUID(Date.parse("yyyy-MM-dd HH:mm z","2012-11-15 12:00 GMT").time)

		assertEquals "10000", KeyHelper.primaryRowKey(10000)
		assertEquals "true", KeyHelper.primaryRowKey(true)
		assertEquals "SomeString", KeyHelper.primaryRowKey("SomeString")
		assertEquals "001352980800000_$uuid".toString(), KeyHelper.primaryRowKey(uuid)
		assertEquals "RED", KeyHelper.primaryRowKey(Color.RED)
		assertEquals "X15", KeyHelper.primaryRowKey(mockObject)
		assertEquals "SITE01__false__001352980800000_${uuid}__2000__X15__PURPLE".toString(), KeyHelper.primaryRowKey(["SITE01", false, uuid, 2000, mockObject, Color.PURPLE])
	}
}
