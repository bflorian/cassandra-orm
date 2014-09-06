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

import com.reachlocal.grails.plugins.cassandra.test.orm.User
import com.reachlocal.grails.plugins.cassandra.test.orm.Visit
import com.reachlocal.grails.plugins.cassandra.utils.OrmHelper

/**
 * @author: Bob Florian
 */
class OrmHelperTests
{

	@Test
	void testMethodForPropertyName()
	{
		assertEquals "getSomeOtherProperty", OrmHelper.methodForPropertyName("get", "someOtherProperty")
		assertEquals "setX", OrmHelper.methodForPropertyName("set", "x")
	}

	@Test
	void testPropertyListFromMethodName()
	{
		assertEquals "name", OrmHelper.propertyListFromMethodName("Name")[0]
		assertEquals "name", OrmHelper.propertyListFromMethodName("NameAndRank")[0]
		assertEquals "rank", OrmHelper.propertyListFromMethodName("NameAndRank")[1]
	}

	@Test
	void testPropertyPropertyNameFromClassName()
	{
		assertEquals "websiteVisit", OrmHelper.propertyNameFromClassName("WebsiteVisit")
		assertEquals "visit", OrmHelper.propertyNameFromClassName("Visit")
		assertEquals "pD", OrmHelper.propertyNameFromClassName("PD")
	}

	@Test
	void testCollection()
	{
		assertEquals "name", OrmHelper.collection("name")[0]
		assertEquals "rank", OrmHelper.collection(["rank"])[0]
	}

	@Test
	void testContainsElement()
	{
		assertTrue OrmHelper.containsElement([1,2,3,4],[4,16,32])
		assertFalse OrmHelper.containsElement([1,3,5,7],[2,4,6,8])
	}

	@Test
	void testSafeGetProperty()
	{
		def visit = new Visit()
		def user = new User()

		assertEquals 0, OrmHelper.safeGetProperty(user, "transients", List, []).size()
		assertEquals "ageInDays", OrmHelper.safeGetProperty(visit, "transients", List, [])[0]
	}

	@Test
	void testAddOptionDefaults()
	{
		assertEquals 1000, OrmHelper.addOptionDefaults([:], 1000).max
		assertEquals 50, OrmHelper.addOptionDefaults([max:50], 1000).max
		assertEquals false, OrmHelper.addOptionDefaults([max:50], 1000).reversed
		assertEquals true, OrmHelper.addOptionDefaults([max:50, reversed:true], 1000).reversed
		assertEquals false, OrmHelper.addOptionDefaults([max:50, reversed:false], 1000).reversed
		assertNull OrmHelper.addOptionDefaults([max:50], 1000).cluster
		assertEquals "xxx", OrmHelper.addOptionDefaults([max:50], 1000, "xxx").cluster
		assertEquals "yyy", OrmHelper.addOptionDefaults([max:50, cluster: "yyy"], 1000, "xxx").cluster
		assertEquals "foobar", OrmHelper.addOptionDefaults([max:50, cluster:"xxx", foo:"foobar"], 1000).foo
	}

	@Test
	void testExpandedNestedArray()
	{
		def key1 = OrmHelper.expandNestedArray(["one","two","three"])
		println key1
		assertEquals 1, key1.size()
		assertEquals key1[0][0], "one"
		assertEquals key1[0][1], "two"
		assertEquals key1[0][2], "three"

		def key1a = OrmHelper.expandNestedArray([["one","oneA"]])
		println key1a
		assertEquals 1, key1.size()
		assertEquals key1a[0][0], "one"
		assertEquals key1a[1][0], "oneA"

		def key2 = OrmHelper.expandNestedArray(["one",["twoA","twoB"],"three"])
		println key2
		assertEquals 2, key2.size()
		assertEquals key2[0][0], "one"
		assertEquals key2[0][1], "twoA"
		assertEquals key2[0][2], "three"
		assertEquals key2[1][0], "one"
		assertEquals key2[1][1], "twoB"
		assertEquals key2[1][2], "three"

		def key3 = OrmHelper.expandNestedArray(["one",["twoA","twoB"],["threeX","threeY"]])
		println key3
		assertEquals 4, key3.size()
		assertEquals key3[0][0], "one"
		assertEquals key3[0][1], "twoA"
		assertEquals key3[0][2], "threeX"
		assertEquals key3[1][0], "one"
		assertEquals key3[1][1], "twoA"
		assertEquals key3[1][2], "threeY"
		assertEquals key3[2][0], "one"
		assertEquals key3[2][1], "twoB"
		assertEquals key3[2][2], "threeX"
		assertEquals key3[3][0], "one"
		assertEquals key3[3][1], "twoB"
		assertEquals key3[3][2], "threeY"
	}
}
