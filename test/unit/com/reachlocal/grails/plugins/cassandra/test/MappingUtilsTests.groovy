/*
 * Copyright 2011 ReachLocal Inc.
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

import grails.test.GrailsUnitTestCase
import com.reachlocal.grails.plugins.cassandra.mapping.MappingUtils

/**
 * @author: Bob Florian
 */
class MappingUtilsTests extends GrailsUnitTestCase
{
	void testStringValue()
	{
		assertEquals '"value"', MappingUtils.stringValue("value")
		assertEquals '123', MappingUtils.stringValue(123)
	}

	void testMethodForPropertyName()
	{
		assertEquals "getSomeOtherProperty", MappingUtils.methodForPropertyName("get", "someOtherProperty")
		assertEquals "setX", MappingUtils.methodForPropertyName("set", "x")
	}

	void testPropertyListFromMethodName()
	{
		assertEquals "name", MappingUtils.propertyListFromMethodName("Name")[0]
		assertEquals "name", MappingUtils.propertyListFromMethodName("NameAndRank")[0]
		assertEquals "rank", MappingUtils.propertyListFromMethodName("NameAndRank")[1]
	}

	void testCollection()
	{
		assertEquals "name", MappingUtils.collection("name")[0]
		assertEquals "rank", MappingUtils.collection(["rank"])[0]
	}

	void testMakeComposite()
	{
		assertEquals "one__two", MappingUtils.makeComposite(["one","two"])
	}

	void testPrimaryKeyRowKey()
	{
		assertEquals "this", MappingUtils.primaryKeyIndexRowKey()
	}

	void testObjectIndexRowKey_Map()
	{
		assertEquals "this?name=Joe", MappingUtils.objectIndexRowKey("name", [name:"Joe", color:"blue", zip:"20832", gender:"F"])
		assertEquals "this?name=Joe&color=blue", MappingUtils.objectIndexRowKey(["name","color"], [name:"Joe", color:"blue", zip:"20832", gender:"F"])
	}
/*
	void testExpandFilters()
	{
		def filters = MappingUtils.expandFilters([eventType:'Radar', subType: 'Review'])
		assertEquals 1, filters.size()
		assertEquals 'Radar', filters[0].eventType
		assertEquals 'Review', filters[0].subType

		def subTypeCnt = new HashCounter()
		filters = MappingUtils.expandFilters([eventType:'Radar', subType: ['Review','Mention']])
		assertEquals 2, filters.size()
		filters.each {f ->
			assertEquals 'Radar', f.eventType
			assertTrue (['Review','Mention'].contains(f.subType))
			subTypeCnt.increment(f.subType)
		}
		assertEquals 1, subTypeCnt.Review
		assertEquals 1, subTypeCnt.Mention

		subTypeCnt = new HashCounter()
		def sourceCnt = new HashCounter()
		filters = MappingUtils.expandFilters([eventType:'Radar', subType: ['Review','Mention'], source:['Yelp','Google','TripAdvisor']])
		assertEquals 6, filters.size()
		filters.each {f ->
			assertEquals 'Radar', f.eventType
			assertTrue (['Review','Mention'].contains(f.subType))
			assertTrue (['Yelp','Google','TripAdvisor'].contains(f.source))
			subTypeCnt.increment(f.subType)
			sourceCnt.increment(f.source)
		}
		assertEquals 3, subTypeCnt.Review
		assertEquals 3, subTypeCnt.Mention
		assertEquals 2, sourceCnt.Yelp
		assertEquals 2, sourceCnt.Google
		assertEquals 2, sourceCnt.TripAdvisor
	}
*/
	void testFindIndex()
	{
		def indexes = ["eventType",["eventType","subType"],["eventType","subType","source"], ["eventType","subType","category"]]

		def filters = MappingUtils.expandFilters([eventType:'Radar'])
		def result = MappingUtils.findIndex(indexes, filters)
		assertEquals "eventType", result

		filters = MappingUtils.expandFilters([fromId:'xxx'])
		result = MappingUtils.findIndex(indexes, filters)
		assertNull result

		filters = MappingUtils.expandFilters([eventType:'Radar', subType:"Article"])
		result = MappingUtils.findIndex(indexes, filters)
		assertEquals 2, result.size()
		assertEquals "eventType", result[0]
		assertEquals "subType", result[1]

		filters = MappingUtils.expandFilters([eventType:'Radar', subType:"Article", source:"Yelp"])
		result = MappingUtils.findIndex(indexes, filters)
		assertEquals 3, result.size()
		assertEquals "eventType", result[0]
		assertEquals "subType", result[1]
		assertEquals "source", result[2]

		filters = MappingUtils.expandFilters([eventType:'Radar', source:"Yelp", source:"Yelp"])
		result = MappingUtils.findIndex(indexes, filters)
		assertNull result
	}

	void testMergeKeys_One()
	{
		def keys = [

				['xx1', 'xx2', 'xx5', 'yy1', 'zz3']
		]
		def result = MappingUtils.mergeKeys(keys, 10)
		assertEquals 5, result.size()
		keys[0].eachWithIndex {k, index ->
			assertEquals k, result[index]
		}

		def result2 = MappingUtils.mergeKeys(keys, 3)
		assertEquals 3, result2.size()
		['xx1', 'xx2', 'xx5'].eachWithIndex {k, index ->
			assertEquals k, result2[index]
		}
	}

	void testMergeKeys_Two()
	{
		def keys = [

				['xx1', 'xx2', 'xx5', 'yy1', 'zz3'],
				['xx2', 'xx3', 'yy2','zz1']
		]
		def result = MappingUtils.mergeKeys(keys, 10)
		assertEquals 8, result.size()
		['xx1', 'xx2', 'xx3', 'xx5', 'yy1', 'yy2', 'zz1', 'zz3'].eachWithIndex {k, index ->
			assertEquals k, result[index]
		}

		def result2 = MappingUtils.mergeKeys(keys, 5)
		assertEquals 5, result2.size()
		['xx1','xx2', 'xx3', 'xx5', 'yy1'].eachWithIndex {k, index ->
			assertEquals k, result2[index]
		}
	}

	void testMergeKeys_Three()
	{
		def keys = [

				['xx1', 'xx2', 'xx5', 'yy1', 'zz3'],
				['xx2', 'xx3', 'yy2', 'zz1'],
				['xx1', 'xx2', 'xx4', 'yy2', 'zz2']
		]
		def result = MappingUtils.mergeKeys(keys, 15)
		assertEquals 10, result.size()
		['xx1', 'xx2', 'xx3', 'xx4', 'xx5', 'yy1', 'yy2', 'zz1', 'zz2', 'zz3'].eachWithIndex {k, index ->
			assertEquals k, result[index]
		}

		def result2 = MappingUtils.mergeKeys(keys, 6)
		assertEquals 6, result2.size()
		['xx1', 'xx2', 'xx3', 'xx4', 'xx5', 'yy1'].eachWithIndex {k, index ->
			assertEquals k, result2[index]
		}
	}
}
