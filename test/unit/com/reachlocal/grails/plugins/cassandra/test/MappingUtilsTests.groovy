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
import java.text.SimpleDateFormat

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

	void testRollUpCounterDates()
	{
		def hf = new SimpleDateFormat("yyyy-MM-dd'T'HH")

		def hours = [
		('2012-02-04T16'): 2,
		('2012-02-04T17'): 4,
		('2012-02-04T18'): 2,
		('2012-02-04T19'): 5,
		('2012-02-04T20'): 1,
		('2012-02-04T21'): 4,
		('2012-02-04T22'): 1,
		('2012-02-04T23'): 2,
		('2012-02-05T00'): 4,
		('2012-02-05T01'): 2,
		('2012-02-05T02'): 1,
		('2012-02-05T04'): 2,
		('2012-02-05T05'): 1
		]

		def days = MappingUtils.rollUpCounterDates(hours, hf, [grain:Calendar.DAY_OF_MONTH, timeZone: TimeZone.getDefault()])
		println days
		assertEquals 2, days.size()
		assertEquals 21, days['2012-02-04']
		assertEquals 10, days['2012-02-05']
	}

	void testRollUpCounterDatesMap()
	{
		def hf = new SimpleDateFormat("yyyy-MM-dd'T'HH")

		def hours = [
				('2012-02-04T14'): [direct: 1],
				('2012-02-04T15'): [direct: 1],
				('2012-02-04T16'): [campaign: 1,direct: 1],
				('2012-02-04T17'): [campaign: 3,direct: 1],
				('2012-02-04T18'): [campaign: 2],
				('2012-02-04T19'): [campaign: 2,direct: 2,organic: 1],
				('2012-02-04T20'): [campaign: 1],
				('2012-02-04T21'): [campaign: 2,organic: 2],
				('2012-02-04T22'): [direct: 1],
				('2012-02-04T23'): [campaign: 1,direct: 1],
				('2012-02-05T00'): [campaign: 2,direct: 2],
				('2012-02-05T01'): [campaign: 1,direct: 3]
		]

		def days = MappingUtils.rollUpCounterDates(hours, hf, [grain:Calendar.DAY_OF_MONTH, timeZone: TimeZone.getDefault()])
		println days
		assertEquals 2, days.size()
		assertEquals 8, days['2012-02-04'].direct
		assertEquals 12, days['2012-02-04'].campaign
		assertEquals 3, days['2012-02-05'].campaign
		assertEquals 5, days['2012-02-05'].direct
	}

	void testGroupByLevel0()
	{
		def hours = [
				('2012-02-04T14'): [direct: 1],
				('2012-02-04T15'): [direct: 1],
				('2012-02-04T16'): [campaign: 1,direct: 1],
				('2012-02-04T17'): [campaign: 3,direct: 1],
				('2012-02-04T18'): [campaign: 2],
				('2012-02-04T19'): [campaign: 2,direct: 2,organic: 1],
				('2012-02-04T20'): [campaign: 1],
				('2012-02-04T21'): [campaign: 2,organic: 2],
				('2012-02-04T22'): [direct: 1],
				('2012-02-04T23'): [campaign: 1,direct: 1],
				('2012-02-05T00'): [campaign: 2,direct: 2],
				('2012-02-05T01'): [campaign: 1,direct: 1]
		]

		def result = MappingUtils.groupBy(hours, 0)
		println result
		assertEquals hours.size(), result.size()
		assertEquals 5, result['2012-02-04T19']
	}

	void testGroupByLevel1()
	{
		def hours = [
				('2012-02-04T14'): [direct: 1],
				('2012-02-04T15'): [direct: 1],
				('2012-02-04T16'): [campaign: 1,direct: 1],
				('2012-02-04T17'): [campaign: 3,direct: 1],
				('2012-02-04T18'): [campaign: 2],
				('2012-02-04T19'): [campaign: 2,direct: 2,organic: 1],
				('2012-02-04T20'): [campaign: 1],
				('2012-02-04T21'): [campaign: 2,organic: 2],
				('2012-02-04T22'): [direct: 1],
				('2012-02-04T23'): [campaign: 1,direct: 1],
				('2012-02-05T00'): [campaign: 2,direct: 2],
				('2012-02-05T01'): [campaign: 1,direct: 1]
		]

		def result = MappingUtils.groupBy(hours, 1)
		println result
		assertEquals 3, result.size()
		assertEquals 11, result.direct
		assertEquals 15, result.campaign
		assertEquals 3, result.organic
	}

	void testGroupByLevel0RollupDates()
	{
		def hf = new SimpleDateFormat("yyyy-MM-dd'T'HH")

		def hours = [
				('2012-02-04T14'): [direct: 1],
				('2012-02-04T15'): [direct: 1],
				('2012-02-04T16'): [campaign: 1,direct: 1],
				('2012-02-04T17'): [campaign: 3,direct: 1],
				('2012-02-04T18'): [campaign: 2],
				('2012-02-04T19'): [campaign: 2,direct: 2,organic: 1],
				('2012-02-04T20'): [campaign: 1],
				('2012-02-04T21'): [campaign: 2,organic: 2],
				('2012-02-04T22'): [direct: 1],
				('2012-02-04T23'): [campaign: 1,direct: 1],
				('2012-02-05T00'): [campaign: 2,direct: 2],
				('2012-02-05T01'): [campaign: 1,direct: 1]
		]

		def result = MappingUtils.groupBy(hours, 0)
		def days = MappingUtils.rollUpCounterDates(result, hf, [grain:Calendar.DAY_OF_MONTH, timeZone: TimeZone.getDefault()])
		println days
		assertEquals 2, days.size()
		assertEquals 23, days['2012-02-04']
		assertEquals 6, days['2012-02-05']
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

	void testFindCounter()
	{
		def counters = [
				[findBy: ['state'], groupBy: ['city']],
				[groupBy: ['birthDate']],
				[findBy: ['gender'], groupBy: ['birthDate']],
				[groupBy: ['birthDate','state']],
				[findBy: ['gender'], groupBy: ['birthDate','city'], dateFormat: new SimpleDateFormat("yyyy-MM-dd'T'HH")],
		]

		def result = MappingUtils.findCounter(counters, [state:'MD'], ['city'])
		assertEquals "city", result.groupBy[0]
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

	void testRollUpCounterDatesMapTiming()
	{
		def hf = new SimpleDateFormat("yyyy-MM-dd'T'HH")

		def values = [
				[direct: 1],
				[direct: 1],
				[campaign: 1,direct: 1],
				[campaign: 3,direct: 1],
				[campaign: 2],
				[campaign: 2,direct: 2,organic: 1],
				[campaign: 1],
				[campaign: 2,organic: 2],
				[direct: 1],
				[campaign: 1,direct: 1],
				[campaign: 2,direct: 2],
				[campaign: 1,direct: 1]
		]

		def hours = [:]

		def cal = Calendar.getInstance()
		cal.setTime(hf.parse("2012-02-04T00"))
		for (int index in 0..9999) {
			hours[hf.format(cal.time)] = values[index % 12].clone()
			cal.add(Calendar.HOUR_OF_DAY,1)
		}

		def t0 = System.currentTimeMillis()
		def days = MappingUtils.rollUpCounterDates(hours, hf, [grain:Calendar.DAY_OF_MONTH, timeZone: TimeZone.getDefault()])
		def elapsed = System.currentTimeMillis() - t0;
		//println days
		println "${hours.size()} items in $elapsed msec."

		//assertEquals 4, days['2011-01-02'].direct
	}
}
