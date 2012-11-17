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
import com.reachlocal.grails.plugins.cassandra.utils.NestedHashMap
import org.junit.Test
import static org.junit.Assert.*

/**
 * @author: Bob Florian
 */
class MappingUtilsTests extends GrailsUnitTestCase
{
	@Test
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

		def days = MappingUtils.rollUpCounterDates(hours, hf, Calendar.DAY_OF_MONTH, TimeZone.getDefault(), null)
		println days
		assertEquals 2, days.size()
		assertEquals 21, days['2012-02-04']
		assertEquals 10, days['2012-02-05']
	}

	@Test
	void testRollUpCounterDatesHoursStringFormat()
	{
		def hf = new SimpleDateFormat("yyyy-MM-dd'T'HH")

		def hours = [
				('2012-02-04T16'): 2,
				('2012-02-04T17'): 4,
				('2012-02-04T18'): 2,
				('2012-02-04T19'): 5,
				('2012-02-04T20'): 1,
				('2012-02-05T16'): 4,
				('2012-02-05T17'): 1,
				('2012-02-05T18'): 2,
				('2012-02-06T19'): 4,
				('2012-02-07T17'): 2,
				('2012-02-07T18'): 1,
				('2012-02-07T19'): 2,
				('2012-02-07T20'): 1
		]

		def days = MappingUtils.rollUpCounterDates(hours, hf, null, TimeZone.getDefault(), "HH")
		println days
		assertEquals 5, days.size()
		assertEquals 6, days['16']
		assertEquals 7, days['17']
		assertEquals 5, days['18']
		assertEquals 11, days['19']
		assertEquals 2, days['20']
	}

	@Test
	void testRollUpCounterDatesHoursStringFormatGMT()
	{
		def hf = new SimpleDateFormat("yyyy-MM-dd'T'HH")
		hf.setTimeZone(TimeZone.getTimeZone("EST"))

		def hours = [
				('2012-02-04T16'): 2,
				('2012-02-04T17'): 4,
				('2012-02-04T18'): 2,
				('2012-02-04T19'): 5,
				('2012-02-04T20'): 1,
				('2012-02-05T16'): 4,
				('2012-02-05T17'): 1,
				('2012-02-05T18'): 2,
				('2012-02-06T19'): 4,
				('2012-02-07T17'): 2,
				('2012-02-07T18'): 1,
				('2012-02-07T19'): 2,
				('2012-02-07T20'): 1
		]

		def days = MappingUtils.rollUpCounterDates(hours, hf, null, TimeZone.getTimeZone("GMT"), "HH")
		println days
		assertEquals 5, days.size()
		assertEquals 6, days['21']
		assertEquals 7, days['22']
		assertEquals 5, days['23']
		assertEquals 11, days['00']
		assertEquals 2, days['01']
	}

	@Test
	void testRollUpCounterDatesHoursObjectFormat()
	{
		def hf = new SimpleDateFormat("yyyy-MM-dd'T'HH")

		def hours = [
				('2012-02-04T16'): 2,
				('2012-02-04T17'): 4,
				('2012-02-04T18'): 2,
				('2012-02-04T19'): 5,
				('2012-02-04T20'): 1,
				('2012-02-05T16'): 4,
				('2012-02-05T17'): 1,
				('2012-02-05T18'): 2,
				('2012-02-06T19'): 4,
				('2012-02-07T17'): 2,
				('2012-02-07T18'): 1,
				('2012-02-07T19'): 2,
				('2012-02-07T20'): 1
		]

		def days = MappingUtils.rollUpCounterDates(hours, hf, null, null, new SimpleDateFormat("HH"))
		println days
		assertEquals 5, days.size()
		assertEquals 6, days['16']
		assertEquals 7, days['17']
		assertEquals 5, days['18']
		assertEquals 11, days['19']
		assertEquals 2, days['20']
	}

	@Test
	void testRollUpCounterDatesHoursObjectFormatGMT()
	{
		def hf = new SimpleDateFormat("yyyy-MM-dd'T'HH")
		hf.setTimeZone(TimeZone.getTimeZone("EST"))

		def hours = [
				('2012-02-04T16'): 2,
				('2012-02-04T17'): 4,
				('2012-02-04T18'): 2,
				('2012-02-04T19'): 5,
				('2012-02-04T20'): 1,
				('2012-02-05T16'): 4,
				('2012-02-05T17'): 1,
				('2012-02-05T18'): 2,
				('2012-02-06T19'): 4,
				('2012-02-07T17'): 2,
				('2012-02-07T18'): 1,
				('2012-02-07T19'): 2,
				('2012-02-07T20'): 1
		]

		def fmt = new SimpleDateFormat("HH")
		fmt.setTimeZone(TimeZone.getTimeZone("GMT"))
		def days = MappingUtils.rollUpCounterDates(hours, hf, null, null, fmt)
		println days
		assertEquals 5, days.size()
		assertEquals 6, days['21']
		assertEquals 7, days['22']
		assertEquals 5, days['23']
		assertEquals 11, days['00']
		assertEquals 2, days['01']
	}

	@Test
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

		def days = MappingUtils.rollUpCounterDates(hours, hf, Calendar.DAY_OF_MONTH, TimeZone.getDefault(), null)
		println days
		assertEquals 2, days.size()
		assertEquals 8, days['2012-02-04'].direct
		assertEquals 12, days['2012-02-04'].campaign
		assertEquals 3, days['2012-02-05'].campaign
		assertEquals 5, days['2012-02-05'].direct
	}

	@Test
	void testGroupByLevel0()
	{
		def hours = [
				('2012-02-04T14'): [direct: 1L],
				('2012-02-04T15'): [direct: 1L],
				('2012-02-04T16'): [campaign: 1L,direct: 1L],
				('2012-02-04T17'): [campaign: 3L,direct: 1L],
				('2012-02-04T18'): [campaign: 2L],
				('2012-02-04T19'): [campaign: 2L,direct: 2L,organic: 1L],
				('2012-02-04T20'): [campaign: 1L],
				('2012-02-04T21'): [campaign: 2L,organic: 2L],
				('2012-02-04T22'): [direct: 1L],
				('2012-02-04T23'): [campaign: 1L,direct: 1L],
				('2012-02-05T00'): [campaign: 2L,direct: 2L],
				('2012-02-05T01'): [campaign: 1L,direct: 1L]
		]
		def map = new NestedHashMap()
		map.putAll(hours)

		def result = map.groupBy(0)
		println result
		assertEquals hours.size(), result.size()
		assertEquals 5, result['2012-02-04T19']
	}

	@Test
	void testGroupByLevel1()
	{
		def hours = [
				('2012-02-04T14'): [direct: 1L],
				('2012-02-04T15'): [direct: 1L],
				('2012-02-04T16'): [campaign: 1L,direct: 1L],
				('2012-02-04T17'): [campaign: 3L,direct: 1L],
				('2012-02-04T18'): [campaign: 2L],
				('2012-02-04T19'): [campaign: 2L,direct: 2L,organic: 1L],
				('2012-02-04T20'): [campaign: 1L],
				('2012-02-04T21'): [campaign: 2L,organic: 2L],
				('2012-02-04T22'): [direct: 1L],
				('2012-02-04T23'): [campaign: 1L,direct: 1L],
				('2012-02-05T00'): [campaign: 2L,direct: 2L],
				('2012-02-05T01'): [campaign: 1L,direct: 1L]
		]

		def map = new NestedHashMap()
		map.putAll(hours)

		def result = map.groupBy(1)
		println result
		assertEquals 3, result.size()
		assertEquals 11, result.direct
		assertEquals 15, result.campaign
		assertEquals 3, result.organic
	}

	@Test
	void testGroupByLevel0RollupDates()
	{
		def hf = new SimpleDateFormat("yyyy-MM-dd'T'HH")

		def hours = [
				('2012-02-04T14'): [direct: 1L],
				('2012-02-04T15'): [direct: 1L],
				('2012-02-04T16'): [campaign: 1L,direct: 1L],
				('2012-02-04T17'): [campaign: 3L,direct: 1L],
				('2012-02-04T18'): [campaign: 2L],
				('2012-02-04T19'): [campaign: 2L,direct: 2L,organic: 1L],
				('2012-02-04T20'): [campaign: 1L],
				('2012-02-04T21'): [campaign: 2L,organic: 2L],
				('2012-02-04T22'): [direct: 1L],
				('2012-02-04T23'): [campaign: 1L,direct: 1L],
				('2012-02-05T00'): [campaign: 2L,direct: 2L],
				('2012-02-05T01'): [campaign: 1L,direct: 1L]
		]
		def map = new NestedHashMap()
		map.putAll(hours)

		def result = map.groupBy(0)
		def days = MappingUtils.rollUpCounterDates(result, hf, Calendar.DAY_OF_MONTH, TimeZone.getDefault(), null)
		println days
		assertEquals 2, days.size()
		assertEquals 23, days['2012-02-04']
		assertEquals 6, days['2012-02-05']
	}

	@Test
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

	@Test
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

	@Test
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

	@Test
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

	@Test
	void testMergeKeys_TwoReversed()
	{
		def keys = [

				['xx1', 'xx2', 'xx5', 'yy1', 'zz3'],
				['xx2', 'xx3', 'yy2','zz1']
		]
		def result = MappingUtils.mergeKeys(keys, 10, true)
		assertEquals 8, result.size()
		['xx1', 'xx2', 'xx3', 'xx5', 'yy1', 'yy2', 'zz1', 'zz3'].reverse().eachWithIndex {k, index ->
			assertEquals k, result[index]
		}

		def result2 = MappingUtils.mergeKeys(keys, 5, true)
		assertEquals 5, result2.size()
		['xx5', 'yy1', 'yy2', 'zz1', 'zz3'].reverse().eachWithIndex {k, index ->
			assertEquals k, result2[index]
		}
	}

	@Test
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

	@Test
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
		def days = MappingUtils.rollUpCounterDates(hours, hf, Calendar.DAY_OF_MONTH, TimeZone.getDefault(), null)
		def elapsed = System.currentTimeMillis() - t0;
		//println days
		println "${hours.size()} items in $elapsed msec."

		//assertEquals 4, days['2011-01-02'].direct
	}
}
