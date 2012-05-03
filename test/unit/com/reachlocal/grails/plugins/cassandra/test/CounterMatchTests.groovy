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

import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroup
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroupMeeting
import grails.test.GrailsUnitTestCase
import java.text.SimpleDateFormat
import com.reachlocal.grails.plugins.cassandra.mapping.MappingUtils

/**
 * @author: Bob Florian
 */
class CounterMatchTests extends GrailsUnitTestCase
{
	void testFindExact1()
	{
		def counters = [
				[findBy: ['state'], groupBy: ['city']],
				[groupBy: ['birthDate']],
				[findBy: ['gender'], groupBy: ['birthDate']],
				[groupBy: ['birthDate','state']],
				[findBy: ['gender'], groupBy: ['birthDate','city']],
		]

		def filters = MappingUtils.expandFilters([state:'MD'])
		def result = MappingUtils.findCounter(counters, filters, ['city'])
		assertEquals "city", result.groupBy[0]
		assertEquals 1, result.findBy.size()
	}

	void testFindExact2()
	{
		def counters = [
				[findBy: ['state'], groupBy: ['city']],
				[groupBy: ['birthDate']],
				[findBy: ['gender'], groupBy: ['birthDate']],
				[groupBy: ['birthDate','state']],
				[findBy: ['gender'], groupBy: ['birthDate','city']],
				[findBy: ['gender','status'], groupBy: ['birthDate','city']],
		]

		def filters = MappingUtils.expandFilters([status:'Married', gender:'Male'])
		def result = MappingUtils.findCounter(counters, filters, ['city'])
		assertEquals "birthDate", result.groupBy[0]
		assertEquals "city", result.groupBy[1]
		assertEquals 2, result.findBy.size()
	}

	void testFindBest1()
	{
		def counters = [
				[findBy: ['state'], groupBy: ['city']],
				[groupBy: ['birthDate']],
				[findBy: ['gender'], groupBy: ['birthDate']],
				[groupBy: ['birthDate','state']],
				[findBy: ['gender'], groupBy: ['birthDate','city']],
		]

		def filters = MappingUtils.expandFilters([gender:'Female', city:'Olney'])
		def result = MappingUtils.findCounter(counters, filters, [])
		assertEquals "birthDate", result.groupBy[0]
		assertEquals "city", result.groupBy[1]
		assertEquals 1, result.findBy.size()
	}

	void testFindBest1Fail()
	{
		def counters = [
				[findBy: ['state'], groupBy: ['city']],
				[groupBy: ['birthDate']],
				[findBy: ['gender'], groupBy: ['birthDate']],
				[groupBy: ['birthDate','state']],
				[findBy: ['gender'], groupBy: ['birthDate','city']],
		]

		def filters = MappingUtils.expandFilters([status:'Female', state:'MD'])
		def result = MappingUtils.findCounter(counters, filters, [])
		assertNull result
	}

	void testFindBest2()
	{
		def counters = [
				[findBy: ['state'], groupBy: ['city']],
				[findBy: ['gender'], groupBy: ['birthDate']],
				[findBy: ['gender'], groupBy: ['birthDate','city']],
				[findBy: ['gender'], groupBy: ['birthDate','city','zip5','zip4']],
		]

		def filters = MappingUtils.expandFilters([gender:'Female', city:'Olney'])
		def result = MappingUtils.findCounter(counters, filters, [])
		assertEquals "birthDate", result.groupBy[0]
		assertEquals "city", result.groupBy[1]
		assertEquals 2, result.groupBy.size()
		assertEquals 1, result.findBy.size()
	}

	void testFindExactAfterBest()
	{
		def counters = [
				[findBy: ['state'], groupBy: ['city']],
				[findBy: ['gender'], groupBy: ['birthDate']],
				[findBy: ['gender'], groupBy: ['birthDate','city']],
				[findBy: ['gender'], groupBy: ['birthDate','city','zip5','zip4']],
				[findBy: ['gender','city'], groupBy: ['birthDate','zip5','zip4']],
		]

		def filters = MappingUtils.expandFilters([gender:'Female', city:'Olney'])
		def result = MappingUtils.findCounter(counters, filters, [])
		assertEquals "birthDate", result.groupBy[0]
		assertEquals "zip5", result.groupBy[1]
		assertEquals 2, result.findBy.size()
	}

	void testFindExactBeforeBest()
	{
		def counters = [
				[findBy: ['state'], groupBy: ['city']],
				[findBy: ['gender','city'], groupBy: ['birthDate','zip5','zip4']],
				[findBy: ['gender'], groupBy: ['birthDate']],
				[findBy: ['gender'], groupBy: ['birthDate','city']],
				[findBy: ['gender'], groupBy: ['birthDate','city','zip5','zip4']],
		]

		def filters = MappingUtils.expandFilters([gender:'Female', city:'Olney'])
		def result = MappingUtils.findCounter(counters, filters, [])
		assertEquals "birthDate", result.groupBy[0]
		assertEquals "zip5", result.groupBy[1]
		assertEquals 2, result.findBy.size()
	}

	void testFindBestGroupOptimum1()
	{
		def counters = [
				[findBy: ['state'], groupBy: ['city']],
				[findBy: ['gender'], groupBy: ['birthDate']],
				[findBy: ['gender'], groupBy: ['birthDate','city','zip5','zip4']],
				[findBy: ['gender'], groupBy: ['birthDate','city']],
		]

		def filters = MappingUtils.expandFilters([gender:'Female', city:'Olney'])
		def result = MappingUtils.findCounter(counters, filters, ['city'])
		assertEquals "birthDate", result.groupBy[0]
		assertEquals "city", result.groupBy[1]
		assertEquals 2, result.groupBy.size()
		assertEquals 1, result.findBy.size()
	}

	void testFindBestGroupOptimum2()
	{
		def counters = [
				[findBy: ['state'], groupBy: ['city']],
				[findBy: ['gender'], groupBy: ['birthDate']],
				[findBy: ['gender'], groupBy: ['birthDate','city','zip5','zip4']],
				[findBy: ['gender'], groupBy: ['birthDate','city','zip5']],
				[findBy: ['gender'], groupBy: ['birthDate','city']],
		]

		def filters = MappingUtils.expandFilters([gender:'Female', city:'Olney'])
		def result = MappingUtils.findCounter(counters, filters, ['city','zip5'])
		assertEquals "birthDate", result.groupBy[0]
		assertEquals "city", result.groupBy[1]
		assertEquals 3, result.groupBy.size()
		assertEquals 1, result.findBy.size()
	}
}
