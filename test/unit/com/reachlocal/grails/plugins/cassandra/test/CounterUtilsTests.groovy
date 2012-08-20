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

import com.reachlocal.grails.plugins.cassandra.test.orm.User
import com.reachlocal.grails.plugins.cassandra.mapping.CounterUtils
import java.text.SimpleDateFormat
import com.reachlocal.grails.plugins.cassandra.mapping.MappingUtils
import org.junit.Test;
import static org.junit.Assert.*

/**
 * @author: Bob Florian
 */
class CounterUtilsTests extends OrmTestCase
{
	protected void setUp()
	{
		initialize()
		new User(uuid: 'x1', period: 'Month', gender: 'Male', state: 'MD', city: 'Olney', birthDate:  DAY_FORMAT.parse('1980-03-15')).save()
		new User(uuid: 'x2', period: 'Year', gender: 'Male', state: 'MD', city: 'Rockville', birthDate:  DAY_FORMAT.parse('1981-03-15')).save()
		new User(uuid: 'x3', period: 'Month', gender: 'Male', state: 'MD', city: 'Olney', birthDate:  DAY_FORMAT.parse('1980-04-15')).save()
		new User(uuid: 'x4', period: 'Year', gender: 'Male', state: 'VA', city: 'Reston', birthDate:  DAY_FORMAT.parse('1982-03-15')).save()
		new User(uuid: 'x5', period: 'Month', gender: 'Male', state: 'VA', city: 'Leesburg', birthDate:  DAY_FORMAT.parse('1980-07-15')).save()
		new User(uuid: 'x6', period: 'Year', gender: 'Male', state: 'CA', city: 'Woodland Hills', birthDate:  DAY_FORMAT.parse('1985-03-15')).save()
		new User(uuid: 'x7', period: 'Month', gender: 'Male', state: 'CA', city: 'Pleasanton', birthDate:  DAY_FORMAT.parse('1980-06-15')).save()
		new User(uuid: 'y1', period: 'Year', gender: 'Female', state: 'MD', city: 'Olney', birthDate:  DAY_FORMAT.parse('1980-03-15')).save()
		new User(uuid: 'y2', period: 'Month', gender: 'Female', state: 'MD', city: 'Rockville', birthDate:  DAY_FORMAT.parse('1983-03-15')).save()
		new User(uuid: 'y3', period: 'Year', gender: 'Female', state: 'MD', city: 'Olney', birthDate:  DAY_FORMAT.parse('1980-02-15')).save()
		new User(uuid: 'y4', period: 'Month', gender: 'Female', state: 'VA', city: 'Reston', birthDate:  DAY_FORMAT.parse('1980-01-15')).save()
		new User(uuid: 'y5', period: 'Year', gender: 'Female', state: 'VA', city: 'Leesburg', birthDate:  DAY_FORMAT.parse('1987-03-15')).save()
		new User(uuid: 'y6', period: 'Month', gender: 'Female', state: 'CA', city: 'Woodland Hills', birthDate:  DAY_FORMAT.parse('1988-03-15')).save()
		new User(uuid: 'y7', period: 'Year', gender: 'Female', state: 'CA', city: 'Pleasanton', birthDate:  DAY_FORMAT.parse('1989-03-15')).save()
	}

	@Test
	void testGetCounterColumnsSingleFilter()
	{
		def cols = CounterUtils.getCounterColumns(User, [[state: 'MD']], [], null,
				[findBy: ['state'], groupBy: ['city']],
				null, null, null, null)
		assertEquals 2, cols.size()
		assertEquals 4, cols.Olney
		assertEquals 2, cols.Rockville
		println cols
	}

	@Test
	void testGetCounterColumnsMultiFilter()
	{
		def cols = CounterUtils.getCounterColumns(User, [[state: 'MD'],[state: 'VA']], ['state'], null,
				[findBy: ['state'], groupBy: ['city']],
				null, null, null, null)
		println cols
	}

	@Test
	void testGetCounterColumnsMultiFilterDate()
	{
		def cols = CounterUtils.getDateCounterColumns(User, [[gender: 'Male'],[gender: 'Female']], ['gender'], null,
				[findBy: ['gender'], groupBy: ['birthDate','city']],
				null, null, true, null)
		println cols
	}

	@Test
	void testGetCounterColumnsSingleFilterDate2()
	{
		def cols = CounterUtils.getDateCounterColumns(User, [[gender: 'Male']], [], null,
				[findBy: ['gender'], groupBy: ['birthDate','state','city']],
				null, null, true, null)
		println cols

		//def g1 = cols.groupBy(1)
		//println g1
		//def g2 = cols.groupBy([1,2])
		//println g2
	}

	@Test
	void testGetCounterColumnsMultiFilterDate2()
	{
		def cols = CounterUtils.getDateCounterColumns(User, [[gender: 'Male'],[gender: 'Female']], ['gender'], null,
				[findBy: ['gender'], groupBy: ['birthDate','state','city']],
				null, null, true, null)
		println cols

		//def g1 = cols.groupBy( 1)
		//println g1
		//def g2 = cols.groupBy( [1,2])
		//println g2
		//def g3 = cols.groupBy( [1,2,3])
		//println g3
		//def g4 = cols.groupBy( [2])
		//println g4
	}

	@Test
	void testGetCountersMultiWherGroupByOneColumn()
	{
		def cols = MappingUtils.getCounters(User,
			User.cassandraMapping.counters,
			[gender:  ['Male','Female']],
			['state'],
			null, null, null, null, null, null, null, null)

		println cols
	}

	@Test
	void testGetCountersMultiWherGroupByTwoColumns()
	{
		def cols = MappingUtils.getCounters(User,
				User.cassandraMapping.counters,
				[gender:  ['Male','Female']],
				['state','city'],
				null, null, null, null, null, null, null, null)

		println cols
	}

	@Test
	void testGetCountersMultiWherGroupByOneRowOneColumn()
	{
		def cols = MappingUtils.getCounters(User,
				User.cassandraMapping.counters,
				[gender:  ['Male','Female']],
				['gender','city'],
				null, null, null, null, null, null, null, null)

		println cols
	}

	@Test
	void testGetCountersMultiWherGroupByOneRowTwoColumns()
	{
		def cols = MappingUtils.getCounters(User,
				User.cassandraMapping.counters,
				[gender:  ['Male','Female']],
				['gender','state','city'],
				null, null, null, null, null, null, null, null)

		println cols
	}

	@Test
	void testGetCountersMultiWherGroupByOneRowTwoColumnsReversed()
	{
		def cols = MappingUtils.getCounters(User,
				User.cassandraMapping.counters,
				[gender:  ['Male','Female']],
				['gender','city','state'],
				null, null, null, null, null, null, null, null)

		println cols
	}

	@Test
	void testGetCountersMultiWherGroupByTwoRowsTwoColumns()
	{
		def cols = MappingUtils.getCounters(User,
				User.cassandraMapping.counters,
				[gender:  ['Male','Female'], period: ['Month','Year']],
				['gender','period','state','city'],
				null, null, null, null, null, null, null, null)

		println cols
	}

	@Test
	void testGetCountersMultiWherGroupByTwoRowsTwoColumnsReversed()
	{
		def cols = MappingUtils.getCounters(User,
				User.cassandraMapping.counters,
				[gender:  ['Male','Female'], period: ['Month','Year']],
				['period','gender','state','city'],
				null, null, null, null, null, null, null, null)

		println cols
	}

	@Test
	void testGetCountersMultiWherGroupByZeroRowsTwoColumnsWithDate()
	{
		def cols = MappingUtils.getCounters(User,
				User.cassandraMapping.counters,
				[gender:  ['Male','Female'], period: ['Month','Year']],
				['birthDate','state'],
				null, null, null, null, null, null, null, null)

		println cols
	}

	@Test
	void testGetCountersMultiWherGroupByOneRowTwoColumnsWithDate()
	{
		def cols = MappingUtils.getCounters(User,
				User.cassandraMapping.counters,
				[gender:  ['Male','Female'], period: ['Month','Year']],
				['birthDate','state','gender'],
				null, null, null, null, null, null, null, null)

		println cols
	}

	@Test
	void testGetCountersMultiWherGroupByOneRowTwoColumnsWithDateAndGrain()
	{
		def cols = MappingUtils.getCounters(User,
				User.cassandraMapping.counters,
				[gender:  ['Male','Female'], period: ['Month','Year']],
				['birthDate','state','gender'],
				null, null, null, null, Calendar.YEAR, null, null, null)

		println cols
	}

	static protected DAY_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

	static {
		DAY_FORMAT.setTimeZone(TimeZone.getTimeZone("America/New_York"))
	}
}
