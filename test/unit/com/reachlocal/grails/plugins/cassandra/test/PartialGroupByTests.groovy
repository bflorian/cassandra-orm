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
import com.reachlocal.grails.plugins.cassandra.mapping.MappingUtils
import java.text.SimpleDateFormat
import org.junit.Test
import static org.junit.Assert.*

/**
 * @author: Bob Florian
 */
class PartialGroupByTests extends OrmTestCase
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
		new User(uuid: 'y8', period: 'Month', gender: 'Female', state: 'CA', city: 'Woodland Hills', birthDate:  DAY_FORMAT.parse('1992-03-15')).save()
	}

	@Test
	void testGetCountersOneMultiWhereGroupByOneColumn()
	{
		def cols = MappingUtils.getCounters(User,
				User.cassandraMapping.counters,
				[gender:  'Male'],
				['state'],
				null, null, null, null, null, null, null, null, null)

		println cols
		assertEquals 3, cols.size()
		assertEquals 3, cols.MD
		assertEquals 2, cols.VA
		assertEquals 2, cols.CA
	}

	@Test
	void testGetCountersTwoMultiWhereGroupByOneColumn()
	{
		def cols = MappingUtils.getCounters(User,
				User.cassandraMapping.counters,
				[gender:  'Female', period: 'Month'],
				['city'],
				null, null, null, null, null, null, null, null, null)

		println cols
		assertEquals 3, cols.size()
		assertEquals 1, cols.Reston
		assertEquals 1, cols.Rockville
		assertEquals 2, cols["Woodland Hills"]
	}

	@Test
	void testGetCountersOneMultiWhereGroupByDateColumn()
	{
		def cols = MappingUtils.getCounters(User,
				User.cassandraMapping.counters,
				[gender:  'Male'],
				['birthDate'],
				null, null, null, null, Calendar.YEAR, null, null, null, null)

		println cols
		assertEquals 4, cols.size()
		assertEquals 4, cols["1980"]
		assertEquals 1, cols["1981"]
		assertEquals 1, cols["1982"]
		assertEquals 1, cols["1985"]

	}

	static protected DAY_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

	static {
		DAY_FORMAT.setTimeZone(TimeZone.getTimeZone("America/New_York"))
	}
}
