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

import java.text.SimpleDateFormat
import com.reachlocal.grails.plugins.cassandra.utils.DateHelper
import org.junit.Test;
import static org.junit.Assert.*

/**
 * @author: Bob Florian
 */
class DateHelperTests extends GroovyTestCase
{
	static df = new SimpleDateFormat('yyyy-MM-dd')
	static tf = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss')
	static tsf = new SimpleDateFormat("yyyy-MM-dd'T'HH")

	@Test
	void testFillDates()
	{
		def hours = [
				('2012-02-04T16'): 2,
				//('2012-02-04T17'): 4,
				('2012-02-04T18'): 2,
				('2012-02-04T19'): 5,
				//('2012-02-04T20'): 1,
				//('2012-02-04T21'): 4,
				('2012-02-04T22'): 1,
				('2012-02-04T23'): 2,
				//('2012-02-05T00'): 4,
				//('2012-02-05T01'): 2,
				//('2012-02-05T02'): 1,
				('2012-02-05T04'): 2,
				('2012-02-05T05'): 1
		]

		def hours2 = DateHelper.fillDates(hours, Calendar.HOUR_OF_DAY)
		println hours2
		assertEquals 2, hours2['2012-02-04T16']
		assertEquals 2, hours2['2012-02-04T18']
		assertEquals 5, hours2['2012-02-04T19']
		assertEquals 1, hours2['2012-02-04T22']
		assertEquals 2, hours2['2012-02-04T23']
		assertEquals 2, hours2['2012-02-05T04']
		assertEquals 1, hours2['2012-02-05T05']
		assertTrue hours2.keySet().contains('2012-02-04T17')
		assertTrue hours2.keySet().contains('2012-02-04T20')
		assertTrue hours2.keySet().contains('2012-02-04T21')
		assertTrue hours2.keySet().contains('2012-02-05T00')
		assertTrue hours2.keySet().contains('2012-02-05T01')
		assertTrue hours2.keySet().contains('2012-02-05T02')
		assertTrue hours2.keySet().contains('2012-02-05T05')
	}

	@Test
	void testFillDatesDay()
	{
		def hours = [
				('2012-01-15'): 2,
				('2012-02-01'): 2,
				('2012-02-02'): 5,
				('2012-02-04'): 1,
				('2012-02-05'): 2,
				('2012-02-10'): 2,
				('2012-02-15'): 1
		]

		def hours2 = DateHelper.fillDates(hours, Calendar.DAY_OF_MONTH)
		println hours2
		assertEquals 32, hours2.size()
		assertEquals 2, hours2['2012-01-15']
		assertEquals 2, hours2['2012-02-01']
		assertEquals 5, hours2['2012-02-02']
		assertEquals 1, hours2['2012-02-04']
		assertEquals 2, hours2['2012-02-05']
		assertEquals 2, hours2['2012-02-10']
		assertEquals 1, hours2['2012-02-15']
	}

	@Test
	void testLastDayOfMonth()
	{
		def cal = Calendar.getInstance()
		cal.setTime(df.parse('2012-04-30'))
		def isLast = DateHelper.isLastDayOfMonth(cal)

		cal.setTime(df.parse('2012-04-29'))
		assertFalse DateHelper.isLastDayOfMonth(cal)

		cal.setTime(df.parse('2012-04-01'))
		assertFalse DateHelper.isLastDayOfMonth(cal)
	}

	@Test
	void testSetBeginningOfWholeMonth()
	{
		def cal = Calendar.getInstance()
		def time = df.parse('2012-05-01')
		cal.setTime(time)

		def cal2 = DateHelper.setBeginningOfWholeMonth(cal)
		println tf.format(cal2.time)
		assertEquals cal2.get(Calendar.MONTH), cal2.get(Calendar.MONTH)

		cal.setTime(df.parse('2012-05-02'))
		cal2 = DateHelper.setBeginningOfWholeMonth(cal)
		println tf.format(cal2.time)
		assertEquals cal.get(Calendar.MONTH)+1, cal2.get(Calendar.MONTH)

		cal.setTime(tf.parse('2012-07-01 01:00:00'))
		cal2 = DateHelper.setBeginningOfWholeMonth(cal)
		println tf.format(cal2.time)
		assertEquals cal.get(Calendar.MONTH)+1, cal2.get(Calendar.MONTH)

		cal.setTime(tf.parse('2012-07-01 00:55:00'))
		cal2 = DateHelper.setBeginningOfWholeMonth(cal)
		println tf.format(cal2.time)
		assertEquals cal.get(Calendar.MONTH), cal2.get(Calendar.MONTH)
	}

	@Test
	void testSetEndOfWholeMonth()
	{
		def cal = Calendar.getInstance()
		def time = tf.parse('2012-05-31 23:00:00')
		cal.setTime(time)

		def cal2 = DateHelper.setEndOfWholeMonth(cal)
		println tf.format(cal2.time)
		assertEquals cal2.get(Calendar.MONTH), cal2.get(Calendar.MONTH)

		cal.setTime(df.parse('2012-05-30'))
		cal2 = DateHelper.setEndOfWholeMonth(cal)
		println tf.format(cal2.time)
		assertEquals cal.get(Calendar.MONTH)-1, cal2.get(Calendar.MONTH)

		cal.setTime(tf.parse('2012-07-31 22:00:00'))
		cal2 = DateHelper.setEndOfWholeMonth(cal)
		println tf.format(cal2.time)
		assertEquals cal.get(Calendar.MONTH)-1, cal2.get(Calendar.MONTH)

		cal.setTime(df.parse('2012-07-31'))
		cal2 = DateHelper.setEndOfWholeMonth(cal)
		println tf.format(cal2.time)
		assertEquals cal.get(Calendar.MONTH)-1, cal2.get(Calendar.MONTH)

	}

	@Test
	void testSetBeginningOfWholeDay()
	{
		def cal = Calendar.getInstance()
		def time = df.parse('2012-05-01')
		cal.setTime(time)

		def cal2 = DateHelper.setBeginningOfWholeDay(cal)
		println tf.format(cal2.time)
		assertEquals cal2.get(Calendar.DAY_OF_MONTH), cal2.get(Calendar.DAY_OF_MONTH)

		cal.setTime(tf.parse('2012-07-01 00:59:00'))
		cal2 = DateHelper.setBeginningOfWholeDay(cal)
		println tf.format(cal2.time)
		assertEquals cal.get(Calendar.DAY_OF_MONTH), cal2.get(Calendar.DAY_OF_MONTH)

		cal.setTime(tf.parse('2012-07-01 01:00:00'))
		cal2 = DateHelper.setBeginningOfWholeDay(cal)
		println tf.format(cal2.time)
		assertEquals cal.get(Calendar.DAY_OF_MONTH)+1, cal2.get(Calendar.DAY_OF_MONTH)
	}

	@Test
	void testSetEndOfWholeDay()
	{
		def cal = Calendar.getInstance()
		def time = tf.parse('2012-05-31 23:59:59')
		cal.setTime(time)

		def cal2 = DateHelper.setEndOfWholeDay(cal)
		println tf.format(cal2.time)
		assertEquals cal2.get(Calendar.DAY_OF_MONTH), cal2.get(Calendar.DAY_OF_MONTH)

		cal.setTime(tf.parse('2012-05-31 23:00:00'))
		cal2 = DateHelper.setEndOfWholeDay(cal)
		println tf.format(cal2.time)
		assertEquals cal.get(Calendar.DAY_OF_MONTH), cal2.get(Calendar.DAY_OF_MONTH)

		cal.setTime(tf.parse('2012-07-31 22:00:00'))
		cal2 = DateHelper.setEndOfWholeDay(cal)
		println tf.format(cal2.time)
		assertEquals cal.get(Calendar.DAY_OF_MONTH)-1, cal2.get(Calendar.DAY_OF_MONTH)

		cal.setTime(df.parse('2012-07-31'))
		cal2 = DateHelper.setEndOfWholeDay(cal)
		println tf.format(cal2.time)
		assertEquals cal.get(Calendar.DAY_OF_MONTH)-1, cal2.get(Calendar.DAY_OF_MONTH)

	}

	@Test
	void testNoMonth()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-05-01 10:00:00'))
		cal1 = DateHelper.setBeginningOfWholeMonth(cal1)
		println cal1.time

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-05-30 10:00:00'))
		cal2 = DateHelper.setEndOfWholeMonth(cal2)
		println cal2.time

		assertTrue cal1.time.after(cal2.time)
	}

	@Test
	void testRollUpCounterDates()
	{
		def hf = new SimpleDateFormat("yyyy-MM-dd'T'HH")
		def df = new SimpleDateFormat("yyyy-MM-dd")

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

		def days = DateHelper.rollUpCounterDates(hours, hf, df)
		println days
		assertEquals 2, days.size()
		assertEquals 21, days['2012-02-04']
		assertEquals 10, days['2012-02-05']
	}

	@Test
	void testRollUpCounterDatesMap()
	{
		def hf = new SimpleDateFormat("yyyy-MM-dd'T'HH")
		def df = new SimpleDateFormat("yyyy-MM-dd")

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

		def days = DateHelper.rollUpCounterDates(hours, hf, df)
		println days
		assertEquals 2, days.size()
		assertEquals 8, days['2012-02-04'].direct
		assertEquals 12, days['2012-02-04'].campaign
	}

	@Test
	void testRollUpCounterDatesMapTiming()
	{
		def hf = new SimpleDateFormat("yyyy-MM-dd'T'HH")
		def df = new SimpleDateFormat("yyyy-MM-dd")

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
		def days = DateHelper.rollUpCounterDates(hours, hf, df)
		def elapsed = System.currentTimeMillis() - t0;
		//println days
		println "${hours.size()} items in $elapsed msec."

		//assertEquals 4, days['2011-01-02'].direct
	}
}
