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
import com.reachlocal.grails.plugins.cassandra.utils.DateRangeParser

/**
 * @author: Bob Florian
 */
class DateRangeParserTests extends GroovyTestCase
{
	static df = new SimpleDateFormat('yyyy-MM-dd')
	static tf = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss')
	static tsf = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss.SSS')

	void testOne_Hour()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-02-22 15:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-02-23 20:00:00'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 1, ranges.size()
		assertEquals Calendar.HOUR_OF_DAY, ranges[0].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals cal2.time, ranges[0].finish
	}

	void testOne_Day()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2011-01-01 00:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2011-01-01 23:59:59'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 1, ranges.size()
		assertEquals Calendar.DAY_OF_MONTH, ranges[0].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals cal2.time, ranges[0].finish
	}

	void testOne_Month()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2011-01-01 00:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2011-01-31 23:00:00'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 1, ranges.size()
		assertEquals Calendar.MONTH, ranges[0].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals cal2.time, ranges[0].finish
	}

	void testTwo_HourDay()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-02-22 15:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-02-24 23:59:59'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 2, ranges.size()
		assertEquals Calendar.HOUR_OF_DAY, ranges[0].grain
		assertEquals Calendar.DAY_OF_MONTH, ranges[1].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-02-22 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-02-23 00:00:00.000'), ranges[1].start
		assertEquals cal2.time, ranges[1].finish
	}

	void testTwo_HourMonth()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-03-31 15:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-05-31 23:59:59'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 2, ranges.size()
		assertEquals Calendar.HOUR_OF_DAY, ranges[0].grain
		assertEquals Calendar.MONTH, ranges[1].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-03-31 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-04-01 00:00:00.000'), ranges[1].start
		assertEquals cal2.time, ranges[1].finish
	}

	void testTwo_DayHour()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-03-15 00:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-03-16 11:59:59'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 2, ranges.size()
		assertEquals Calendar.DAY_OF_MONTH, ranges[0].grain
		assertEquals Calendar.HOUR_OF_DAY, ranges[1].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-03-15 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-03-16 00:00:00.000'), ranges[1].start
		assertEquals cal2.time, ranges[1].finish
	}

	void testThree_HourDayHour()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-02-22 14:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-03-20 17:00:00'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 3, ranges.size()
		assertEquals Calendar.HOUR_OF_DAY, ranges[0].grain
		assertEquals Calendar.DAY_OF_MONTH, ranges[1].grain
		assertEquals Calendar.HOUR_OF_DAY, ranges[2].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-02-22 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-02-23 00:00:00.000'), ranges[1].start
		assertEquals tsf.parse('2012-03-19 23:59:59.999'), ranges[1].finish
		assertEquals tsf.parse('2012-03-20 00:00:00.000'), ranges[2].start
		assertEquals cal2.time, ranges[2].finish
	}

	void testThree_HourDayMonth()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-02-22 14:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-04-30 23:00:00'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 3, ranges.size()
		assertEquals Calendar.HOUR_OF_DAY, ranges[0].grain
		assertEquals Calendar.DAY_OF_MONTH, ranges[1].grain
		assertEquals Calendar.MONTH, ranges[2].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-02-22 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-02-23 00:00:00.000'), ranges[1].start
		assertEquals tsf.parse('2012-02-29 23:59:59.999'), ranges[1].finish
		assertEquals tsf.parse('2012-03-01 00:00:00.000'), ranges[2].start
		assertEquals cal2.time, ranges[2].finish
	}

	void testThree_HourMonthHour()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-03-31 14:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-05-01 17:00:00'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 3, ranges.size()
		assertEquals Calendar.HOUR_OF_DAY, ranges[0].grain
		assertEquals Calendar.MONTH, ranges[1].grain
		assertEquals Calendar.HOUR_OF_DAY, ranges[2].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-03-31 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-04-01 00:00:00.000'), ranges[1].start
		assertEquals tsf.parse('2012-04-30 23:59:59.999'), ranges[1].finish
		assertEquals tsf.parse('2012-05-01 00:00:00.000'), ranges[2].start
		assertEquals cal2.time, ranges[2].finish
	}

	void testThree_HourMonthDay()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-03-31 14:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-05-01 23:00:00'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 3, ranges.size()
		assertEquals Calendar.HOUR_OF_DAY, ranges[0].grain
		assertEquals Calendar.MONTH, ranges[1].grain
		assertEquals Calendar.DAY_OF_MONTH, ranges[2].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-03-31 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-04-01 00:00:00.000'), ranges[1].start
		assertEquals tsf.parse('2012-04-30 23:59:59.999'), ranges[1].finish
		assertEquals tsf.parse('2012-05-01 00:00:00.000'), ranges[2].start
		assertEquals cal2.time, ranges[2].finish
	}

	void testThree_DayMonthHour()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-03-31 00:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-05-01 22:00:00'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 3, ranges.size()
		assertEquals Calendar.DAY_OF_MONTH, ranges[0].grain
		assertEquals Calendar.MONTH, ranges[1].grain
		assertEquals Calendar.HOUR_OF_DAY, ranges[2].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-03-31 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-04-01 00:00:00.000'), ranges[1].start
		assertEquals tsf.parse('2012-04-30 23:59:59.999'), ranges[1].finish
		assertEquals tsf.parse('2012-05-01 00:00:00.000'), ranges[2].start
		assertEquals cal2.time, ranges[2].finish
	}

	void testThree_DayMonthDay()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-03-31 00:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-05-02 23:00:00'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 3, ranges.size()
		assertEquals Calendar.DAY_OF_MONTH, ranges[0].grain
		assertEquals Calendar.MONTH, ranges[1].grain
		assertEquals Calendar.DAY_OF_MONTH, ranges[2].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-03-31 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-04-01 00:00:00.000'), ranges[1].start
		assertEquals tsf.parse('2012-04-30 23:59:59.999'), ranges[1].finish
		assertEquals tsf.parse('2012-05-01 00:00:00.000'), ranges[2].start
		assertEquals cal2.time, ranges[2].finish
	}

	void testThree_MonthDayHour()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-02-01 00:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-04-15 22:00:00'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 3, ranges.size()
		assertEquals Calendar.MONTH, ranges[0].grain
		assertEquals Calendar.DAY_OF_MONTH, ranges[1].grain
		assertEquals Calendar.HOUR_OF_DAY, ranges[2].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-03-31 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-04-01 00:00:00.000'), ranges[1].start
		assertEquals tsf.parse('2012-04-14 23:59:59.999'), ranges[1].finish
		assertEquals tsf.parse('2012-04-15 00:00:00.000'), ranges[2].start
		assertEquals cal2.time, ranges[2].finish
	}

	void testFour_HourDayMonthDay()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-02-22 15:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-04-15 23:00:00'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 4, ranges.size()
		assertEquals Calendar.HOUR_OF_DAY, ranges[0].grain
		assertEquals Calendar.DAY_OF_MONTH, ranges[1].grain
		assertEquals Calendar.MONTH, ranges[2].grain
		assertEquals Calendar.DAY_OF_MONTH, ranges[3].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-02-22 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-02-23 00:00:00.000'), ranges[1].start
		assertEquals tsf.parse('2012-02-29 23:59:59.999'), ranges[1].finish
		assertEquals tsf.parse('2012-03-01 00:00:00.000'), ranges[2].start
		assertEquals tsf.parse('2012-03-31 23:59:59.999'), ranges[2].finish
		assertEquals tsf.parse('2012-04-01 00:00:00.000'), ranges[3].start
		assertEquals cal2.time, ranges[3].finish
	}

	void testFour_HourDayMonthHour()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-02-22 15:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-04-01 22:30:00'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 4, ranges.size()
		assertEquals Calendar.HOUR_OF_DAY, ranges[0].grain
		assertEquals Calendar.DAY_OF_MONTH, ranges[1].grain
		assertEquals Calendar.MONTH, ranges[2].grain
		assertEquals Calendar.HOUR_OF_DAY, ranges[3].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-02-22 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-02-23 00:00:00.000'), ranges[1].start
		assertEquals tsf.parse('2012-02-29 23:59:59.999'), ranges[1].finish
		assertEquals tsf.parse('2012-03-01 00:00:00.000'), ranges[2].start
		assertEquals tsf.parse('2012-03-31 23:59:59.999'), ranges[2].finish
		assertEquals tsf.parse('2012-04-01 00:00:00.000'), ranges[3].start
		assertEquals cal2.time, ranges[3].finish
	}

	void testFour_HourMonthDayHour()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-04-30 07:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-06-15 21:00:00'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 4, ranges.size()
		assertEquals Calendar.HOUR_OF_DAY, ranges[0].grain
		assertEquals Calendar.MONTH, ranges[1].grain
		assertEquals Calendar.DAY_OF_MONTH, ranges[2].grain
		assertEquals Calendar.HOUR_OF_DAY, ranges[3].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-04-30 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-05-01 00:00:00.000'), ranges[1].start
		assertEquals tsf.parse('2012-05-31 23:59:59.999'), ranges[1].finish
		assertEquals tsf.parse('2012-06-01 00:00:00.000'), ranges[2].start
		assertEquals tsf.parse('2012-06-14 23:59:59.999'), ranges[2].finish
		assertEquals tsf.parse('2012-06-15 00:00:00.000'), ranges[3].start
		assertEquals cal2.time, ranges[3].finish
	}

	void testFour_DayMonthDayHour()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-04-26 00:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-06-15 21:00:00'))

		def parser = new DateRangeParser(cal1, cal2)
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 4, ranges.size()
		assertEquals Calendar.DAY_OF_MONTH, ranges[0].grain
		assertEquals Calendar.MONTH, ranges[1].grain
		assertEquals Calendar.DAY_OF_MONTH, ranges[2].grain
		assertEquals Calendar.HOUR_OF_DAY, ranges[3].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-04-30 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-05-01 00:00:00.000'), ranges[1].start
		assertEquals tsf.parse('2012-05-31 23:59:59.999'), ranges[1].finish
		assertEquals tsf.parse('2012-06-01 00:00:00.000'), ranges[2].start
		assertEquals tsf.parse('2012-06-14 23:59:59.999'), ranges[2].finish
		assertEquals tsf.parse('2012-06-15 00:00:00.000'), ranges[3].start
		assertEquals cal2.time, ranges[3].finish
	}

	void testAllFive()
	{
		def cal1 = Calendar.getInstance()
		cal1.setTime(tf.parse('2012-02-22 15:00:00'))

		def cal2 = Calendar.getInstance()
		cal2.setTime(tf.parse('2012-04-15 08:00:00'))

		def parser = new DateRangeParser(cal1, cal2)
		println parser
		def ranges = parser.getDateRanges()
		println ranges

		assertEquals 5, ranges.size()
		assertEquals Calendar.HOUR_OF_DAY, ranges[0].grain
		assertEquals Calendar.DAY_OF_MONTH, ranges[1].grain
		assertEquals Calendar.MONTH, ranges[2].grain
		assertEquals Calendar.DAY_OF_MONTH, ranges[3].grain
		assertEquals Calendar.HOUR_OF_DAY, ranges[4].grain
		assertEquals cal1.time, ranges[0].start
		assertEquals tsf.parse('2012-02-22 23:59:59.999'), ranges[0].finish
		assertEquals tsf.parse('2012-02-23 00:00:00.000'), ranges[1].start
		assertEquals tsf.parse('2012-02-29 23:59:59.999'), ranges[1].finish
		assertEquals tsf.parse('2012-03-01 00:00:00.000'), ranges[2].start
		assertEquals tsf.parse('2012-03-31 23:59:59.999'), ranges[2].finish
		assertEquals tsf.parse('2012-04-01 00:00:00.000'), ranges[3].start
		assertEquals tsf.parse('2012-04-14 23:59:59.999'), ranges[3].finish
		assertEquals tsf.parse('2012-04-15 00:00:00.000'), ranges[4].start
		assertEquals cal2.time, ranges[4].finish
	}
}
