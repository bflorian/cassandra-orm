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

import com.reachlocal.grails.plugins.cassandra.utils.TimeZoneAdjuster
import java.text.SimpleDateFormat

/**
 * @author: Bob Florian
 */
class TimeZoneAdjusterTests extends GroovyTestCase
{
	static tf = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss')
	def dataTimeZone = TimeZone.getTimeZone("GMT");
	def clientTimeZone = TimeZone.getTimeZone("America/New_York")
	public TimeZoneAdjusterTests()
	{

	}

	void testConstructorDay()
	{
		def start = tf.parse("2012-03-01 00:00:00")
		def finish = tf.parse("2012-03-31 23:59:59")
		def adjuster = new TimeZoneAdjuster(start, finish, dataTimeZone, clientTimeZone, Calendar.DAY_OF_MONTH );
		println adjuster
	}

	void testConstructorMonth()
	{
		def start = tf.parse("2012-02-01 00:00:00")
		def finish = tf.parse("2012-07-31 23:59:59")
		def adjuster = new TimeZoneAdjuster(start, finish, dataTimeZone, clientTimeZone, Calendar.MONTH );
		println adjuster
	}

	void testMergeMonth()
	{
		def primary = [
				('2012-02') : 100L, // 100 + 4 - 6 = 98
			    ('2012-03') : 200L, // 200 + 9 - 6 = 203
				('2012-04') : 300L, // 300 + 11 - 13 = 298
				('2012-05') : 400L, // 400 + 3 - 6 = 397
				('2012-06') : 500L, // 500 + 9 - 10 = 499
		]

		def hour = [
				('2012-01-31') : [('19'):1, ('20'):0, ('21'):0, ('22'):0, ('23'):3],
				('2012-02-01') : [('19'):2, ('20'):0, ('21'):0, ('22'):0, ('23'):4],

				('2012-02-29') : [('19'):2, ('20'):0, ('21'):0, ('22'):0, ('23'):7],
				('2012-03-01') : [('19'):1, ('20'):0, ('21'):0, ('22'):0, ('23'):5],

				('2012-03-31') : [('19'):3, ('20'):0, ('21'):0, ('22'):0, ('23'):8],
				('2012-04-01') : [('19'):4, ('20'):0, ('21'):0, ('22'):0, ('23'):9],

				('2012-04-30') : [('19'):1, ('20'):0, ('21'):0, ('22'):0, ('23'):3],
				('2012-05-01') : [('19'):2, ('20'):0, ('21'):0, ('22'):0, ('23'):6],

				('2012-05-31') : [('19'):1, ('20'):0, ('21'):0, ('22'):0, ('23'):9],
				('2012-06-01') : [('19'):5, ('20'):0, ('21'):0, ('22'):0, ('23'):10]
		]

		def start = tf.parse("2012-02-01 00:00:00")
		def finish = tf.parse("2012-06-30 23:59:59")
		def adjuster = new TimeZoneAdjuster(start, finish, dataTimeZone, clientTimeZone, Calendar.MONTH );

		def counts = adjuster.mergeCounts(primary, hour)
		counts.each {k,v ->
			println "$k => $v"
		}
		assertEquals 98, counts['2012-02']
		assertEquals 203, counts['2012-03']
		assertEquals 299, counts['2012-04']
		assertEquals 397, counts['2012-05']
		assertEquals 499, counts['2012-06']
	}

	void testMergeDay()
	{
		def primary = [
				('2012-03-08') : 100L, // 100 + 4 - 6 = 98
				('2012-03-09') : 200L, // 200 + 6 - 6 = 200
				('2012-03-10') : 300L, // 300 + 6 - 13 = 293
				('2012-03-11') : 400L, // 400 + 13 - 4 = 409
				('2012-03-12') : 500L, // 500 + 3 - 9 = 494
		]

		def hour = [
				('2012-03-07') : [('19'):1, ('20'):0, ('21'):0, ('22'):0, ('23'):3],
				('2012-03-08') : [('19'):2, ('20'):0, ('21'):0, ('22'):0, ('23'):4],
				('2012-03-09') : [('19'):1, ('20'):0, ('21'):0, ('22'):0, ('23'):5],
				('2012-03-10') : [('19'):4, ('20'):0, ('21'):0, ('22'):0, ('23'):9],
				('2012-03-11') : [('19'):1, ('20'):0, ('21'):0, ('22'):0, ('23'):3],
				('2012-03-12') : [('19'):1, ('20'):0, ('21'):0, ('22'):0, ('23'):9]
		]

		def start = tf.parse("2012-03-08 00:00:00")
		def finish = tf.parse("2012-03-12 23:59:59")
		def adjuster = new TimeZoneAdjuster(start, finish, dataTimeZone, clientTimeZone, Calendar.DAY_OF_MONTH );

		def counts = adjuster.mergeCounts(primary, hour)
		counts.each {k,v ->
			println "$k => $v"
		}
		assertEquals 98, counts['2012-03-08']
		assertEquals 200, counts['2012-03-09']
		assertEquals 293, counts['2012-03-10']
		assertEquals 409, counts['2012-03-11']
		assertEquals 494, counts['2012-03-12']
	}

	void testMergeMonthMap()
	{
		def primary = [
				('2012-02') : [left: 100L, right: 50L], // 100 + 4 - 6 = 98
				('2012-03') : [left: 200L, center: 100L], // 200 + 9 - 6 = 203
				('2012-04') : [left: 300L, right: 150L], // 300 + 11 - 13 = 298
				('2012-05') : [left: 400L, right: 200L], // 400 + 3 - 6 = 397
				('2012-06') : [left: 500L, right: 250L], // 500 + 9 - 10 = 499
		]

		def hour = [
				('2012-01-31') : [('19'):[left: 1], ('23'):[left: 2, right: 3]],
				('2012-02-01') : [('19'):[left: 1], ('23'):[left: 2, right: 3]],

				('2012-02-29') : [('19'):[left: 1], ('23'):[left: 2, right: 3]],
				('2012-03-01') : [('19'):[left: 1], ('23'):[left: 2, right: 3]],

				('2012-03-31') : [('19'):[left: 1], ('23'):[left: 2, right: 3]],
				('2012-04-01') : [('19'):[left: 1], ('23'):[left: 2, right: 3]],

				('2012-04-30') : [('19'):[left: 1], ('23'):[left: 2, center: 3]],
				('2012-05-01') : [('19'):[left: 1], ('23'):[left: 2, right: 3]],

				('2012-05-31') : [('19'):[left: 1], ('23'):[left: 2, right: 3]],
				('2012-06-01') : [('19'):[left: 1], ('23'):[left: 2, right: 3]],
		]

		def start = tf.parse("2012-02-01 00:00:00")
		def finish = tf.parse("2012-06-30 23:59:59")
		def adjuster = new TimeZoneAdjuster(start, finish, dataTimeZone, clientTimeZone, Calendar.MONTH );

		def counts = adjuster.mergeCounts(primary, hour)
		counts.each {k,v ->
			println "$k => $v"
		}
		assertEquals 5, counts.size()
	}
}
