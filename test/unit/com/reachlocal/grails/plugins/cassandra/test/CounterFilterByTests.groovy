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

import grails.test.GrailsUnitTestCase
import com.reachlocal.grails.plugins.cassandra.mapping.CounterUtils

/**
 * @author: Bob Florian
 */
class CounterFilterByTests  extends GrailsUnitTestCase
{
	void testFilterBy1()
	{
		def data = [
				('2012-02-04T14'): [OtherPaid: 1],
				('2012-02-04T15'): [OtherPaid: 1],
				('2012-02-04T16'): [ReachLocal: 1,OtherPaid: 1],
				('2012-02-04T17'): [ReachLocal: 3,OtherPaid: 1],
				('2012-02-04T18'): [ReachLocal: 2],
				('2012-02-04T19'): [ReachLocal: 2,OtherPaid: 2,Organic: 1],
				('2012-02-04T20'): [ReachLocal: 1],
				('2012-02-04T21'): [ReachLocal: 2,Organic: 2],
				('2012-02-04T22'): [OtherPaid: 1],
				('2012-02-04T23'): [ReachLocal: 1,OtherPaid: 1],
				('2012-02-05T00'): [ReachLocal: 2,OtherPaid: 2],
				('2012-02-05T01'): [ReachLocal: 1,OtherPaid: 1]
		]

		def result = CounterUtils.filterBy(data, ['occurTime','refClass'], [refClass: 'Organic'])
		println result
	}

	void testFilterBy2()
	{
		def data = [
				('2012-02-04T14'): [OtherPaid: 1],
				('2012-02-04T15'): [OtherPaid: 1],
				('2012-02-04T16'): [ReachLocal: 1,OtherPaid: 1],
				('2012-02-04T17'): [ReachLocal: 3,OtherPaid: 1],
				('2012-02-04T18'): [ReachLocal: 2],
				('2012-02-04T19'): [ReachLocal: 2,OtherPaid: 2,Organic: 1],
				('2012-02-04T20'): [ReachLocal: 1],
				('2012-02-04T21'): [ReachLocal: 2,Organic: 2],
				('2012-02-04T22'): [OtherPaid: 1],
				('2012-02-04T23'): [ReachLocal: 1,OtherPaid: 1],
				('2012-02-05T00'): [ReachLocal: 2,OtherPaid: 2],
				('2012-02-05T01'): [ReachLocal: 1,OtherPaid: 1]
		]

		def result = CounterUtils.filterBy(data, ['occurTime','refClass'], [refClass: ['Organic','ReachLocal']])
		println result
	}

	void testFilterBy3()
	{
		def data = [
				('2012-02-04T14'): [OtherPaid: [Google:10,Bing:4]],
				('2012-02-04T15'): [OtherPaid: [Google:12,Bing:2,Yahoo:3]],
				('2012-02-04T16'): [ReachLocal: [Google:6,Bing:2], OtherPaid: [Google:7,Bing:1]],
				('2012-02-04T17'): [ReachLocal: [Bing:4, Yahoo: 3], OtherPaid: [Google:10,Bing:2]],
				('2012-02-04T18'): [ReachLocal: [Google: 5]],
				('2012-02-04T19'): [ReachLocal: [Google:12,Bing:2,Yahoo:3],OtherPaid: [Google:8,Yahoo:3],Organic: [Google:4,Bing:1]]
		]

		def result = CounterUtils.filterBy(data, ['occurTime','refClass','refName'], [refName: ['Yahoo','Bing']])
		println result
	}
}
