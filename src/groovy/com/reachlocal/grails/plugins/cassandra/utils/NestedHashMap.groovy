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

package com.reachlocal.grails.plugins.cassandra.utils

/**
 * @author: Bob Florian
 */
class NestedHashMap extends LinkedHashMap
{
	Object put(List args)
	{
		if (args.size() < 2) {
			throw new IllegalArgumentException("There aren't enough items. Must specify at least one key and a value.")
		}
		else if (args.size() == 2) {
			super.put(args[0], args[1])
		}
		else {
			def map = this
			args[0..-3].each {
				if (!map.containsKey(it)) {
					map[it] = new NestedHashMap()
				}
				map = map[it]
			}
			map[args[-2]] = args[-1]
		}
	}

	Object put(Object... args)
	{
		put(args as List)
	}

	void increment(List args)
	{
		if (args.size() < 2) {
			throw new IllegalArgumentException("There aren't enough items. Must specify at least one key and a value.")
		}
		else if (args.size() == 2) {
			increment(args[0], args[1])
		}
		else {
			def map = this
			def slice = args[0..-3]
			slice.each {
				if (!map.containsKey(it)) {
					map[it] = new NestedHashMap()
				}
				map = map[it]
			}
			map.increment(args[-2], args[-1])
		}
	}

	void increment(name, value=1)
	{
		def entry = get(name)
		if (entry) {
			put(name, entry + value)
		}
		else {
			put(name, value)
		}
	}

	def total()
	{
		mapTotal(this)
	}

	static mapTotal(Map map)
	{
		def total = 0L
		map.each {key, value ->
			total += mapTotal(value)
		}
		return total
	}

	static mapTotal(number) {
		return number
	}

	def groupBy(int level)
	{
		groupBy([level])
	}

	def groupBy(List levels)
	{
		def result = new NestedHashMap()
		processGroupByItem(this, [], levels, result)
		return result
	}

	static void processGroupByItem(Map item, List keys, List groupLevels, NestedHashMap result)
	{
		item.each {key, value ->
			processGroupByItem(value, keys + [key], groupLevels, result)
		}
	}

	static void processGroupByItem(Number item, List keys, List groupLevels, NestedHashMap result)
	{
		def resultKeys = groupLevels.collect{
			keys[it]
		}
		result.increment(resultKeys + [item])
	}
}

