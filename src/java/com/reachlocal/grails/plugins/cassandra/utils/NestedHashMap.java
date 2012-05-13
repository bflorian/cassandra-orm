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

package com.reachlocal.grails.plugins.cassandra.utils;

import java.util.*;

/**
 * @author: Bob Florian
 */
public class NestedHashMap extends LinkedHashMap
{
	public void put(List args)
	{
		int argc = args.size();
		if (argc < 2) {
			throw new IllegalArgumentException("There aren't enough items. Must specify at least one key and a value.");
		}
		else if (args.size() == 2) {
			super.put(args.get(0), args.get(1));
		}
		else {
			NestedHashMap map = this;
			for (Object key: args.subList(0, argc-2)) {
				if (!map.containsKey(key)) {
					map.put(key, new NestedHashMap());
				}
				map = (NestedHashMap)map.get(key);
			}
			map.put(args.get(argc-2), args.get(argc-1));
		}
	}

	public void put(Object... args)
	{
		List list = new ArrayList(args.length);
		for (Object item: args) {
			list.add(item);
		}
		put(list);
	}

	public void increment(List args)
	{
		int argc = args.size();
		if (argc < 2) {
			throw new IllegalArgumentException("There aren't enough items. Must specify at least one key and a value.");
		}
		else if (args.size() == 2) {
			increment((String)args.get(0), (Long)args.get(1));
		}
		else {
			NestedHashMap map = this;
			List slice = args.subList(0, argc-2);
			for (Object key: slice) {
				if (!map.containsKey(key)) {
					map.put(key, new NestedHashMap());
				}
				map = (NestedHashMap)map.get(key);
			}
			map.increment((String)args.get(argc-2), (Long)args.get(argc-1));
		}
	}

	public void increment(String name, Long value)
	{
		Object entry = get(name);
		if (entry != null) {
			put(name, ((Long)entry) + value) ;
		}
		else {
			put(name, value);
		}
	}

	public void increment(String name)
	{
		increment(name, 1L);
	}

	public Long total()
	{
		return mapTotal(this);
	}

	public static Long mapTotal(Map map)
	{
		Long total = 0L;
		for(Object value: map.values()) {
			total += mapTotal(value);
		}
		return total;
	}

	static Long mapTotal(Long number) {
		return number;
	}

	static Long mapTotal(Object value) {
		if (value instanceof Map) {
			return mapTotal((Map)value);
		}
		else {
			return mapTotal((Long)value);
		}
	}

	Map groupBy(Integer level)
	{
		List list = new ArrayList();
		list.add(level);
		return groupBy(list);
	}

	Map groupBy(List levels)
	{
		NestedHashMap result = new NestedHashMap();
		List keys = new ArrayList();
		processGroupByItem(this, keys, levels, result);
		return result;
	}

	static void processGroupByItem(Map item, List keys, List groupLevels, NestedHashMap result)
	{
		Iterator iter = item.entrySet().iterator();
		while(iter.hasNext()) {
			Map.Entry entry = (Map.Entry)iter.next();
			List newKeys = new ArrayList(keys);
			newKeys.add(entry.getKey());
			processGroupByItem(entry.getValue(), newKeys, groupLevels, result);
		}
	}

	static void processGroupByItem(Long item, List keys, List groupLevels, NestedHashMap result)
	{
		List resultKeys = new ArrayList();
		for (Object it: groupLevels) {
			resultKeys.add(keys.get((Integer) it));
		}
		resultKeys.add(item);
		result.increment(resultKeys);
	}

	static void processGroupByItem(Object item, List keys, List groupLevels, NestedHashMap result)
	{
		if (item instanceof Map) {
			processGroupByItem((Map)item, keys, groupLevels, result);
		}
		else {
			processGroupByItem((Long)item, keys, groupLevels, result);
		}
	}
}
