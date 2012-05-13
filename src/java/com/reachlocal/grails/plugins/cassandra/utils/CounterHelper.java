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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: Bob Florian
 */
public class CounterHelper
{
	static public List<Integer> filterMatchIndexes(Map<String, List<String>> columnFilter, List<String> groupBy)
	{
		Set<String> matchKeys = columnFilter.keySet();
		List<Integer> matchIndexes = new ArrayList<Integer>();
		Integer index = 0;
		for (String key: groupBy) {
			if (matchKeys.contains(key)) {
				matchIndexes.add(index);
			}
			index++;
		}
		return matchIndexes;
	}

	static public boolean filterPassed(List<Integer>matchIndexes, List<String>keyValues, List<String>groupBy, Map<String, List<String>>columnFilter)
	{
		boolean passed = true;
		for (Integer index: matchIndexes) {
			String kv = keyValues.get(index);
			String k = groupBy.get(index);
			List<String> fv = columnFilter.get(k);
			if (!fv.contains(kv)) {
				passed = false;
				break;
			}
		}
		return passed;
	}

	static public List<String> filterResultKeyValues(List<String>keyValues, List<Integer>matchIndexes)
	{
		List<String> resultKeyValues = new ArrayList<String>();
		Integer index = 0;
		for (String kv: keyValues) {
			if (!matchIndexes.contains(index)) {
				resultKeyValues.add(kv);
			}
			index++;
		}
		return resultKeyValues;
	}

	static List<String> mergeDateKeys(List<String> rowKeys, List<String> columnKeys)
	{
		if (rowKeys.size() > 0) {
			List<String> result = new ArrayList<String>(rowKeys.size() + columnKeys.size());
			if (columnKeys.size() > 1) {
				result.add(columnKeys.get(0));
				result.addAll(rowKeys);
				result.addAll(columnKeys.subList(1, columnKeys.size()));
			}
			else {
				result.addAll(rowKeys);
				result.addAll(columnKeys);
			}
			return result;
		}
		else {
			return columnKeys;
		}
	}

	static List<String> mergeNonDateKeys(List<String> rowKeys, List<String> columnKeys)
	{
		if (rowKeys.size() > 0) {
			List<String> result = new ArrayList<String>(rowKeys.size() + columnKeys.size());
			result.addAll(rowKeys);
			result.addAll(columnKeys);
			return result;
		}
		else {
			return columnKeys;
		}
	}
}
