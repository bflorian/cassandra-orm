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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author: Bob Florian
 */
public class DateRange
{
	public Date start;

	public Date finish;

	public int grain;

	public DateRange(Date start, Date finish, int grain)
	{
		this.start = start;
		this.finish = finish;
		this.grain = grain;
	}

	public String toString()
	{
		DateFormat tf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return "[start: " + tf.format(this.start) + ", finish: " + tf.format(this.finish) + ", grain: " + label(this.grain) + "]";
	}

	static private String label(int grain)
	{
		switch(grain) {
			case 11:
				return "hour";
			case 5:
				return "day";
			case 2:
				return "month";
			default:
				return String.valueOf(grain);
		}
	}
}
