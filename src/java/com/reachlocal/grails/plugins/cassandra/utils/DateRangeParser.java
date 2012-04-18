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
import java.util.*;

/**
 * @author: Bob Florian
 */
public class DateRangeParser
{
	private Calendar start;
	private Calendar finish;
	private Calendar startMonth;
	private Calendar finishMonth;
	private Calendar startDay;
	private Calendar finishDay;
	private TimeZone timeZone;
	private boolean considerDayStartTime;
	private boolean considerDayFinishTime;

	public DateRangeParser(Date start, Date finish, TimeZone timeZone)
	{
		this.start = Calendar.getInstance(timeZone);
		this.start.setTime(start);
		this.finish = Calendar.getInstance(timeZone);
		this.finish.setTime(finish);
		this.timeZone = timeZone;
		initialize();
	}

	public DateRangeParser(Calendar start, Calendar finish)
	{
		this.start = start;
		this.finish = finish;
		this.timeZone = start.getTimeZone();
		initialize();
	}

	private void initialize()
	{
		this.startMonth = DateHelper.setBeginningOfWholeMonth(start);
		this.finishMonth = DateHelper.setEndOfWholeMonth(finish);
		this.startDay = DateHelper.setBeginningOfWholeDay(start);
		this.finishDay = DateHelper.setEndOfWholeDay(finish);
		/*
		System.out.println("start:       " + this.start.getTime());
		System.out.println("finish:      " + this.finish.getTime());
		System.out.println("startDay:    " + this.startDay.getTime());
		System.out.println("finishDay:   " + this.finishDay.getTime());
		System.out.println("startMonth:  " + this.startMonth.getTime());
		System.out.println("finishMonth: " + this.finishMonth.getTime());
        */
		this.considerDayStartTime = startDay.get(Calendar.MONTH) < startMonth.get(Calendar.MONTH) && !startDay.getTime().after(finishDay.getTime());
		this.considerDayFinishTime = finishDay.get(Calendar.MONTH) > finishMonth.get(Calendar.MONTH) && !startDay.getTime().after(finishDay.getTime());
	}

	public boolean getHasWholeMonth()
	{
		return !startMonth.getTime().after(finishMonth.getTime());
	}

	public boolean getHasWholeDayAfterMonth()
	{
		return !startMonth.getTime().after(finishMonth.getTime());
	}

	public boolean getHasWholeDayWithoutMonth()
	{
		return !startMonth.getTime().after(finishMonth.getTime());
	}

	public DateRange getMonthRange()
	{
		DateRange result = null;
		if (getHasWholeMonth()) {
			result = new DateRange(startMonth.getTime(), finishMonth.getTime(), Calendar.MONTH);
		}
		return result;
	}

	public DateRange getDayRange1()
	{
		DateRange result = null;
		if (getHasWholeMonth()) {
			if (startDay.get(Calendar.MONTH) < startMonth.get(Calendar.MONTH)) {
				result = new DateRange(startDay.getTime(), new Date(startMonth.getTime().getTime()-1), Calendar.DAY_OF_MONTH);
			}
		}
		else {
			// no whole months, check if whole days
			if (!startDay.getTime().after(finishDay.getTime())) {
				result = new DateRange(startDay.getTime(), finishDay.getTime(), Calendar.DAY_OF_MONTH);
			}
		}
		return result;
	}

	public DateRange getDayRange2()
	{
		DateRange result = null;
		if (getHasWholeMonth()) {
			if (considerDayFinishTime) {
				result = new DateRange(new Date(finishMonth.getTime().getTime()+1), finishDay.getTime(), Calendar.DAY_OF_MONTH);
			}
		}
		return result;
	}

	public DateRange getHourRange1()
	{
		DateRange result = null;
		if (getHasWholeMonth()) {
			if (considerDayStartTime) {
				Date ts = new Date(startDay.getTime().getTime()-1);
				if (!ts.before(start.getTime())) {
					result = new DateRange(start.getTime(), ts, Calendar.HOUR_OF_DAY);
				}
			}
			else {
				Date ts = new Date(startMonth.getTime().getTime()-1);
				if (!ts.before(start.getTime())) {
					result = new DateRange(start.getTime(), ts, Calendar.HOUR_OF_DAY);
				}
			}
		}
		else {
			if (considerDayStartTime) {
				Date ts1 = start.getTime();
				Date ts2 = new Date(startDay.getTime().getTime()-1);
				if (!ts2.before(ts1)) {
					result = new DateRange(ts1, ts2, Calendar.HOUR_OF_DAY);
				}
			}
			else if(startDay.getTime().after(finishDay.getTime()))  {
				result = new DateRange(start.getTime(), finish.getTime(), Calendar.HOUR_OF_DAY);
			}
		}
		return result;
	}

	public DateRange getHourRange2()
	{
		DateRange result = null;
		if (getHasWholeMonth()) {
			if (considerDayFinishTime) {
				Date ts = new Date(finishDay.getTime().getTime()+1);
				if (!ts.after(finish.getTime())) {
					result = new DateRange(ts, finish.getTime(), Calendar.HOUR_OF_DAY);
				}
			}
			else {
				Date ts = new Date(finishMonth.getTime().getTime()+1);
				if (!ts.after(finish.getTime())) {
					result = new DateRange(ts, finish.getTime(), Calendar.HOUR_OF_DAY);
				}
			}
		}
		else {
			if (considerDayFinishTime) {
				Date ts = new Date(finishDay.getTime().getTime()+1);
				if (!ts.after(finish.getTime())) {
					result = new DateRange(ts, finish.getTime(), Calendar.HOUR_OF_DAY);
				}
			}
		}
		return result;
	}

	public List<DateRange> getDateRanges()
	{
		List<DateRange> list = new ArrayList<DateRange>(5);
		DateRange range = getHourRange1();
		if (range != null && range.start != range.finish) {
			list.add(range);
		}

		range = getDayRange1();
		if (range != null) {
			list.add(range);
		}

		range = getMonthRange();
		if (range != null) {
			list.add(range);
		}

		range = getDayRange2();
		if (range != null) {
			list.add(range);
		}

		range = getHourRange2();
		if (range != null) {
			list.add(range);
		}

		return list;
	}

	public String toString()
	{
		return
				"DateRangeParser(start: " + TF.format(start.getTime()) +
				", finish: " + TF.format(finish.getTime()) +
				", startDay: " + TF.format(startDay.getTime()) +
				", finishDay: " + TF.format(finishDay.getTime()) +
				", startMonth: " + TF.format(startMonth.getTime()) +
				", finishMonth: " + TF.format(finishMonth.getTime()) + ")";
	}

	static private DateFormat TF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
}
