/**
 * Copyright 2013 Benjamin Lerer
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.horizondb.model;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Benjamin
 *
 */
public class PartitionTypeTest {

	@Test
	public void testGetPartitionTimeRangeByDay() throws ParseException {
		
		long time = getTime("2013.11.16 09:12:35.670");
		
		Calendar calendar = toCalendar(time);
		
		TimeRange range = PartitionType.BY_DAY.getPartitionTimeRange(calendar);
		
		long start = getTime("2013.11.16 00:00:00.000");
		long end = getTime("2013.11.16 23:59:59.999");
		
		TimeRange expected = new TimeRange(start, end);
		
		assertEquals(expected, range);
	}
	
	@Test
	public void testGetPartitionTimeRangeByDayWithStartTime() throws ParseException {
		
		long time = getTime("2013.11.16 00:00:00.000");
		
		Calendar calendar = toCalendar(time);
		
		TimeRange range = PartitionType.BY_DAY.getPartitionTimeRange(calendar);
		
		long start = getTime("2013.11.16 00:00:00.000");
		long end = getTime("2013.11.16 23:59:59.999");
		
		TimeRange expected = new TimeRange(start, end);
		
		assertEquals(expected, range);
	}
	
	@Test
	public void testGetPartitionTimeRangeByMonthWithStartTime() throws ParseException {
		
		long time = getTime("2013.11.01 00:00:00.000");
		
		Calendar calendar = toCalendar(time);
		
		TimeRange range = PartitionType.BY_MONTH.getPartitionTimeRange(calendar);
		
		long start = getTime("2013.11.01 00:00:00.000");
		long end = getTime("2013.11.30 23:59:59.999");
		
		TimeRange expected = new TimeRange(start, end);
		
		assertEquals(expected, range);
	}
	
	@Test
	public void testGetPartitionTimeRangeByMonth() throws ParseException {
		
		long time = getTime("2013.11.16 09:12:35.670");
		
		Calendar calendar = toCalendar(time);
		
		TimeRange range = PartitionType.BY_MONTH.getPartitionTimeRange(calendar);
		
		long start = getTime("2013.11.01 00:00:00.000");
		long end = getTime("2013.11.30 23:59:59.999");
		
		TimeRange expected = new TimeRange(start, end);
		
		assertEquals(expected, range);
	}
	
	@Test
	public void testGetPartitionTimeRangeByWeekWithStartTime() throws ParseException {
		
		long time = getTime("2013.11.11 00:00:00.000");
		
		Calendar calendar = toCalendar(time);
		
		TimeRange range = PartitionType.BY_WEEK.getPartitionTimeRange(calendar);
		
		long start = getTime("2013.11.11 00:00:00.000");
		long end = getTime("2013.11.17 23:59:59.999");
		
		TimeRange expected = new TimeRange(start, end);
		
		assertEquals(expected, range);
	}
	
	@Test
	public void testGetPartitionTimeRangeByWeek() throws ParseException {
		
		long time = getTime("2013.11.16 09:12:35.670");
		
		Calendar calendar = toCalendar(time);
		
		TimeRange range = PartitionType.BY_WEEK.getPartitionTimeRange(calendar);
		
		long start = getTime("2013.11.11 00:00:00.000");
		long end = getTime("2013.11.17 23:59:59.999");
		
		TimeRange expected = new TimeRange(start, end);
		
		assertEquals(expected, range);
	}
	
	@Test
	public void testGetPartitionTimeRangeWithSunday() throws ParseException {
		
		long time = getTime("2013.11.17 09:12:35.670");
		
		Calendar calendar = toCalendar(time);
		
		TimeRange range = PartitionType.BY_WEEK.getPartitionTimeRange(calendar);
		
		long start = getTime("2013.11.11 00:00:00.000");
		long end = getTime("2013.11.17 23:59:59.999");
		
		TimeRange expected = new TimeRange(start, end);
		
		assertEquals(expected, range);
	}
	
	@Test
	public void testGetPartitionTimeRangeWithMonday() throws ParseException {
		
		long time = getTime("2013.11.11 00:00:00.000");
		
		Calendar calendar = toCalendar(time);
		
		TimeRange range = PartitionType.BY_WEEK.getPartitionTimeRange(calendar);
		
		long start = getTime("2013.11.11 00:00:00.000");
		long end = getTime("2013.11.17 23:59:59.999");
		
		TimeRange expected = new TimeRange(start, end);
		
		assertEquals(expected, range);
	}
	
	/**
	 * Returns the time in milliseconds corresponding  to the specified {@link String}.
	 * 
	 * @param dateAsText the date/time to convert in milliseconds 
	 * @return the time in milliseconds corresponding  to the specified {@link String}.
	 * @throws ParseException if a problem occurs while generating the time.
	 */
    private static long getTime(String dateAsText) throws ParseException {
	    
    	SimpleDateFormat format = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS");
		return format.parse(dateAsText).getTime();
    }
	
    private static Calendar toCalendar(long timeInMilliseconds) {

    	TimeZone timeZone = TimeZone.getTimeZone("Europe/Berlin");
    	Calendar calendar = Calendar.getInstance(timeZone);
    	calendar.setTimeInMillis(timeInMilliseconds);
		return calendar;
    }
}
