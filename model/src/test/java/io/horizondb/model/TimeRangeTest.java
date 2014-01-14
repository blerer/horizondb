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

import io.horizondb.model.TimeRange;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;

public class TimeRangeTest {

	@Test
	public void testIncludes() {
		
		TimeRange range = new TimeRange(200, 400);
		
		assertTrue(range.includes(300));
		assertTrue(range.includes(200));
		assertTrue(range.includes(400));
		assertFalse(range.includes(100));
		assertFalse(range.includes(500));
	}
	
	@Test
	public void testIsBefore() {
		
		TimeRange range = new TimeRange(200, 400);
		
		assertFalse(range.isBefore(300));
		assertFalse(range.isBefore(200));
		assertFalse(range.isBefore(400));
		assertFalse(range.isBefore(100));
		assertTrue(range.isBefore(500));
	}
	
	@Test
	public void testIsAfter() {
		
		TimeRange range = new TimeRange(200, 400);
		
		assertFalse(range.isAfter(300));
		assertFalse(range.isAfter(200));
		assertFalse(range.isAfter(400));
		assertTrue(range.isAfter(100));
		assertFalse(range.isAfter(500));
	}
	
	@Test
	public void testIncludesRange() {
		
		TimeRange range = new TimeRange(200, 400);
		
		assertTrue(range.includes(new TimeRange(300, 350)));
		assertTrue(range.includes(new TimeRange(200, 350)));
		assertTrue(range.includes(new TimeRange(200, 400)));
		assertFalse(range.includes(new TimeRange(100, 150)));
		assertFalse(range.includes(new TimeRange(600, 750)));
		assertFalse(range.includes(new TimeRange(100, 300)));
		assertFalse(range.includes(new TimeRange(300, 500)));
	}
	
	@Test
	public void testSplit() {
		
		TimeRange range = new TimeRange(200, 400);
		
		assertArrayEquals(range.split(300), new TimeRange[]{new TimeRange(200, 299), new TimeRange(300, 400)});
		assertArrayEquals(range.split(200), new TimeRange[]{new TimeRange(200, 400)});
		assertArrayEquals(range.split(400), new TimeRange[]{new TimeRange(200, 399), new TimeRange(400, 400)});
	}

}
