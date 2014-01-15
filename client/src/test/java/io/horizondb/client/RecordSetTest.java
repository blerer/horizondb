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
package io.horizondb.client;

import io.horizondb.model.DatabaseDefinition;
import io.horizondb.model.FieldType;
import io.horizondb.model.Record;
import io.horizondb.model.RecordIterator;
import io.horizondb.model.RecordTypeDefinition;
import io.horizondb.model.TimeSeriesDefinition;
import io.horizondb.model.records.TimeSeriesRecord;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 *
 */
public class RecordSetTest {

	private TimeSeriesDefinition definition;
	
	@Before
	public void setUp() {
		
		RecordTypeDefinition recordType = RecordTypeDefinition.newBuilder("ExchangeState")
		                                                      .addMillisecondTimestampField("timestamp")
		                                                      .addByteField("status")
		                                                      .build();
		
		DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");
		
		this.definition = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
		                                    .timeUnit(TimeUnit.NANOSECONDS)
		                                    .addRecordType(recordType)
		                                    .build();
	}
	
	@After
	public void tearDown() {
		
		this.definition = null;
	}
	
	@Test
	public void testWithFullRecords() throws IOException {
		
		TimeSeriesRecord first = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		first.setTimestampInNanos(0, 12000700);
		first.setTimestampInMillis(1, 12);
		first.setByte(2, 3);

		TimeSeriesRecord second = new TimeSeriesRecord(0,
		                                               TimeUnit.NANOSECONDS,
		                                               FieldType.MILLISECONDS_TIMESTAMP,
		                                               FieldType.BYTE);
		second.setTimestampInNanos(0, 13000900);
		second.setTimestampInMillis(1, 13);
		second.setByte(2, 3);

		TimeSeriesRecord third = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		third.setTimestampInNanos(0, 13004400);
		third.setTimestampInMillis(1, 13);
		third.setByte(2, 1);

		RecordIterator iterator = new RecordIteratorStub(asList(first, second, third));
		
		try (RecordSet recordSet = new RecordSet(this.definition, iterator)) {

			assertTrue(recordSet.next());

			assertEquals(first.getTimestampInNanos(0), recordSet.getTimestampInNanos(0));
			assertEquals(first.getTimestampInMillis(1), recordSet.getTimestampInMillis(1));
			assertEquals(first.getByte(2), recordSet.getByte(2));

			assertTrue(recordSet.next());

			assertEquals(second.getTimestampInNanos(0), recordSet.getTimestampInNanos(0));
			assertEquals(second.getTimestampInMillis(1), recordSet.getTimestampInMillis(1));
			assertEquals(second.getByte(2), recordSet.getByte(2));

			assertTrue(recordSet.next());

			assertEquals(third.getTimestampInNanos(0), recordSet.getTimestampInNanos(0));
			assertEquals(third.getTimestampInMillis(1), recordSet.getTimestampInMillis(1));
			assertEquals(third.getByte(2), recordSet.getByte(2));

			assertFalse(recordSet.next());
		}
	}

	@Test
	public void testWithDeltas() throws IOException {
		
		TimeSeriesRecord first = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		first.setTimestampInNanos(0, 12000700);
		first.setTimestampInMillis(1, 12);
		first.setByte(2, 3);

		TimeSeriesRecord second = new TimeSeriesRecord(0,
		                                               TimeUnit.NANOSECONDS,
		                                               FieldType.MILLISECONDS_TIMESTAMP,
		                                               FieldType.BYTE);
		second.setDelta(true);
		second.setTimestampInNanos(0, 1000200);
		second.setTimestampInMillis(1, 1);

		TimeSeriesRecord third = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		third.setDelta(true);
		third.setTimestampInNanos(0, 3500);
		third.setByte(2, -2);

		RecordIterator iterator = new RecordIteratorStub(asList(first, second, third));
		
		try (RecordSet recordSet = new RecordSet(this.definition, iterator)) {

			assertTrue(recordSet.next());

			assertEquals(first.getTimestampInNanos(0), recordSet.getTimestampInNanos(0));
			assertEquals(first.getTimestampInMillis(1), recordSet.getTimestampInMillis(1));
			assertEquals(first.getByte(2), recordSet.getByte(2));

			assertTrue(recordSet.next());
			
			assertEquals(13000900, recordSet.getTimestampInNanos(0));
			assertEquals(13, recordSet.getTimestampInMillis(1));
			assertEquals(3, recordSet.getByte(2));

			assertTrue(recordSet.next());
			
			assertEquals(13004400, recordSet.getTimestampInNanos(0));
			assertEquals(13, recordSet.getTimestampInMillis(1));
			assertEquals(1, recordSet.getByte(2));

			assertFalse(recordSet.next());
		}
	}
	
	@Test
	public void testWithDeltasAndFullState() throws IOException {
		
		TimeSeriesRecord first = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		first.setTimestampInNanos(0, 12000700);
		first.setTimestampInMillis(1, 12);
		first.setByte(2, 3);

		TimeSeriesRecord second = new TimeSeriesRecord(0,
		                                               TimeUnit.NANOSECONDS,
		                                               FieldType.MILLISECONDS_TIMESTAMP,
		                                               FieldType.BYTE);
		second.setDelta(true);
		second.setTimestampInNanos(0, 1000200);
		second.setTimestampInMillis(1, 1);

		TimeSeriesRecord third = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		third.setTimestampInNanos(0, 13004400);
		third.setTimestampInMillis(1, 13);
		third.setByte(2, 1);

		RecordIterator iterator = new RecordIteratorStub(asList(first, second, third));
		
		try (RecordSet recordSet = new RecordSet(this.definition, iterator)) {

			assertTrue(recordSet.next());

			assertEquals(first.getTimestampInNanos(0), recordSet.getTimestampInNanos(0));
			assertEquals(first.getTimestampInMillis(1), recordSet.getTimestampInMillis(1));
			assertEquals(first.getByte(2), recordSet.getByte(2));

			assertTrue(recordSet.next());
			
			assertEquals(13000900, recordSet.getTimestampInNanos(0));
			assertEquals(13, recordSet.getTimestampInMillis(1));
			assertEquals(3, recordSet.getByte(2));

			assertTrue(recordSet.next());
			
			assertEquals(13004400, recordSet.getTimestampInNanos(0));
			assertEquals(13, recordSet.getTimestampInMillis(1));
			assertEquals(1, recordSet.getByte(2));

			assertFalse(recordSet.next());
		}
	}
	
	private static class RecordIteratorStub implements RecordIterator {

		private final Iterator<? extends Record> iterator;
		
		public RecordIteratorStub(Iterable<? extends Record> iterable) {
			this.iterator = iterable.iterator();
		}
		
		/**
		 * {@inheritDoc}
		 */
        @Override
        public void close() throws IOException {

        }

		/**
		 * {@inheritDoc}
		 */
        @Override
        public boolean hasNext() throws IOException {
	        return this.iterator.hasNext();
        }

		/**
		 * {@inheritDoc}
		 */
        @Override
        public Record next() throws IOException {
        	return this.iterator.next();
        }
		
	}
}
