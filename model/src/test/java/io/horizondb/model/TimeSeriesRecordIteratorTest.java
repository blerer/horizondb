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

import io.horizondb.model.records.TimeSeriesRecord;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimeSeriesRecordIteratorTest {

	@Test
	public void testWithOnlyOneRecord() {

		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();

		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .addRecordType(trade)
		                                                      .build();

		TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(definition)
		                                                   .newRecord("Trade")
		                                                   .setTimestampInMillis(0, 1000000)
		                                                   .setDecimal(1, 125, 1)
		                                                   .setLong(2, 10)
		                                                   .build();

		TimeSeriesRecord expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setTimestampInMillis(0, 1000000);
		expected.setDecimal(1, 125, 1);
		expected.setLong(2, 10);

		assertTrue(iterator.hasNext());
		assertEquals(expected, iterator.next());
		assertFalse(iterator.hasNext());
	}
	
	@Test
	public void testWithOnlyTwoRecordOfDifferentType() throws ParseException {
		
		long time = getReferenceTime();
		
		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();
		
		RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
		                                                 .addDecimalField("bidPrice")
		                                                 .addDecimalField("askPrice")
		                                                 .addLongField("bidVolume")
		                                                 .addLongField("askVolume")
		                                                 .build();

		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .addRecordType(quote)
		                                                      .addRecordType(trade)
		                                                      .build();

		TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(definition)
											               .newRecord("Quote")
											               .setTimestampInMillis(0, time)
											               .setDecimal(1, 123, 1)
											               .setDecimal(2, 125, 1)
											               .setLong(3, 6)
											               .setLong(4, 14)
		                                                   .newRecord("Trade")
		                                                   .setTimestampInMillis(0, time + 100)
		                                                   .setDecimal(1, 125, 1)
		                                                   .setLong(2, 10)
		                                                   .build();

		TimeSeriesRecord expectedQuote = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.DECIMAL, FieldType.LONG, FieldType.LONG);
		expectedQuote.setTimestampInMillis(0, time);
		expectedQuote.setDecimal(1, 123, 1);
		expectedQuote.setDecimal(2, 125, 1);
		expectedQuote.setLong(3, 6);
		expectedQuote.setLong(4, 14);
		
		assertTrue(iterator.hasNext());
		assertEquals(expectedQuote, iterator.next());
		
		TimeSeriesRecord expectedTrade = new TimeSeriesRecord(1, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expectedTrade.setTimestampInMillis(0, time + 100);
		expectedTrade.setDecimal(1, 125, 1);
		expectedTrade.setLong(2, 10);

		assertTrue(iterator.hasNext());
		assertEquals(expectedTrade, iterator.next());
		assertFalse(iterator.hasNext());
	}

	@Test
	public void testWithTwoRecordsOfTheSameType() throws ParseException {

		long time = getReferenceTime();
		
		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();

		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .addRecordType(trade)
		                                                      .build();

		TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(definition)
		                                                   .newRecord("Trade")
		                                                   .setTimestampInMillis(0, time)
		                                                   .setDecimal(1, 125, 1)
		                                                   .setLong(2, 10)
		                                                   .newRecord("Trade")
		                                                   .setTimestampInMillis(0, time + 50)
		                                                   .setDecimal(1, 124, 1)
		                                                   .setLong(2, 5)
		                                                   .build();

		TimeSeriesRecord expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setTimestampInMillis(0, time);
		expected.setDecimal(1, 125, 1);
		expected.setLong(2, 10);

		assertTrue(iterator.hasNext());
		assertEquals(expected, iterator.next());
		
		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setDelta(true);
		expected.setTimestampInMillis(0, 50);
		expected.setDecimal(1, -1, 1);
		expected.setLong(2, -5);
		
		assertTrue(iterator.hasNext());
		assertEquals(expected, iterator.next());
		assertFalse(iterator.hasNext());
	}
	
	@Test
	public void testWithThreeRecordsOfSameType() throws ParseException {

		long time = getReferenceTime();
		
		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();

		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .addRecordType(trade)
		                                                      .build();

		TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(definition)
		                                                   .newRecord("Trade")
		                                                   .setTimestampInMillis(0, time)
		                                                   .setDecimal(1, 125, -1)
		                                                   .setLong(2, 10)
		                                                   .newRecord("Trade")
		                                                   .setTimestampInMillis(0, time + 50)
		                                                   .setDecimal(1, 124, -1)
		                                                   .setLong(2, 5)
		                                                   .newRecord("Trade")
		                                                   .setTimestampInMillis(0, time + 120)
		                                                   .setDecimal(1, 13, 0)
		                                                   .setLong(2, 6)
		                                                   .build();

		TimeSeriesRecord expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setTimestampInMillis(0, time);
		expected.setDecimal(1, 125, -1);
		expected.setLong(2, 10);

		assertTrue(iterator.hasNext());
		assertEquals(expected, iterator.next());
		
		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setDelta(true);
		expected.setTimestampInMillis(0, 50);
		expected.setDecimal(1, -1, -1);
		expected.setLong(2, -5);
		
		assertTrue(iterator.hasNext());
		assertEquals(expected, iterator.next());
		
		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setDelta(true);
		expected.setTimestampInMillis(0, 70);
		expected.setDecimal(1, 6, -1);
		expected.setLong(2, 1);
		
		assertTrue(iterator.hasNext());
		assertEquals(expected, iterator.next());
		
		assertFalse(iterator.hasNext());
	}

	
	@Test
	public void testWithThreeRecordsOfSameTypeInDisorder() throws ParseException {

		long time = getReferenceTime();
		
		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();

		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .addRecordType(trade)
		                                                      .build();

		TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(definition)
		                                                   .newRecord("Trade")
		                                                   .setTimestampInMillis(0, time + 50)
		                                                   .setDecimal(1, 124, -1)
		                                                   .setLong(2, 5)
		                                                   .newRecord("Trade")
		                                                   .setTimestampInMillis(0, time + 120)
		                                                   .setDecimal(1, 13, 0)
		                                                   .setLong(2, 6)
		                                                   .newRecord("Trade")
		                                                   .setTimestampInMillis(0, time)
		                                                   .setDecimal(1, 125, -1)
		                                                   .setLong(2, 10)
		                                                   .build();

		TimeSeriesRecord expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setTimestampInMillis(0, time);
		expected.setDecimal(1, 125, -1);
		expected.setLong(2, 10);

		assertTrue(iterator.hasNext());
		assertEquals(expected, iterator.next());
		
		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setDelta(true);
		expected.setTimestampInMillis(0, 50);
		expected.setDecimal(1, -1, -1);
		expected.setLong(2, -5);
		
		assertTrue(iterator.hasNext());
		assertEquals(expected, iterator.next());
		
		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setDelta(true);
		expected.setTimestampInMillis(0, 70);
		expected.setDecimal(1, 6, -1);
		expected.setLong(2, 1);
		
		assertTrue(iterator.hasNext());
		assertEquals(expected, iterator.next());
		
		assertFalse(iterator.hasNext());
	}
	
	@Test
	public void testWithMultipleRecordsOfDifferentType() throws ParseException {

		long time = getReferenceTime();
		
		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();
		
		RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
		                                                 .addDecimalField("bidPrice")
		                                                 .addDecimalField("askPrice")
		                                                 .addLongField("bidVolume")
		                                                 .addLongField("askVolume")
		                                                 .build();
		
		RecordTypeDefinition exchangeState = RecordTypeDefinition.newBuilder("ExchangeState")
												                 .addByteField("status")
												                 .build();
		
		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .addRecordType(trade)
		                                                      .addRecordType(quote)
		                                                      .addRecordType(exchangeState)
		                                                      .build();

		TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(definition)
														   .newRecord("ExchangeState")
											               .setTimestampInMillis(0, time)
											               .setByte(1, 1)
		                                                   .newRecord("Quote")
		                                                   .setTimestampInMillis(0, time)
		                                                   .setDecimal(1, 123, -1)
		                                                   .setDecimal(2, 125, -1)
		                                                   .setLong(3, 6)
		                                                   .setLong(4, 14)
		                                                   .newRecord("Trade")
		                                                   .setTimestampInMillis(0, time + 5)
		                                                   .setDecimal(1, 125, -1)
		                                                   .setLong(2, 10)
		                                                   .newRecord("Quote")
											               .setTimestampInMillis(0, time + 15)
											               .setDecimal(1, 123, -1)
											               .setDecimal(2, 125, -1)
											               .setLong(3, 6)
											               .setLong(4, 4)
		                                                   .newRecord("Trade")
		                                                   .setTimestampInMillis(0, time + 50)
		                                                   .setDecimal(1, 124, -1)
		                                                   .setLong(2, 5)
		                                                   .newRecord("Trade")
		                                                   .setTimestampInMillis(0, time + 120)
		                                                   .setDecimal(1, 13, 0)
		                                                   .setLong(2, 6)
		                                                   .newRecord("Quote")
											               .setTimestampInMillis(0, time + 150)
											               .setDecimal(1, 123, -1)
											               .setDecimal(2, 125, -1)
											               .setLong(3, 6)
											               .setLong(4, 4)
		                                                   .build();

		TimeSeriesRecord expectedES = new TimeSeriesRecord(2, TimeUnit.MILLISECONDS, FieldType.BYTE);
		expectedES.setTimestampInMillis(0, time);
		expectedES.setByte(1, 1);
		
		assertTrue(iterator.hasNext());
		assertEquals(expectedES, iterator.next());
		
		TimeSeriesRecord expectedQuote = new TimeSeriesRecord(1, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.DECIMAL, FieldType.LONG, FieldType.LONG);
		expectedQuote.setTimestampInMillis(0, time);
		expectedQuote.setDecimal(1, 123, -1);
		expectedQuote.setDecimal(2, 125, -1);
		expectedQuote.setLong(3, 6);
		expectedQuote.setLong(4, 14);
		
		assertTrue(iterator.hasNext());
		assertEquals(expectedQuote, iterator.next());
		
		TimeSeriesRecord expectedTrade = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expectedTrade.setTimestampInMillis(0, time + 5);
		expectedTrade.setDecimal(1, 125, -1);
		expectedTrade.setLong(2, 10);

		assertTrue(iterator.hasNext());
		assertEquals(expectedTrade, iterator.next());
		
		expectedQuote = new TimeSeriesRecord(1, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.DECIMAL, FieldType.LONG, FieldType.LONG);
		expectedQuote.setDelta(true);
		expectedQuote.setTimestampInMillis(0, 15);
		expectedQuote.setDecimal(1, 0, -1);
		expectedQuote.setDecimal(2, 0, -1);
		expectedQuote.setLong(3, 0);
		expectedQuote.setLong(4, -10);
		
		assertTrue(iterator.hasNext());
		assertEquals(expectedQuote, iterator.next());
		
		expectedTrade = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expectedTrade.setDelta(true);
		expectedTrade.setTimestampInMillis(0, 45);
		expectedTrade.setDecimal(1, -1, -1);
		expectedTrade.setLong(2, -5);
		
		assertTrue(iterator.hasNext());
		assertEquals(expectedTrade, iterator.next());
		
		expectedTrade = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expectedTrade.setDelta(true);
		expectedTrade.setTimestampInMillis(0, 70);
		expectedTrade.setDecimal(1, 6, -1);
		expectedTrade.setLong(2, 1);
		
		assertTrue(iterator.hasNext());
		assertEquals(expectedTrade, iterator.next());
		
		expectedQuote = new TimeSeriesRecord(1, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.DECIMAL, FieldType.LONG, FieldType.LONG);
		expectedQuote.setDelta(true);
		expectedQuote.setTimestampInMillis(0, 135);
		expectedQuote.setDecimal(1, 0, -1);
		expectedQuote.setDecimal(2, 0, -1);
		expectedQuote.setLong(3, 0);
		expectedQuote.setLong(4, 0);
		
		assertTrue(iterator.hasNext());
		assertEquals(expectedQuote, iterator.next());
		
		assertFalse(iterator.hasNext());
	}
	
	/**
	 * Returns the reference time used during the test.
	 * 
	 * @return the reference time used during the test.
	 * @throws ParseException if a problem occurs while generating the time.
	 */
    private static long getReferenceTime() throws ParseException {
	    
    	SimpleDateFormat format = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
		return format.parse("2013.11.14 11:46:00").getTime();
    }
}
