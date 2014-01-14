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
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Benjamin
 * 
 */
public class CompositeRecordIteratorTest {

	private static final TimeZone TIMEZONE = TimeZone.getTimeZone("Europe/Berlin");

	@Test
	public void testWithEmptyIterator() {

		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();

		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .timeZone(TIMEZONE)
		                                                      .partitionType(PartitionType.BY_DAY)
		                                                      .addRecordType(trade)
		                                                      .build();

		CompositeRecordIterator recordIterator = CompositeRecordIterator.newBuilder(definition)
		                                                                .build();

		Assert.assertFalse(recordIterator.hasNext());
	}
	
	@Test
	public void testWithOnlyOneRecord() throws ParseException {

		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();

		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .timeZone(TIMEZONE)
		                                                      .partitionType(PartitionType.BY_DAY)
		                                                      .addRecordType(trade)
		                                                      .build();

		long timestamp = getTime("2013.11.14 11:46:00.000");

		CompositeRecordIterator recordIterator = CompositeRecordIterator.newBuilder(definition)
		                                                                .newRecord("Trade")
		                                                                .setTimestampInMillis(0, timestamp)
		                                                                .setDecimal(1, 125, 1)
		                                                                .setLong(2, 10)
		                                                                .build();

		TimeSeriesRecord expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setTimestampInMillis(0, timestamp);
		expected.setDecimal(1, 125, 1);
		expected.setLong(2, 10);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());
		Assert.assertFalse(recordIterator.hasNext());
		Assert.assertFalse(recordIterator.hasNext());
	}
	
	@Test
	public void testWithTwoRecordsOfTheSameType() throws ParseException {

		long time = getTime("2013.11.14 11:46:00.000");

		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();

		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .timeZone(TIMEZONE)
		                                                      .partitionType(PartitionType.BY_DAY)
		                                                      .addRecordType(trade)
		                                                      .build();

		CompositeRecordIterator recordIterator = CompositeRecordIterator.newBuilder(definition)
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

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setDelta(true);
		expected.setTimestampInMillis(0, 50);
		expected.setDecimal(1, -1, 1);
		expected.setLong(2, -5);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());
		Assert.assertFalse(recordIterator.hasNext());
	}

	@Test
	public void testWithTwoRecordsOfTheSameTypeInDifferentPartitions() throws ParseException {

		long time = getTime("2013.11.14 11:46:00.000");
		long time2 = getTime("2013.11.15 17:35:00.000");

		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();

		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .timeZone(TIMEZONE)
		                                                      .partitionType(PartitionType.BY_DAY)
		                                                      .addRecordType(trade)
		                                                      .build();

		CompositeRecordIterator recordIterator = CompositeRecordIterator.newBuilder(definition)
		                                                                .newRecord("Trade")
		                                                                .setTimestampInMillis(0, time)
		                                                                .setDecimal(1, 125, 1)
		                                                                .setLong(2, 10)
		                                                                .newRecord("Trade")
		                                                                .setTimestampInMillis(0, time2)
		                                                                .setDecimal(1, 124, 1)
		                                                                .setLong(2, 5)
		                                                                .build();

		TimeSeriesRecord expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setTimestampInMillis(0, time);
		expected.setDecimal(1, 125, 1);
		expected.setLong(2, 10);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setTimestampInMillis(0, time2);
		expected.setDecimal(1, 124, 1);
		expected.setLong(2, 5);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());
		Assert.assertFalse(recordIterator.hasNext());
	}

	@Test
	public void testWithThreeRecordsOfSameType() throws ParseException {

		long time = getTime("2013.11.14 11:46:00.000");

		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();

		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .timeZone(TIMEZONE)
		                                                      .partitionType(PartitionType.BY_DAY)
		                                                      .addRecordType(trade)
		                                                      .build();

		CompositeRecordIterator recordIterator = CompositeRecordIterator.newBuilder(definition)
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

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setDelta(true);
		expected.setTimestampInMillis(0, 50);
		expected.setDecimal(1, -1, -1);
		expected.setLong(2, -5);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setDelta(true);
		expected.setTimestampInMillis(0, 70);
		expected.setDecimal(1, 6, -1);
		expected.setLong(2, 1);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		Assert.assertFalse(recordIterator.hasNext());
	}

	@Test
	public void testWithThreeRecordsOfSameTypeWithinTwoDifferentPartitions() throws ParseException {

		long time = getTime("2013.11.14 11:46:00.000");
		long time2 = getTime("2013.11.15 17:35:00.000");

		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();

		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .timeZone(TIMEZONE)
		                                                      .partitionType(PartitionType.BY_DAY)
		                                                      .addRecordType(trade)
		                                                      .build();

		CompositeRecordIterator recordIterator = CompositeRecordIterator.newBuilder(definition)
		                                                                .newRecord("Trade")
		                                                                .setTimestampInMillis(0, time)
		                                                                .setDecimal(1, 125, 1)
		                                                                .setLong(2, 10)
		                                                                .newRecord("Trade")
		                                                                .setTimestampInMillis(0, time + 50)
		                                                                .setDecimal(1, 124, 1)
		                                                                .setLong(2, 5)
		                                                                .newRecord("Trade")
		                                                                .setTimestampInMillis(0, time2)
		                                                                .setDecimal(1, 13, 0)
		                                                                .setLong(2, 6)
		                                                                .build();

		TimeSeriesRecord expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setTimestampInMillis(0, time);
		expected.setDecimal(1, 125, 1);
		expected.setLong(2, 10);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setDelta(true);
		expected.setTimestampInMillis(0, 50);
		expected.setDecimal(1, -1, 1);
		expected.setLong(2, -5);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setTimestampInMillis(0, time2);
		expected.setDecimal(1, 13, 0);
		expected.setLong(2, 6);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		Assert.assertFalse(recordIterator.hasNext());
	}

	@Test
	public void testWithThreeRecordsOfSameTypeInDisorder() throws ParseException {

		long time = getTime("2013.11.14 11:46:00.000");

		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();

		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .timeZone(TIMEZONE)
		                                                      .partitionType(PartitionType.BY_DAY)
		                                                      .addRecordType(trade)
		                                                      .build();

		CompositeRecordIterator recordIterator = CompositeRecordIterator.newBuilder(definition)
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

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setDelta(true);
		expected.setTimestampInMillis(0, 50);
		expected.setDecimal(1, -1, -1);
		expected.setLong(2, -5);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setDelta(true);
		expected.setTimestampInMillis(0, 70);
		expected.setDecimal(1, 6, -1);
		expected.setLong(2, 1);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		Assert.assertFalse(recordIterator.hasNext());
	}

	@Test
	public void testWithThreeRecordsOfSameTypeInDisorderWithTwoPartition() throws ParseException {

		long time = getTime("2013.11.14 11:46:00.000");
		long time2 = getTime("2013.11.15 17:35:00.000");

		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
		                                                 .addDecimalField("price")
		                                                 .addLongField("volume")
		                                                 .build();

		TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("test", "DAX")
		                                                      .timeUnit(TimeUnit.MILLISECONDS)
		                                                      .timeZone(TIMEZONE)
		                                                      .partitionType(PartitionType.BY_DAY)
		                                                      .addRecordType(trade)
		                                                      .build();

		CompositeRecordIterator recordIterator = CompositeRecordIterator.newBuilder(definition)
		                                                                .newRecord("Trade")
		                                                                .setTimestampInMillis(0, time2 + 50)
		                                                                .setDecimal(1, 124, -1)
		                                                                .setLong(2, 5)
		                                                                .newRecord("Trade")
		                                                                .setTimestampInMillis(0, time2 + 120)
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

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setTimestampInMillis(0, time2 + 50);
		expected.setDecimal(1, 124, -1);
		expected.setLong(2, 5);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expected.setDelta(true);
		expected.setTimestampInMillis(0, 70);
		expected.setDecimal(1, 6, -1);
		expected.setLong(2, 1);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expected, recordIterator.next());

		Assert.assertFalse(recordIterator.hasNext());
	}

	@Test
	public void testWithOnlyTwoRecordOfDifferentType() throws ParseException {

		long time = getTime("2013.11.14 11:46:00.000");

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
		                                                      .timeZone(TIMEZONE)
		                                                      .partitionType(PartitionType.BY_DAY)
		                                                      .addRecordType(quote)
		                                                      .addRecordType(trade)
		                                                      .build();

		CompositeRecordIterator recordIterator = CompositeRecordIterator.newBuilder(definition)
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

		TimeSeriesRecord expectedQuote = new TimeSeriesRecord(0,
		                                                      TimeUnit.MILLISECONDS,
		                                                      FieldType.DECIMAL,
		                                                      FieldType.DECIMAL,
		                                                      FieldType.LONG,
		                                                      FieldType.LONG);
		expectedQuote.setTimestampInMillis(0, time);
		expectedQuote.setDecimal(1, 123, 1);
		expectedQuote.setDecimal(2, 125, 1);
		expectedQuote.setLong(3, 6);
		expectedQuote.setLong(4, 14);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedQuote, recordIterator.next());

		TimeSeriesRecord expectedTrade = new TimeSeriesRecord(1,
		                                                      TimeUnit.MILLISECONDS,
		                                                      FieldType.DECIMAL,
		                                                      FieldType.LONG);
		expectedTrade.setTimestampInMillis(0, time + 100);
		expectedTrade.setDecimal(1, 125, 1);
		expectedTrade.setLong(2, 10);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedTrade, recordIterator.next());
		Assert.assertFalse(recordIterator.hasNext());
	}

	@Test
	public void testWithMultipleRecordsOfDifferentType() throws ParseException {

		long time = getTime("2013.11.14 11:46:00.000");

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
		                                                      .timeZone(TIMEZONE)
		                                                      .partitionType(PartitionType.BY_DAY)
		                                                      .addRecordType(trade)
		                                                      .addRecordType(quote)
		                                                      .addRecordType(exchangeState)
		                                                      .build();

		TimeSeriesRecordIterator recordIterator = TimeSeriesRecordIterator.newBuilder(definition)
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

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedES, recordIterator.next());

		TimeSeriesRecord expectedQuote = new TimeSeriesRecord(1,
		                                                      TimeUnit.MILLISECONDS,
		                                                      FieldType.DECIMAL,
		                                                      FieldType.DECIMAL,
		                                                      FieldType.LONG,
		                                                      FieldType.LONG);
		expectedQuote.setTimestampInMillis(0, time);
		expectedQuote.setDecimal(1, 123, -1);
		expectedQuote.setDecimal(2, 125, -1);
		expectedQuote.setLong(3, 6);
		expectedQuote.setLong(4, 14);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedQuote, recordIterator.next());

		TimeSeriesRecord expectedTrade = new TimeSeriesRecord(0,
		                                                      TimeUnit.MILLISECONDS,
		                                                      FieldType.DECIMAL,
		                                                      FieldType.LONG);
		expectedTrade.setTimestampInMillis(0, time + 5);
		expectedTrade.setDecimal(1, 125, -1);
		expectedTrade.setLong(2, 10);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedTrade, recordIterator.next());

		expectedQuote = new TimeSeriesRecord(1,
		                                     TimeUnit.MILLISECONDS,
		                                     FieldType.DECIMAL,
		                                     FieldType.DECIMAL,
		                                     FieldType.LONG,
		                                     FieldType.LONG);
		expectedQuote.setDelta(true);
		expectedQuote.setTimestampInMillis(0, 15);
		expectedQuote.setDecimal(1, 0, -1);
		expectedQuote.setDecimal(2, 0, -1);
		expectedQuote.setLong(3, 0);
		expectedQuote.setLong(4, -10);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedQuote, recordIterator.next());

		expectedTrade = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expectedTrade.setDelta(true);
		expectedTrade.setTimestampInMillis(0, 45);
		expectedTrade.setDecimal(1, -1, -1);
		expectedTrade.setLong(2, -5);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedTrade, recordIterator.next());

		expectedTrade = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expectedTrade.setDelta(true);
		expectedTrade.setTimestampInMillis(0, 70);
		expectedTrade.setDecimal(1, 6, -1);
		expectedTrade.setLong(2, 1);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedTrade, recordIterator.next());

		expectedQuote = new TimeSeriesRecord(1,
		                                     TimeUnit.MILLISECONDS,
		                                     FieldType.DECIMAL,
		                                     FieldType.DECIMAL,
		                                     FieldType.LONG,
		                                     FieldType.LONG);
		expectedQuote.setDelta(true);
		expectedQuote.setTimestampInMillis(0, 135);
		expectedQuote.setDecimal(1, 0, -1);
		expectedQuote.setDecimal(2, 0, -1);
		expectedQuote.setLong(3, 0);
		expectedQuote.setLong(4, 0);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedQuote, recordIterator.next());

		Assert.assertFalse(recordIterator.hasNext());
	}

	@Test
	public void testWithMultipleRecordsOfDifferentTypeAndTwoPartition() throws ParseException {

		long time = getTime("2013.11.14 11:46:00.000");
		long time2 = getTime("2013.11.15 17:35:00.000");

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
		                                                      .timeZone(TIMEZONE)
		                                                      .partitionType(PartitionType.BY_DAY)
		                                                      .addRecordType(trade)
		                                                      .addRecordType(quote)
		                                                      .addRecordType(exchangeState)
		                                                      .build();

		CompositeRecordIterator recordIterator = CompositeRecordIterator.newBuilder(definition)
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
		                                                                .setTimestampInMillis(0, time2 + 50)
		                                                                .setDecimal(1, 124, -1)
		                                                                .setLong(2, 5)
		                                                                .newRecord("Trade")
		                                                                .setTimestampInMillis(0, time2 + 120)
		                                                                .setDecimal(1, 13, 0)
		                                                                .setLong(2, 6)
		                                                                .newRecord("Quote")
		                                                                .setTimestampInMillis(0, time2 + 150)
		                                                                .setDecimal(1, 123, -1)
		                                                                .setDecimal(2, 125, -1)
		                                                                .setLong(3, 6)
		                                                                .setLong(4, 4)
		                                                                .build();

		TimeSeriesRecord expectedES = new TimeSeriesRecord(2, TimeUnit.MILLISECONDS, FieldType.BYTE);
		expectedES.setTimestampInMillis(0, time);
		expectedES.setByte(1, 1);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedES, recordIterator.next());

		TimeSeriesRecord expectedQuote = new TimeSeriesRecord(1,
		                                                      TimeUnit.MILLISECONDS,
		                                                      FieldType.DECIMAL,
		                                                      FieldType.DECIMAL,
		                                                      FieldType.LONG,
		                                                      FieldType.LONG);
		expectedQuote.setTimestampInMillis(0, time);
		expectedQuote.setDecimal(1, 123, -1);
		expectedQuote.setDecimal(2, 125, -1);
		expectedQuote.setLong(3, 6);
		expectedQuote.setLong(4, 14);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedQuote, recordIterator.next());

		TimeSeriesRecord expectedTrade = new TimeSeriesRecord(0,
		                                                      TimeUnit.MILLISECONDS,
		                                                      FieldType.DECIMAL,
		                                                      FieldType.LONG);
		expectedTrade.setTimestampInMillis(0, time + 5);
		expectedTrade.setDecimal(1, 125, -1);
		expectedTrade.setLong(2, 10);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedTrade, recordIterator.next());

		expectedQuote = new TimeSeriesRecord(1,
		                                     TimeUnit.MILLISECONDS,
		                                     FieldType.DECIMAL,
		                                     FieldType.DECIMAL,
		                                     FieldType.LONG,
		                                     FieldType.LONG);
		expectedQuote.setDelta(true);
		expectedQuote.setTimestampInMillis(0, 15);
		expectedQuote.setDecimal(1, 0, -1);
		expectedQuote.setDecimal(2, 0, -1);
		expectedQuote.setLong(3, 0);
		expectedQuote.setLong(4, -10);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedQuote, recordIterator.next());

		expectedTrade = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expectedTrade.setTimestampInMillis(0, time2 + 50);
		expectedTrade.setDecimal(1, 124, -1);
		expectedTrade.setLong(2, 5);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedTrade, recordIterator.next());

		expectedTrade = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
		expectedTrade.setDelta(true);
		expectedTrade.setTimestampInMillis(0, 70);
		expectedTrade.setDecimal(1, 6, -1);
		expectedTrade.setLong(2, 1);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedTrade, recordIterator.next());

		expectedQuote = new TimeSeriesRecord(1,
		                                     TimeUnit.MILLISECONDS,
		                                     FieldType.DECIMAL,
		                                     FieldType.DECIMAL,
		                                     FieldType.LONG,
		                                     FieldType.LONG);
		expectedQuote.setTimestampInMillis(0, time2 + 150);
		expectedQuote.setDecimal(1, 123, -1);
		expectedQuote.setDecimal(2, 125, -1);
		expectedQuote.setLong(3, 6);
		expectedQuote.setLong(4, 4);

		Assert.assertTrue(recordIterator.hasNext());
		Assert.assertEquals(expectedQuote, recordIterator.next());

		Assert.assertFalse(recordIterator.hasNext());
	}

	/**
	 * Returns the time in milliseconds corresponding to the specified
	 * {@link String}.
	 * 
	 * @param dateAsText the date/time to convert in milliseconds
	 * @return the time in milliseconds corresponding to the specified
	 *         {@link String}.
	 * @throws ParseException if a problem occurs while generating the time.
	 */
	private static long getTime(String dateAsText) throws ParseException {

		SimpleDateFormat format = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS");
		return format.parse(dateAsText).getTime();
	}

}
