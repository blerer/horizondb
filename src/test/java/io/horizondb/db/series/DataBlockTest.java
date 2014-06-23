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
package io.horizondb.db.series;

import io.horizondb.db.Configuration;
import io.horizondb.io.BufferAllocator;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.RecordListBuilder;
import io.horizondb.model.core.iterators.BinaryTimeSeriesRecordIterator;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.Range;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 * 
 */
public class DataBlockTest {

    /**
     * The time reference.
     */
    private static long TIME_IN_MILLIS = TimeUtils.parseDateTime("2013-11-26 12:00:00.000");

    /**
     * The time reference.
     */
    private static long TIME_IN_NANOS = TimeUnit.MILLISECONDS.toNanos(TIME_IN_MILLIS);

    @Test
    public void testWriteWithASetOfRecordsStartingWithFullRecord() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .build();

        BufferAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        DataBlock dataBlock = new DataBlock(def);

        List<TimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 12000700)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                   .setByte(2, 3)
                                                                   .newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                   .setByte(2, 3)
                                                                   .newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13004400)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                   .setByte(2, 1)
                                                                   .build();

        TimeSeriesRecord[] previousRecords = new TimeSeriesRecord[1];
        
        dataBlock = dataBlock.write(allocator, previousRecords, records);

        assertEquals(range(TIME_IN_NANOS + 12000700L, TIME_IN_NANOS + 13004400L), dataBlock.getRange());
        assertEquals(3, dataBlock.getNumberOfRecords(0));

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, dataBlock.newInput())) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();
            
            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(1000200, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(0, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(3500, actual.getTimestampInNanos(0));
            assertEquals(0, actual.getTimestampInMillis(1));
            assertEquals(-2, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }
    }
        
    @Test
    public void testNewInputWithTwoConsecutiveCalls() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .build();

        BufferAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        DataBlock dataBlock = new DataBlock(def);

        List<TimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 12000700)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                   .setByte(2, 3)
                                                                   .newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                   .setByte(2, 3)
                                                                   .newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13004400)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                   .setByte(2, 1)
                                                                   .build();
        
        dataBlock = dataBlock.write(allocator, new TimeSeriesRecord[1], records);

        for (int i = 0; i < 2; i++) {

            try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, dataBlock.newInput())) {

                assertTrue(readIterator.hasNext());
                Record actual = readIterator.next();

                assertFalse(actual.isDelta());
                assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
                assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
                assertEquals(3, actual.getByte(2));

                assertTrue(readIterator.hasNext());
                actual = readIterator.next();

                assertTrue(actual.isDelta());
                assertEquals(1000200, actual.getTimestampInNanos(0));
                assertEquals(1, actual.getTimestampInMillis(1));
                assertEquals(0, actual.getByte(2));

                assertTrue(readIterator.hasNext());
                actual = readIterator.next();

                assertTrue(actual.isDelta());
                assertEquals(3500, actual.getTimestampInNanos(0));
                assertEquals(0, actual.getTimestampInMillis(1));
                assertEquals(-2, actual.getByte(2));

                assertFalse(readIterator.hasNext());
            }
        }
    }

    @Test
    public void testWriteWithASetOfRecordsStartingWithDelta() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .build();

        BufferAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        DataBlock dataBlock = new DataBlock(def);

        List<TimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 12000700)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                   .setByte(2, 3)
                                                                   .newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                   .setByte(2, 3)
                                                                   .newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13004400)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                   .setByte(2, 1)
                                                                   .build();
                
        TimeSeriesRecord[] previousRecords = new TimeSeriesRecord[]{records.remove(0)};
                
        dataBlock = dataBlock.write(allocator, previousRecords, records);
        
        assertEquals(range(TIME_IN_NANOS + 13000900L, TIME_IN_NANOS + 13004400L), dataBlock.getRange());
        assertEquals(2, dataBlock.getNumberOfRecords(0));

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, dataBlock.newInput())) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 13000900, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 13, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(3500, actual.getTimestampInNanos(0));
            assertEquals(0, actual.getTimestampInMillis(1));
            assertEquals(-2, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }
    }

    @Test
    public void testWriteWithAnEmptyBlockAndASetOfRecordsStartingWithDelta() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .build();

        BufferAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        DataBlock dataBlock = new DataBlock(def);

        List<TimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 12000700)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                   .setByte(2, 3)
                                                                   .newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                   .setByte(2, 3)
                                                                   .newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13004400)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                   .setByte(2, 1)
                                                                   .build();
                
        TimeSeriesRecord[] previousRecords = new TimeSeriesRecord[]{records.remove(0)};
                
        dataBlock = dataBlock.write(allocator, previousRecords, records);
        
        assertEquals(range(TIME_IN_NANOS + 13000900L, TIME_IN_NANOS + 13004400L), dataBlock.getRange());
        assertEquals(2, dataBlock.getNumberOfRecords(0));

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, dataBlock.newInput())) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 13000900, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 13, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(3500, actual.getTimestampInNanos(0));
            assertEquals(0, actual.getTimestampInMillis(1));
            assertEquals(-2, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }
    }
        
    @Test
    public void testWriteWithMultipleRecordsWithoutDelta() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();
        
        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        DataBlock dataBlock = new DataBlock(def);

        List<TimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                          .setTimestampInNanos(0,
                                                                                               TIME_IN_NANOS + 12000700)
                                                                          .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                          .setByte(2, 3)
                                                                          .build();

        TimeSeriesRecord[] previousRecords = new TimeSeriesRecord[1];
        
        dataBlock = dataBlock.write(allocator, previousRecords, records);
        
        assertEquals(range(TIME_IN_NANOS + 12000700, TIME_IN_NANOS + 12000700), dataBlock.getRange());
        assertEquals(1, dataBlock.getNumberOfRecords(0));

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                             .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                             .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                             .setByte(2, 3)
                                             .build();

        dataBlock = dataBlock.write(allocator, previousRecords, records);
        
        assertEquals(range(TIME_IN_NANOS + 12000700, TIME_IN_NANOS + 13000900), dataBlock.getRange());
        assertEquals(2, dataBlock.getNumberOfRecords(0));

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 13004400)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                            .setByte(2, 1)
                                            .build();

        dataBlock = dataBlock.write(allocator, previousRecords, records);
        
        assertEquals(range(TIME_IN_NANOS + 12000700, TIME_IN_NANOS + 13004400), dataBlock.getRange());
        assertEquals(3, dataBlock.getNumberOfRecords(0));

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, dataBlock.newInput())) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(1000200, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(0, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(3500, actual.getTimestampInNanos(0));
            assertEquals(0, actual.getTimestampInMillis(1));
            assertEquals(-2, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }
    }

    @Test
    public void testWriteWithMultipleSetOfRecordsWithDeltas() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        DataBlock dataBlock = new DataBlock(def);

        List<TimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 12000700)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                   .setByte(2, 3)
                                                                   .newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                   .setByte(2, 3)
                                                                   .newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13004400)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                   .setByte(2, 1)
                                                                   .build();
        
        TimeSeriesRecord[] previousRecords = new TimeSeriesRecord[1];

        dataBlock = dataBlock.write(allocator, previousRecords, records);
        assertEquals(range(TIME_IN_NANOS + 12000700, TIME_IN_NANOS + 13004400), dataBlock.getRange());
        assertEquals(3, dataBlock.getNumberOfRecords(0));

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                             .setTimestampInNanos(0, TIME_IN_NANOS + 13006400)
                                             .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                             .setByte(2, 2)
                                             .build();

        dataBlock = dataBlock.write(allocator, previousRecords, records);
        assertEquals(range(TIME_IN_NANOS + 12000700, TIME_IN_NANOS + 13006400), dataBlock.getRange());
        assertEquals(4, dataBlock.getNumberOfRecords(0));

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, dataBlock.newInput())) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(1000200, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(0, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(3500, actual.getTimestampInNanos(0));
            assertEquals(0, actual.getTimestampInMillis(1));
            assertEquals(-2, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(2000, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(1, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }
    }

    @Test
    public void testWriteImmutability() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                     .timeUnit(TimeUnit.NANOSECONDS)
                                                     .addRecordType(recordTypeDefinition)
                                                     .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        DataBlock firstDataBlock = new DataBlock(def); 

        List<TimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 12000700)
                                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                   .setByte(2, 3)
                                                                   .build();

        TimeSeriesRecord[] previousRecords = new TimeSeriesRecord[1];
        
        DataBlock secondDataBlock = firstDataBlock.write(allocator, previousRecords, records);

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                             .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                             .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                             .setByte(2, 3)
                                             .build();

        DataBlock thirdDataBlock = secondDataBlock.write(allocator, previousRecords, records);

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                             .setTimestampInNanos(0, TIME_IN_NANOS + 13004400)
                                             .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                             .setByte(2, 1)
                                             .build();

        DataBlock fourthDataBlock = thirdDataBlock.write(allocator, previousRecords, records);
        
        assertEquals(range(TIME_IN_NANOS + 12000700, TIME_IN_NANOS + 12000700), secondDataBlock.getRange());
        assertEquals(1, secondDataBlock.getNumberOfRecords(0));

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, firstDataBlock.newInput())) {
            assertFalse(readIterator.hasNext());
        }

        assertEquals(range(TIME_IN_NANOS + 12000700, TIME_IN_NANOS + 13000900), thirdDataBlock.getRange());
        assertEquals(2, thirdDataBlock.getNumberOfRecords(0));
        
        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, secondDataBlock.newInput())) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }

        assertEquals(range(TIME_IN_NANOS + 12000700, TIME_IN_NANOS + 13000900), thirdDataBlock.getRange());
        assertEquals(2, thirdDataBlock.getNumberOfRecords(0));
        
        assertEquals(range(TIME_IN_NANOS + 12000700, TIME_IN_NANOS + 13004400), fourthDataBlock.getRange());
        assertEquals(3, fourthDataBlock.getNumberOfRecords(0));
        
        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, fourthDataBlock.newInput())) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(1000200, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(0, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(3500, actual.getTimestampInNanos(0));
            assertEquals(0, actual.getTimestampInMillis(1));
            assertEquals(-2, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }
    }

    /**
     * Creates a closed range from the specified time to the specified time.
     * 
     * @param fromInNanosecond the lower time in nanoseconds
     * @param toInNanoSeconds the upper time in nanoseconds
     * @return a closed range from the specified time to the specified time.
     */
    private static Range<Field> range(long fromInNanosecond, long toInNanoSeconds) {

        Field fromField = FieldType.NANOSECONDS_TIMESTAMP.newField();
        fromField.setTimestampInNanos(fromInNanosecond);

        Field toField = FieldType.NANOSECONDS_TIMESTAMP.newField();
        toField.setTimestampInNanos(toInNanoSeconds);

        Range<Field> expected = Range.closed(fromField, toField);
        return expected;
    }
}
