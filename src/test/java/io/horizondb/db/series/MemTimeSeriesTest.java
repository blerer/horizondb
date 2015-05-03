/**
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
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.RecordListBuilder;
import io.horizondb.model.core.iterators.BinaryTimeSeriesRecordIterator;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.junit.Test;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static io.horizondb.model.schema.FieldType.NANOSECONDS_TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class MemTimeSeriesTest {

    /**
     * The time reference.
     */
    private static long TIME_IN_MILLIS = TimeUtils.parseDateTime("2013-11-26 12:00:00.000");

    /**
     * The time reference.
     */
    private static long TIME_IN_NANOS = TimeUnit.MILLISECONDS.toNanos(TIME_IN_MILLIS);

    @Test
    public void testWriteWithOneIterator() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition def = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                     .timeUnit(TimeUnit.NANOSECONDS)
                                                     .addRecordType(recordTypeDefinition)
                                                     .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(configuration, def);

        List<BinaryTimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 12000700)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                         .setByte(2, 3)
                                                                         .newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 13000900)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                         .setByte(2, 3)
                                                                         .newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 13004400)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                         .setByte(2, 1)
                                                                         .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());
        assertEquals(TIME_IN_NANOS + 13004400, memTimeSeries.getGreatestTimestamp());

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, memTimeSeries.newInput())) {

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
    public void testGetFirstSegmentId() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition def = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                     .timeUnit(TimeUnit.NANOSECONDS)
                                                     .addRecordType(recordTypeDefinition)
                                                     .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(configuration, def);

        List<BinaryTimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 12000700)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                         .setByte(2, 3)
                                                                         .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, Futures.immediateFuture(new ReplayPosition(1, 1)));

        assertEquals(1, memTimeSeries.getFirstSegmentId());

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                            .setByte(2, 3)
                                            .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, Futures.immediateFuture(new ReplayPosition(2, 1)));

        assertEquals(1, memTimeSeries.getFirstSegmentId());
    }

    @Test
    public void testWriteWithMultipleBatchsDelta() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition def = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                     .timeUnit(TimeUnit.NANOSECONDS)
                                                     .addRecordType(recordTypeDefinition)
                                                     .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(configuration, def);

        List<BinaryTimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 12000700)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                         .setByte(2, 3)
                                                                         .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                            .setByte(2, 3)
                                            .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 13004400)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                            .setByte(2, 1)
                                            .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        assertEquals(TIME_IN_NANOS + 13004400, memTimeSeries.getGreatestTimestamp());

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, memTimeSeries.newInput())) {

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
    public void testWriteWithMultipleBatchsWithDeltas() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition def = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                     .timeUnit(TimeUnit.NANOSECONDS)
                                                     .addRecordType(recordTypeDefinition)
                                                     .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(configuration, def);

        List<BinaryTimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 12000700)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                         .setByte(2, 3)
                                                                         .newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 13000900)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                         .setByte(2, 3)
                                                                         .newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 13004400)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                         .setByte(2, 1)
                                                                         .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 13006400)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                            .setByte(2, 2)
                                            .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        assertEquals(TIME_IN_NANOS + 13006400, memTimeSeries.getGreatestTimestamp());

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, memTimeSeries.newInput())) {

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
    public void testUpdateWriteBeforeExistingData() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition def = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                     .timeUnit(TimeUnit.NANOSECONDS)
                                                     .addRecordType(recordTypeDefinition)
                                                     .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(configuration, def);

        List<BinaryTimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
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
                                                                         .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 11000800)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 11)
                                            .setByte(2, 2)
                                            .newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 12000200)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                            .setByte(2, 4)
                                            .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        assertEquals(TIME_IN_NANOS + 13004400, memTimeSeries.getGreatestTimestamp());

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, memTimeSeries.newInput())) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 11000800L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 11, actual.getTimestampInMillis(1));
            assertEquals(2, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(400, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(2, actual.getByte(2));
            
            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(300, actual.getTimestampInNanos(0));
            assertEquals(0, actual.getTimestampInMillis(1));
            assertEquals(-2, actual.getByte(2));
            
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
    public void testWriteWithMultipleBatchsWithDeltasAndMultipleBlocks() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition def = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                     .timeUnit(TimeUnit.NANOSECONDS)
                                                     .addRecordType(recordTypeDefinition)
                                                     .blockSize(35)
                                                     .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(configuration, def);

        List<BinaryTimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 12000700)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                         .setByte(2, 3)
                                                                         .newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 13000900)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                         .setByte(2, 3)
                                                                         .newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 13004400)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                         .setByte(2, 1)
                                                                         .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        assertEquals(1, memTimeSeries.getNumberOfBlocks());

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 13006400)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                            .setByte(2, 2)
                                            .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        assertEquals(2, memTimeSeries.getNumberOfBlocks());
        assertEquals(TIME_IN_NANOS + 13006400, memTimeSeries.getGreatestTimestamp());

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, memTimeSeries.newInput())) {

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

            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 13006400, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 14, actual.getTimestampInMillis(1));
            assertEquals(2, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }
    }

    @Test
    public void testReadWithOnlySecondOneOfTwoBlocksHit() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition def = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                     .timeUnit(TimeUnit.NANOSECONDS)
                                                     .addRecordType(recordTypeDefinition)
                                                     .blockSize(35)
                                                     .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(configuration, def);

        List<BinaryTimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 12000700)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                         .setByte(2, 3)
                                                                         .newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 13000900)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                         .setByte(2, 3)
                                                                         .newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 13004400)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                         .setByte(2, 1)
                                                                         .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        assertEquals(1, memTimeSeries.getNumberOfBlocks());

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 13006400)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                            .setByte(2, 2)
                                            .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        assertEquals(2, memTimeSeries.getNumberOfBlocks());
        assertEquals(TIME_IN_NANOS + 13006400, memTimeSeries.getGreatestTimestamp());

        Field from = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 13006000);
        Field to = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 14000000);

        RangeSet<Field> rangeSet = ImmutableRangeSet.of(Range.closed(from, to));

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, memTimeSeries.newInput(rangeSet))) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 13006400, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 14, actual.getTimestampInMillis(1));
            assertEquals(2, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }
    }

    @Test
    public void testReadWithOnlyFirstOneOfTwoBlocksHit() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition def = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                     .timeUnit(TimeUnit.NANOSECONDS)
                                                     .addRecordType(recordTypeDefinition)
                                                     .blockSize(35)
                                                     .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(configuration, def);

        List<BinaryTimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 12000700)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                         .setByte(2, 3)
                                                                         .newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 13000900)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                         .setByte(2, 3)
                                                                         .newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 13004400)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                         .setByte(2, 1)
                                                                         .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        assertEquals(1, memTimeSeries.getNumberOfBlocks());

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 13006400)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                            .setByte(2, 2)
                                            .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        assertEquals(2, memTimeSeries.getNumberOfBlocks());
        assertEquals(TIME_IN_NANOS + 13006400, memTimeSeries.getGreatestTimestamp());

        Field from = FieldType.NANOSECONDS_TIMESTAMP.newField();
        from.setTimestampInNanos(TIME_IN_NANOS + 12000000);

        Field to = FieldType.NANOSECONDS_TIMESTAMP.newField();
        to.setTimestampInNanos(TIME_IN_NANOS + 13000000);

        RangeSet<Field> rangeSet = ImmutableRangeSet.of(Range.closed(from, to));

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, memTimeSeries.newInput(rangeSet))) {

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
    public void testReadWithNoBlocksHit() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition def = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                     .timeUnit(TimeUnit.NANOSECONDS)
                                                     .addRecordType(recordTypeDefinition)
                                                     .blockSize(35)
                                                     .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(configuration, def);

        List<BinaryTimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 12000700)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                         .setByte(2, 3)
                                                                         .newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 13000900)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                         .setByte(2, 3)
                                                                         .newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 13004400)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                         .setByte(2, 1)
                                                                         .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        assertEquals(1, memTimeSeries.getNumberOfBlocks());

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 13006400)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                            .setByte(2, 2)
                                            .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        assertEquals(2, memTimeSeries.getNumberOfBlocks());
        assertEquals(TIME_IN_NANOS + 13006400, memTimeSeries.getGreatestTimestamp());

        Field from = FieldType.NANOSECONDS_TIMESTAMP.newField();
        from.setTimestampInNanos(TIME_IN_NANOS + 14000000);

        Field to = FieldType.NANOSECONDS_TIMESTAMP.newField();
        to.setTimestampInNanos(TIME_IN_NANOS + 15000000);

        RangeSet<Field> rangeSet = ImmutableRangeSet.of(Range.closed(from, to));

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, memTimeSeries.newInput(rangeSet))) {

            assertFalse(readIterator.hasNext());
        }
    }

    @Test
    public void testReadSameDataConcurently() throws Exception {

        Configuration configuration = Configuration.newBuilder().build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition def = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                     .timeUnit(TimeUnit.NANOSECONDS)
                                                     .addRecordType(recordTypeDefinition)
                                                     .blockSize(35)
                                                     .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(configuration, def);

        List<BinaryTimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
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
                                                                         .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        assertEquals(1, memTimeSeries.getNumberOfBlocks());

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 13006400)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                            .setByte(2, 2)
                                            .buildBinaryRecords();

        memTimeSeries = memTimeSeries.write(allocator, records, newFuture());

        assertEquals(2, memTimeSeries.getNumberOfBlocks());
        assertEquals(TIME_IN_NANOS + 13006400, memTimeSeries.getGreatestTimestamp());

        Field from = FieldType.NANOSECONDS_TIMESTAMP.newField();
        from.setTimestampInNanos(TIME_IN_NANOS + 12000000);

        Field to = FieldType.NANOSECONDS_TIMESTAMP.newField();
        to.setTimestampInNanos(TIME_IN_NANOS + 14000000);

        RangeSet<Field> rangeSet = ImmutableRangeSet.of(Range.closed(from, to));

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, memTimeSeries.newInput(rangeSet))) {
            try (RecordIterator readIterator2 = new BinaryTimeSeriesRecordIterator(def, memTimeSeries.newInput(rangeSet))) {

                assertTrue(readIterator.hasNext());
                Record actual = readIterator.next();
                
                assertTrue(readIterator2.hasNext());
                Record actual2 = readIterator2.next();

                assertFalse(actual.isDelta());
                assertFalse(actual2.isDelta());
                assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
                assertEquals(TIME_IN_NANOS + 12000700L, actual2.getTimestampInNanos(0));
                assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
                assertEquals(TIME_IN_MILLIS + 12, actual2.getTimestampInMillis(1));
                assertEquals(3, actual.getByte(2));
                assertEquals(3, actual2.getByte(2));

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

                assertTrue(readIterator2.hasNext());
                actual2 = readIterator2.next();

                assertTrue(actual2.isDelta());
                assertEquals(1000200, actual2.getTimestampInNanos(0));
                assertEquals(1, actual2.getTimestampInMillis(1));
                assertEquals(0, actual2.getByte(2));

                assertTrue(readIterator2.hasNext());
                actual = readIterator2.next();

                assertTrue(actual2.isDelta());
                assertEquals(3500, actual2.getTimestampInNanos(0));
                assertEquals(0, actual2.getTimestampInMillis(1));
                assertEquals(-2, actual2.getByte(2));

                assertTrue(readIterator2.hasNext());
                actual2 = readIterator2.next();

                assertFalse(actual2.isDelta());
                assertEquals(TIME_IN_NANOS + 13006400, actual2.getTimestampInNanos(0));
                assertEquals(TIME_IN_MILLIS + 14, actual2.getTimestampInMillis(1));
                assertEquals(2, actual2.getByte(2));

                actual = readIterator.next();

                assertFalse(actual.isDelta());
                assertEquals(TIME_IN_NANOS + 13006400, actual.getTimestampInNanos(0));
                assertEquals(TIME_IN_MILLIS + 14, actual.getTimestampInMillis(1));
                assertEquals(2, actual.getByte(2));

                assertFalse(readIterator.hasNext());

                assertFalse(readIterator2.hasNext());
            }
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

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition def = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                     .timeUnit(TimeUnit.NANOSECONDS)
                                                     .addRecordType(recordTypeDefinition)
                                                     .build();

        SlabAllocator allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        MemTimeSeries firstMemTimeSeries = new MemTimeSeries(configuration, def);

        List<BinaryTimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
                                                                         .setTimestampInNanos(0,
                                                                                              TIME_IN_NANOS + 12000700)
                                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                         .setByte(2, 3)
                                                                         .buildBinaryRecords();

        MemTimeSeries secondMemTimeSeries = firstMemTimeSeries.write(allocator, records, newFuture());

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                            .setByte(2, 3)
                                            .buildBinaryRecords();

        MemTimeSeries thirdMemTimeSeries = secondMemTimeSeries.write(allocator, records, newFuture());

        records = new RecordListBuilder(def).newRecord("exchangeState")
                                            .setTimestampInNanos(0, TIME_IN_NANOS + 13004400)
                                            .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                            .setByte(2, 1)
                                            .buildBinaryRecords();

        MemTimeSeries fourthMemTimeSeries = thirdMemTimeSeries.write(allocator, records, newFuture());

        assertEquals(TIME_IN_NANOS + 13000900, thirdMemTimeSeries.getGreatestTimestamp());
        assertEquals(TIME_IN_NANOS + 13004400, fourthMemTimeSeries.getGreatestTimestamp());

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, firstMemTimeSeries.newInput())) {
            assertFalse(readIterator.hasNext());
        }

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, secondMemTimeSeries.newInput())) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, fourthMemTimeSeries.newInput())) {

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

    private static ListenableFuture<ReplayPosition> newFuture() {

        return EasyMock.createNiceMock(ListenableFuture.class);
    }
}
