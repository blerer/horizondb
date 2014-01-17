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
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.model.BinaryTimeSeriesRecordIterator;
import io.horizondb.model.DatabaseDefinition;
import io.horizondb.model.FieldType;
import io.horizondb.model.RecordIterator;
import io.horizondb.model.RecordTypeDefinition;
import io.horizondb.model.TimeSeriesDefinition;
import io.horizondb.model.TimeSeriesRecordIterator;
import io.horizondb.model.core.Record;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Benjamin
 * 
 */
public class MemTimeSeriesTest {

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

        TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(def)
                                                                    .newRecord("exchangeState")
                                                                    .setTimestampInNanos(0, 12000700)
                                                                    .setTimestampInMillis(1, 12)
                                                                    .setByte(2, 3)
                                                                    .newRecord("exchangeState")
                                                                    .setTimestampInNanos(0, 13000900)
                                                                    .setTimestampInMillis(1, 13)
                                                                    .setByte(2, 3)
                                                                    .newRecord("exchangeState")
                                                                    .setTimestampInNanos(0, 13004400)
                                                                    .setTimestampInMillis(1, 13)
                                                                    .setByte(2, 1)
                                                                    .build();

        memTimeSeries = memTimeSeries.write(allocator, iterator, newFuture());
        assertEquals(13004400, memTimeSeries.getGreatestTimestamp());

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, memTimeSeries.newInput())) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(12000700L, actual.getTimestampInNanos(0));
            assertEquals(12, actual.getTimestampInMillis(1));
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
    public void testWriteWithMultipleIterators() throws Exception {

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

        TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(def)
                                                                    .newRecord("exchangeState")
                                                                    .setTimestampInNanos(0, 12000700)
                                                                    .setTimestampInMillis(1, 12)
                                                                    .setByte(2, 3)
                                                                    .build();

        memTimeSeries = memTimeSeries.write(allocator, iterator, newFuture());

        iterator = TimeSeriesRecordIterator.newBuilder(def)
                                           .newRecord("exchangeState")
                                           .setTimestampInNanos(0, 13000900)
                                           .setTimestampInMillis(1, 13)
                                           .setByte(2, 3)
                                           .build();

        memTimeSeries = memTimeSeries.write(allocator, iterator, newFuture());

        iterator = TimeSeriesRecordIterator.newBuilder(def)
                                           .newRecord("exchangeState")
                                           .setTimestampInNanos(0, 13004400)
                                           .setTimestampInMillis(1, 13)
                                           .setByte(2, 1)
                                           .build();

        memTimeSeries = memTimeSeries.write(allocator, iterator, newFuture());

        assertEquals(13004400, memTimeSeries.getGreatestTimestamp());

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, memTimeSeries.newInput())) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(12000700L, actual.getTimestampInNanos(0));
            assertEquals(12, actual.getTimestampInMillis(1));
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

        TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(def)
                                                                    .newRecord("exchangeState")
                                                                    .setTimestampInNanos(0, 12000700)
                                                                    .setTimestampInMillis(1, 12)
                                                                    .setByte(2, 3)
                                                                    .build();

        MemTimeSeries secondMemTimeSeries = firstMemTimeSeries.write(allocator, iterator, newFuture());

        iterator = TimeSeriesRecordIterator.newBuilder(def)
                                           .newRecord("exchangeState")
                                           .setTimestampInNanos(0, 13000900)
                                           .setTimestampInMillis(1, 13)
                                           .setByte(2, 3)
                                           .build();

        MemTimeSeries thirdMemTimeSeries = secondMemTimeSeries.write(allocator, iterator, newFuture());

        iterator = TimeSeriesRecordIterator.newBuilder(def)
                                           .newRecord("exchangeState")
                                           .setTimestampInNanos(0, 13004400)
                                           .setTimestampInMillis(1, 13)
                                           .setByte(2, 1)
                                           .build();

        MemTimeSeries fourthMemTimeSeries = thirdMemTimeSeries.write(allocator, iterator, newFuture());

        assertEquals(13000900, thirdMemTimeSeries.getGreatestTimestamp());
        assertEquals(13004400, fourthMemTimeSeries.getGreatestTimestamp());

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, firstMemTimeSeries.newInput())) {
            assertFalse(readIterator.hasNext());
        }

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, secondMemTimeSeries.newInput())) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(12000700L, actual.getTimestampInNanos(0));
            assertEquals(12, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }

        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, fourthMemTimeSeries.newInput())) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(12000700L, actual.getTimestampInNanos(0));
            assertEquals(12, actual.getTimestampInMillis(1));
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
    public void testWriteWithUpdates() throws Exception {

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

        TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(def)
                                                                    .newRecord("exchangeState")
                                                                    .setTimestampInNanos(0, 12000700)
                                                                    .setTimestampInMillis(1, 12)
                                                                    .setByte(2, 3)
                                                                    .build();

        memTimeSeries = memTimeSeries.write(allocator, iterator, newFuture());

        iterator = TimeSeriesRecordIterator.newBuilder(def)
                                           .newRecord("exchangeState")
                                           .setTimestampInNanos(0, 13004400)
                                           .setTimestampInMillis(1, 13)
                                           .setByte(2, 1)
                                           .build();

        memTimeSeries = memTimeSeries.write(allocator, iterator, newFuture());

        iterator = TimeSeriesRecordIterator.newBuilder(def)
                                           .newRecord("exchangeState")
                                           .setTimestampInNanos(0, 13000900)
                                           .setTimestampInMillis(1, 13)
                                           .setByte(2, 3)
                                           .build();

        try {
            memTimeSeries.write(allocator, iterator, newFuture());
            fail();

        } catch (HorizonDBException e) {
            assertTrue(true);
        }

    }

    private static Future<ReplayPosition> newFuture() {

        return EasyMock.createNiceMock(Future.class);
    }
}
