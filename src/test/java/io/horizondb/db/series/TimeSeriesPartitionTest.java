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
import io.horizondb.io.files.FileUtils;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.iterators.DefaultRecordIterator;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static io.horizondb.db.util.TimeUtils.getTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 * 
 */
public class TimeSeriesPartitionTest {

    /**
	 * 
	 */
    private static final int MEMTIMESERIES_SIZE = 50;

    /**
     * The test directory.
     */
    private Path testDirectory;

    private TimeSeriesPartition partition;

    private TimeSeriesDefinition def;

    private TimeSeriesPartitionManager manager;

    private TimeSeriesPartitionListener listener;

    @Before
    public void setUp() throws Exception {

        this.testDirectory = Files.createTempDirectory(this.getClass().getSimpleName());

        Configuration configuration = Configuration.newBuilder()
                                                   .dataDirectory(this.testDirectory)
                                                   .memTimeSeriesSize(MEMTIMESERIES_SIZE)
                                                   .build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        Files.createDirectory(this.testDirectory.resolve("test"));

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        this.def = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                     .timeUnit(TimeUnit.NANOSECONDS)
                                     .addRecordType(recordTypeDefinition)
                                     .build();

        Range<Long> range = newTimeRange("2013.11.26 00:00:00.000", "2013.11.27 00:00:00.000");

        TimeSeriesPartitionMetaData metadata = TimeSeriesPartitionMetaData.newBuilder(range).build();

        this.manager = EasyMock.createMock(TimeSeriesPartitionManager.class);
        this.listener = EasyMock.createMock(TimeSeriesPartitionListener.class);

        this.partition = new TimeSeriesPartition(this.manager, 
                                                 configuration, 
                                                 databaseDefinition.getName(), 
                                                 this.def, 
                                                 metadata);
        
        this.partition.addListener(this.listener);
    }

    @After
    public void tearDown() throws Exception {

        this.partition = null;
        FileUtils.forceDelete(this.testDirectory);
        this.testDirectory = null;
    }

    @Test
    public void testReadWithNoData() throws IOException {

        EasyMock.replay(this.manager, this.listener);

        Range<Long> range = newTimeRange("2013.11.26 12:00:00.000", "2013.11.26 14:00:00.000");

        RecordIterator iterator = this.partition.read(range);

        Assert.assertFalse(iterator.hasNext());

        EasyMock.verify(this.manager, this.listener);
    }

    @Test
    public void testWrite() throws IOException, HorizonDBException {

        this.listener.memoryUsageChanged(this.partition, 0, MEMTIMESERIES_SIZE);
        this.listener.firstSegmentContainingNonPersistedDataChanged(this.partition, null, Long.valueOf(0));

        EasyMock.replay(this.manager, this.listener);

        Range<Long> range = newTimeRange("2013.11.26 12:00:00.000", "2013.11.26 14:00:00.000");

        long timestamp = getTime("2013.11.26 12:32:12.000");

        RecordIterator recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp)
                                                             .setTimestampInMillis(1, timestamp)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 100)
                                                             .setTimestampInMillis(1, timestamp + 100)
                                                             .setByte(2, 5)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 350)
                                                             .setTimestampInMillis(1, timestamp + 350)
                                                             .setByte(2, 10)
                                                             .build();

        this.partition.write(recordIterator, newFuture(0, 1));
        assertEquals(MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());

        RecordIterator iterator = this.partition.read(range);

        assertTrue(iterator.hasNext());
        Record actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp, actual.getTimestampInMillis(0));
        assertEquals(timestamp, actual.getTimestampInMillis(1));
        assertEquals(10, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100L, actual.getTimestampInMillis(0));
        assertEquals(100L, actual.getTimestampInMillis(1));
        assertEquals(-5, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(250, actual.getTimestampInMillis(0));
        assertEquals(250, actual.getTimestampInMillis(1));
        assertEquals(5, actual.getByte(2));

        assertFalse(iterator.hasNext());

        EasyMock.verify(this.manager, this.listener);
    }

    @Test
    public void testTwoWriteOnSameMemTimeSeries() throws IOException, HorizonDBException {

        this.listener.memoryUsageChanged(this.partition, 0, MEMTIMESERIES_SIZE);
        this.listener.firstSegmentContainingNonPersistedDataChanged(this.partition, null, Long.valueOf(0));

        EasyMock.replay(this.manager, this.listener);

        long timestamp = getTime("2013.11.26 12:32:12.000");

        RecordIterator recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp)
                                                             .setTimestampInMillis(1, timestamp)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 100)
                                                             .setTimestampInMillis(1, timestamp + 100)
                                                             .setByte(2, 5)
                                                             .build();

        this.partition.write(recordIterator, newFuture(0, 1));
        assertEquals(MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());
        
        recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 350)
                                              .setTimestampInMillis(1, timestamp + 350)
                                              .setByte(2, 10)
                                              .build();

        this.partition.write(recordIterator, newFuture(0, 2000));
        assertEquals(MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());


        Range<Long> range = Range.closedOpen(Long.valueOf(timestamp), Long.valueOf(timestamp  + 2000));
        RecordIterator iterator = this.partition.read(range);

        assertTrue(iterator.hasNext());
        Record actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp, actual.getTimestampInMillis(0));
        assertEquals(timestamp, actual.getTimestampInMillis(1));
        assertEquals(10, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100L, actual.getTimestampInMillis(0));
        assertEquals(100L, actual.getTimestampInMillis(1));
        assertEquals(-5, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(250, actual.getTimestampInMillis(0));
        assertEquals(250, actual.getTimestampInMillis(1));
        assertEquals(5, actual.getByte(2));

        assertFalse(iterator.hasNext());

        EasyMock.verify(this.manager, this.listener);
    }
    
    @Test
    public void testWriteOnTwoMemSeries() throws IOException, HorizonDBException {

        this.listener.memoryUsageChanged(this.partition, 0, MEMTIMESERIES_SIZE);
        this.listener.firstSegmentContainingNonPersistedDataChanged(this.partition, null, Long.valueOf(0));
        this.listener.memoryUsageChanged(this.partition, MEMTIMESERIES_SIZE, 2 * MEMTIMESERIES_SIZE);

        this.manager.flush(this.partition);

        EasyMock.replay(this.manager, this.listener);

        Range<Long> range = newTimeRange("2013.11.26 12:00:00.000", "2013.11.26 14:00:00.000");

        long timestamp = getTime("2013.11.26 12:32:12.000");

        RecordIterator recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp)
                                                             .setTimestampInMillis(1, timestamp)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 100)
                                                             .setTimestampInMillis(1, timestamp + 100)
                                                             .setByte(2, 5)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 350)
                                                             .setTimestampInMillis(1, timestamp + 350)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 450)
                                                             .setTimestampInMillis(1, timestamp + 450)
                                                             .setByte(2, 6)
                                                             .build();

        this.partition.write(recordIterator, newFuture(0, 1));
        assertEquals(MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());
        assertEquals(Long.valueOf(0), this.partition.getFirstSegmentContainingNonPersistedData());


        recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 600)
                                              .setTimestampInMillis(1, timestamp + 600)
                                              .setByte(2, 6)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 700)
                                              .setTimestampInMillis(1, timestamp + 700)
                                              .setByte(2, 5)
                                              .build();

        this.partition.write(recordIterator, newFuture(0, 2));
        assertEquals(2 * MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());
        assertEquals(Long.valueOf(0), this.partition.getFirstSegmentContainingNonPersistedData());

        RecordIterator iterator = this.partition.read(range);

        assertTrue(iterator.hasNext());
        Record actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp, actual.getTimestampInMillis(0));
        assertEquals(timestamp, actual.getTimestampInMillis(1));
        assertEquals(10, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100L, actual.getTimestampInMillis(0));
        assertEquals(100L, actual.getTimestampInMillis(1));
        assertEquals(-5, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(250, actual.getTimestampInMillis(0));
        assertEquals(250, actual.getTimestampInMillis(1));
        assertEquals(5, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100, actual.getTimestampInMillis(0));
        assertEquals(100, actual.getTimestampInMillis(1));
        assertEquals(-4, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp + 600, actual.getTimestampInMillis(0));
        assertEquals(timestamp + 600, actual.getTimestampInMillis(1));
        assertEquals(6, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100, actual.getTimestampInMillis(0));
        assertEquals(100, actual.getTimestampInMillis(1));
        assertEquals(-1, actual.getByte(2));

        assertFalse(iterator.hasNext());

        EasyMock.verify(this.manager, this.listener);
    }

    @Test
    public void testFlush() throws IOException, HorizonDBException, InterruptedException {

        this.listener.memoryUsageChanged(this.partition, 0, MEMTIMESERIES_SIZE);
        this.listener.firstSegmentContainingNonPersistedDataChanged(this.partition, null, Long.valueOf(0));

        this.manager.flush(this.partition);

        this.listener.memoryUsageChanged(this.partition, MEMTIMESERIES_SIZE, 3 * MEMTIMESERIES_SIZE);

        this.manager.flush(this.partition);

        this.listener.memoryUsageChanged(this.partition, 3 * MEMTIMESERIES_SIZE, MEMTIMESERIES_SIZE);
        this.listener.firstSegmentContainingNonPersistedDataChanged(this.partition, Long.valueOf(0), Long.valueOf(1));

        this.manager.save(this.partition);

        EasyMock.replay(this.manager, this.listener);

        Range<Long> range = newTimeRange("2013.11.26 12:00:00.000", "2013.11.26 14:00:00.000");

        long timestamp = getTime("2013.11.26 12:32:12.000");

        RecordIterator recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp)
                                                             .setTimestampInMillis(1, timestamp)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 100)
                                                             .setTimestampInMillis(1, timestamp + 100)
                                                             .setByte(2, 5)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 350)
                                                             .setTimestampInMillis(1, timestamp + 350)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 450)
                                                             .setTimestampInMillis(1, timestamp + 450)
                                                             .setByte(2, 6)
                                                             .build();

        this.partition.write(recordIterator, newFuture(0, 1));
        assertEquals(MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());
        assertEquals(Long.valueOf(0), this.partition.getFirstSegmentContainingNonPersistedData());

        recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 600)
                                              .setTimestampInMillis(1, timestamp + 600)
                                              .setByte(2, 6)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 700)
                                              .setTimestampInMillis(1, timestamp + 700)
                                              .setByte(2, 5)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 1000)
                                              .setTimestampInMillis(1, timestamp + 1000)
                                              .setByte(2, 6)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 1200)
                                              .setTimestampInMillis(1, timestamp + 1200)
                                              .setByte(2, 5)
                                              .build();

        this.partition.write(recordIterator, newFuture(0, 2));
        assertEquals(3 * MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());
        assertEquals(Long.valueOf(0), this.partition.getFirstSegmentContainingNonPersistedData());

        recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 1400)
                                              .setTimestampInMillis(1, timestamp + 1400)
                                              .setByte(2, 6)
                                              .build();

        this.partition.write(recordIterator, newFuture(1, 1));
        assertEquals(3 * MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());
        assertEquals(Long.valueOf(0), this.partition.getFirstSegmentContainingNonPersistedData());

        this.partition.flush();
        assertEquals(Long.valueOf(1), this.partition.getFirstSegmentContainingNonPersistedData());

        RecordIterator iterator = this.partition.read(range);

        assertTrue(iterator.hasNext());
        Record actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp, actual.getTimestampInMillis(0));
        assertEquals(timestamp, actual.getTimestampInMillis(1));
        assertEquals(10, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100L, actual.getTimestampInMillis(0));
        assertEquals(100L, actual.getTimestampInMillis(1));
        assertEquals(-5, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(250, actual.getTimestampInMillis(0));
        assertEquals(250, actual.getTimestampInMillis(1));
        assertEquals(5, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100, actual.getTimestampInMillis(0));
        assertEquals(100, actual.getTimestampInMillis(1));
        assertEquals(-4, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp + 600, actual.getTimestampInMillis(0));
        assertEquals(timestamp + 600, actual.getTimestampInMillis(1));
        assertEquals(6, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100, actual.getTimestampInMillis(0));
        assertEquals(100, actual.getTimestampInMillis(1));
        assertEquals(-1, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(300, actual.getTimestampInMillis(0));
        assertEquals(300, actual.getTimestampInMillis(1));
        assertEquals(1, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(200, actual.getTimestampInMillis(0));
        assertEquals(200, actual.getTimestampInMillis(1));
        assertEquals(-1, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp + 1400, actual.getTimestampInMillis(0));
        assertEquals(timestamp + 1400, actual.getTimestampInMillis(1));
        assertEquals(6, actual.getByte(2));

        assertFalse(iterator.hasNext());

        EasyMock.verify(this.manager, this.listener);
    }

    @Test
    public void testFlushWithOneMemTimeSeriesFull() throws IOException, HorizonDBException, InterruptedException {

        this.listener.memoryUsageChanged(this.partition, 0, MEMTIMESERIES_SIZE);
        this.listener.firstSegmentContainingNonPersistedDataChanged(this.partition, null, Long.valueOf(0));

        this.manager.flush(this.partition);

        this.listener.memoryUsageChanged(this.partition, MEMTIMESERIES_SIZE, 0);
        this.listener.firstSegmentContainingNonPersistedDataChanged(this.partition, Long.valueOf(0), null);

        this.manager.save(this.partition);

        EasyMock.replay(this.manager, this.listener);

        Range<Long> range = newTimeRange("2013.11.26 12:00:00.000", "2013.11.26 14:00:00.000");

        long timestamp = getTime("2013.11.26 12:32:12.000");

        RecordIterator recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp)
                                                             .setTimestampInMillis(1, timestamp)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 100)
                                                             .setTimestampInMillis(1, timestamp + 100)
                                                             .setByte(2, 5)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 350)
                                                             .setTimestampInMillis(1, timestamp + 350)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 450)
                                                             .setTimestampInMillis(1, timestamp + 450)
                                                             .setByte(2, 6)
                                                             .build();

        this.partition.write(recordIterator, newFuture(0, 1));
        assertEquals(MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());

        this.partition.flush();

        RecordIterator iterator = this.partition.read(range);

        assertTrue(iterator.hasNext());
        Record actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp, actual.getTimestampInMillis(0));
        assertEquals(timestamp, actual.getTimestampInMillis(1));
        assertEquals(10, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100L, actual.getTimestampInMillis(0));
        assertEquals(100L, actual.getTimestampInMillis(1));
        assertEquals(-5, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(250, actual.getTimestampInMillis(0));
        assertEquals(250, actual.getTimestampInMillis(1));
        assertEquals(5, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100, actual.getTimestampInMillis(0));
        assertEquals(100, actual.getTimestampInMillis(1));
        assertEquals(-4, actual.getByte(2));

        assertFalse(iterator.hasNext());

        EasyMock.verify(this.manager, this.listener);
    }

    @Test
    public void testWriteOnThreeMemSeries() throws IOException, HorizonDBException {

        this.listener.memoryUsageChanged(this.partition, 0, MEMTIMESERIES_SIZE);
        this.listener.firstSegmentContainingNonPersistedDataChanged(this.partition, null, Long.valueOf(0));
        this.listener.memoryUsageChanged(this.partition, MEMTIMESERIES_SIZE, 3 * MEMTIMESERIES_SIZE);

        this.manager.flush(this.partition);
        this.manager.flush(this.partition);

        EasyMock.replay(this.manager, this.listener);

        Range<Long> range = newTimeRange("2013.11.26 12:00:00.000", "2013.11.26 14:00:00.000");

        long timestamp = getTime("2013.11.26 12:32:12.000");

        RecordIterator recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp)
                                                             .setTimestampInMillis(1, timestamp)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 100)
                                                             .setTimestampInMillis(1, timestamp + 100)
                                                             .setByte(2, 5)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 350)
                                                             .setTimestampInMillis(1, timestamp + 350)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 450)
                                                             .setTimestampInMillis(1, timestamp + 450)
                                                             .setByte(2, 6)
                                                             .build();

        this.partition.write(recordIterator, newFuture(0, 1));
        assertEquals(MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());

        recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 600)
                                              .setTimestampInMillis(1, timestamp + 600)
                                              .setByte(2, 6)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 700)
                                              .setTimestampInMillis(1, timestamp + 700)
                                              .setByte(2, 5)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 1000)
                                              .setTimestampInMillis(1, timestamp + 1000)
                                              .setByte(2, 6)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 1200)
                                              .setTimestampInMillis(1, timestamp + 1200)
                                              .setByte(2, 5)
                                              .build();

        this.partition.write(recordIterator, newFuture(0, 2));
        assertEquals(3 * MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());

        recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 1400)
                                              .setTimestampInMillis(1, timestamp + 1400)
                                              .setByte(2, 6)
                                              .build();

        this.partition.write(recordIterator, newFuture(0, 3));
        assertEquals(3 * MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());

        RecordIterator iterator = this.partition.read(range);

        assertTrue(iterator.hasNext());
        Record actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp, actual.getTimestampInMillis(0));
        assertEquals(timestamp, actual.getTimestampInMillis(1));
        assertEquals(10, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100L, actual.getTimestampInMillis(0));
        assertEquals(100L, actual.getTimestampInMillis(1));
        assertEquals(-5, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(250, actual.getTimestampInMillis(0));
        assertEquals(250, actual.getTimestampInMillis(1));
        assertEquals(5, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100, actual.getTimestampInMillis(0));
        assertEquals(100, actual.getTimestampInMillis(1));
        assertEquals(-4, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp + 600, actual.getTimestampInMillis(0));
        assertEquals(timestamp + 600, actual.getTimestampInMillis(1));
        assertEquals(6, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100, actual.getTimestampInMillis(0));
        assertEquals(100, actual.getTimestampInMillis(1));
        assertEquals(-1, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(300, actual.getTimestampInMillis(0));
        assertEquals(300, actual.getTimestampInMillis(1));
        assertEquals(1, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(200, actual.getTimestampInMillis(0));
        assertEquals(200, actual.getTimestampInMillis(1));
        assertEquals(-1, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp + 1400, actual.getTimestampInMillis(0));
        assertEquals(timestamp + 1400, actual.getTimestampInMillis(1));
        assertEquals(6, actual.getByte(2));

        assertFalse(iterator.hasNext());

        EasyMock.verify(this.manager, this.listener);
    }

    @Test
    public void testForceFlush() throws IOException, HorizonDBException, InterruptedException {

        this.listener.memoryUsageChanged(this.partition, 0, MEMTIMESERIES_SIZE);
        this.listener.firstSegmentContainingNonPersistedDataChanged(this.partition, null, Long.valueOf(0));

        this.manager.save(this.partition);

        this.listener.memoryUsageChanged(this.partition, MEMTIMESERIES_SIZE, 0);
        this.listener.firstSegmentContainingNonPersistedDataChanged(this.partition, Long.valueOf(0), null);

        EasyMock.replay(this.manager, this.listener);

        Range<Long> range = newTimeRange("2013.11.26 12:00:00.000", "2013.11.26 14:00:00.000");

        long timestamp = getTime("2013.11.26 12:32:12.000");

        RecordIterator recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp)
                                                             .setTimestampInMillis(1, timestamp)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 100)
                                                             .setTimestampInMillis(1, timestamp + 100)
                                                             .setByte(2, 5)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 350)
                                                             .setTimestampInMillis(1, timestamp + 350)
                                                             .setByte(2, 10)
                                                             .build();

        this.partition.write(recordIterator, newFuture(0, 1));

        assertEquals(MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());
        assertEquals(Long.valueOf(0), this.partition.getFirstSegmentContainingNonPersistedData());
        
        this.partition.forceFlush();

        assertEquals(0, this.partition.getMemoryUsage());
        assertEquals(null, this.partition.getFirstSegmentContainingNonPersistedData());

        RecordIterator iterator = this.partition.read(range);

        assertTrue(iterator.hasNext());
        Record actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp, actual.getTimestampInMillis(0));
        assertEquals(timestamp, actual.getTimestampInMillis(1));
        assertEquals(10, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100L, actual.getTimestampInMillis(0));
        assertEquals(100L, actual.getTimestampInMillis(1));
        assertEquals(-5, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(250, actual.getTimestampInMillis(0));
        assertEquals(250, actual.getTimestampInMillis(1));
        assertEquals(5, actual.getByte(2));

        assertFalse(iterator.hasNext());

        EasyMock.verify(this.manager, this.listener);
    }

    @Test
    public void testWriteAfterForceFlush() throws IOException, HorizonDBException, InterruptedException {

        this.listener.memoryUsageChanged(this.partition, 0, MEMTIMESERIES_SIZE);
        this.listener.firstSegmentContainingNonPersistedDataChanged(this.partition, null, Long.valueOf(0));
        this.listener.memoryUsageChanged(this.partition, MEMTIMESERIES_SIZE, 0);
        this.listener.firstSegmentContainingNonPersistedDataChanged(this.partition, Long.valueOf(0), null);
        this.manager.save(this.partition);
        this.listener.memoryUsageChanged(this.partition, 0, MEMTIMESERIES_SIZE);
        this.listener.firstSegmentContainingNonPersistedDataChanged(this.partition, null, Long.valueOf(0));

        EasyMock.replay(this.manager, this.listener);

        Range<Long> range = newTimeRange("2013.11.26 12:00:00.000", "2013.11.26 14:00:00.000");

        long timestamp = getTime("2013.11.26 12:32:12.000");

        RecordIterator recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp)
                                                             .setTimestampInMillis(1, timestamp)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 100)
                                                             .setTimestampInMillis(1, timestamp + 100)
                                                             .setByte(2, 5)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 350)
                                                             .setTimestampInMillis(1, timestamp + 350)
                                                             .setByte(2, 10)
                                                             .build();

        this.partition.write(recordIterator, newFuture(0, 1));
        assertEquals(MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());
        assertEquals(Long.valueOf(0), this.partition.getFirstSegmentContainingNonPersistedData());

        this.partition.forceFlush();
        assertEquals(0, this.partition.getMemoryUsage());
        assertEquals(null, this.partition.getFirstSegmentContainingNonPersistedData());

        recordIterator = DefaultRecordIterator.newBuilder(this.def)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 400)
                                              .setTimestampInMillis(1, timestamp + 400)
                                              .setByte(2, 0)
                                              .newRecord("exchangeState")
                                              .setTimestampInMillis(0, timestamp + 1200)
                                              .setTimestampInMillis(1, timestamp + 1200)
                                              .setByte(2, 0)
                                              .build();

        this.partition.write(recordIterator, newFuture(0, 2));
        assertEquals(MEMTIMESERIES_SIZE, this.partition.getMemoryUsage());
        assertEquals(Long.valueOf(0), this.partition.getFirstSegmentContainingNonPersistedData());

        RecordIterator iterator = this.partition.read(range);

        assertTrue(iterator.hasNext());
        Record actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp, actual.getTimestampInMillis(0));
        assertEquals(timestamp, actual.getTimestampInMillis(1));
        assertEquals(10, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(100L, actual.getTimestampInMillis(0));
        assertEquals(100L, actual.getTimestampInMillis(1));
        assertEquals(-5, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(250, actual.getTimestampInMillis(0));
        assertEquals(250, actual.getTimestampInMillis(1));
        assertEquals(5, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertFalse(actual.isDelta());
        assertEquals(timestamp + 400, actual.getTimestampInMillis(0));
        assertEquals(timestamp + 400, actual.getTimestampInMillis(1));
        assertEquals(0, actual.getByte(2));

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        assertTrue(actual.isDelta());
        assertEquals(800, actual.getTimestampInMillis(0));
        assertEquals(800, actual.getTimestampInMillis(1));
        assertEquals(0, actual.getByte(2));

        assertFalse(iterator.hasNext());

        EasyMock.verify(this.manager, this.listener);
    }

    private static ListenableFuture<ReplayPosition> newFuture(long segment, long position) {

        return Futures.immediateCheckedFuture(new ReplayPosition(segment, position));
    }
    
    private static Range<Long> newTimeRange(String start, String end) {
        
        return Range.closedOpen(Long.valueOf(getTime(start)), 
                                Long.valueOf(getTime(end)));
        
    }
}
