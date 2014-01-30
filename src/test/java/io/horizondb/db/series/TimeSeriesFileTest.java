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
import io.horizondb.io.Buffer;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.files.FileUtils;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.model.TimeRange;
import io.horizondb.model.core.RecordListMultimapBuilder;
import io.horizondb.model.core.RecordUtils;
import io.horizondb.model.core.iterators.DefaultRecordIterator;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;
import io.horizondb.test.AssertFiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.Futures;

import static io.horizondb.db.util.TimeUtils.getTime;

import static java.util.Arrays.asList;

public class TimeSeriesFileTest {

    /**
     * The time reference.
     */
    private static long TIME_IN_MILLIS = getTime("2013.11.26 12:00:00.000");
    
    /**
     * The time reference.
     */
    private static long TIME_IN_NANOS = TimeUnit.MILLISECONDS.toNanos(TIME_IN_MILLIS);
    
    /**
     * The test directory.
     */
    private Path testDirectory;

    private DatabaseDefinition databaseDefinition;
    
    private TimeSeriesDefinition definition;

    private Configuration configuration;

    private TimeSeriesPartitionMetaData metadata;

    @Before
    public void setUp() throws Exception {

        this.testDirectory = Files.createTempDirectory(this.getClass().getSimpleName());

        this.configuration = Configuration.newBuilder().dataDirectory(this.testDirectory).build();

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        Files.createDirectory(this.testDirectory.resolve("test"));

        this.databaseDefinition = new DatabaseDefinition("test");

        this.definition = this.databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                            .timeUnit(TimeUnit.NANOSECONDS)
                                            .addRecordType(recordTypeDefinition)
                                            .build();

        TimeRange range = new TimeRange(getTime("2013.11.26 00:00:00.000"), getTime("2013.11.26 23:59:59.999"));

        this.metadata = TimeSeriesPartitionMetaData.newBuilder(range).build();
    }

    @After
    public void tearDown() throws Exception {

        this.definition = null;
        this.databaseDefinition = null;
        FileUtils.forceDelete(this.testDirectory);
        this.testDirectory = null;
    }

    @Test
    public void testAppend() throws IOException, HorizonDBException, InterruptedException {

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        Collection<TimeSeriesRecord> records = new RecordListMultimapBuilder(this.definition)
                                                                    .newRecord("exchangeState")
                                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 12000700)
                                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                                    .setByte(2, 3)
                                                                    .newRecord("exchangeState")
                                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                    .setByte(2, 3)
                                                                    .newRecord("exchangeState")
                                                                    .setTimestampInNanos(0,  TIME_IN_NANOS + 13004400)
                                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                    .setByte(2, 1)
                                                                    .build();

        FileMetaData fileMetaData = new FileMetaData(this.databaseDefinition.getName(),
                                                     this.definition.getName(),
                                                     this.metadata.getRange());

        Buffer buffer = Buffers.allocate(fileMetaData.computeSerializedSize()
                + RecordUtils.computeSerializedSize(records));
        
        RecordUtils.writeRecords(buffer.writeObject(fileMetaData), records);

        DefaultRecordIterator iterator = new DefaultRecordIterator(records);
        
        memTimeSeries = memTimeSeries.write(allocator, iterator, Futures.immediateFuture(new ReplayPosition(1, 0)));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition.getName(), 
                                                       this.definition, 
                                                       this.metadata)) {

            file.append(asList((TimeSeriesElement) memTimeSeries));

            AssertFiles.assertFileContains(buffer.array(), file.getPath());
        }
    }

    @Test
    public void testDuplicateAppend() throws IOException, HorizonDBException, InterruptedException {

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        Collection<TimeSeriesRecord> records = new RecordListMultimapBuilder(this.definition).newRecord("exchangeState")
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

        FileMetaData fileMetaData = new FileMetaData(this.databaseDefinition.getName(),
                                                     this.definition.getName(),
                                                     this.metadata.getRange());

        Buffer buffer = Buffers.allocate(fileMetaData.computeSerializedSize() + RecordUtils.computeSerializedSize(records));
        RecordUtils.writeRecords(buffer.writeObject(fileMetaData), records);
        
        DefaultRecordIterator iterator = new DefaultRecordIterator(records);
        
        memTimeSeries = memTimeSeries.write(allocator, iterator, Futures.immediateFuture(new ReplayPosition(1, 0)));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition.getName(), 
                                                       this.definition, 
                                                       this.metadata)) {

            file.append(asList((TimeSeriesElement) memTimeSeries));
            file.append(asList((TimeSeriesElement) memTimeSeries));

            AssertFiles.assertFileContains(buffer.array(), file.getPath());
        }
    }

    @Test
    public void testAppendWithTwoTimeSeriesInTheSameCall() throws IOException, HorizonDBException, InterruptedException {

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        MemTimeSeries memTimeSeries2 = new MemTimeSeries(this.configuration, this.definition);

        Collection<TimeSeriesRecord> records = new RecordListMultimapBuilder(this.definition).newRecord("exchangeState")
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

        Collection<TimeSeriesRecord> records2 = new RecordListMultimapBuilder(this.definition).newRecord("exchangeState")
                                                                                .setTimestampInNanos(0, TIME_IN_NANOS + 13014400)
                                                                                .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                                .setByte(2, 2)
                                                                                .newRecord("exchangeState")
                                                                                .setTimestampInNanos(0, TIME_IN_NANOS + 14000900)
                                                                                .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                                                .setByte(2, 3)
                                                                                .build();

        FileMetaData fileMetaData = new FileMetaData(this.databaseDefinition.getName(),
                                                     this.definition.getName(),
                                                     this.metadata.getRange());

        Buffer buffer = Buffers.allocate(fileMetaData.computeSerializedSize()
                + RecordUtils.computeSerializedSize(records) + RecordUtils.computeSerializedSize(records2));

        buffer.writeObject(fileMetaData);
        RecordUtils.writeRecords(buffer, records);
        RecordUtils.writeRecords(buffer, records2);

        ReplayPosition replayPosition = new ReplayPosition(1, RecordUtils.computeSerializedSize(records));

        DefaultRecordIterator iterator = new DefaultRecordIterator(records);
        
        memTimeSeries = memTimeSeries.write(allocator, iterator, Futures.immediateFuture(replayPosition));

        replayPosition = new ReplayPosition(1, RecordUtils.computeSerializedSize(records) + RecordUtils.computeSerializedSize(records2));

        DefaultRecordIterator iterator2 = new DefaultRecordIterator(records2);
        
        memTimeSeries2 = memTimeSeries2.write(allocator, iterator2, Futures.immediateFuture(replayPosition));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition.getName(), 
                                                       this.definition, 
                                                       this.metadata)) {

            file.append(Arrays.<TimeSeriesElement> asList(memTimeSeries, memTimeSeries2));

            AssertFiles.assertFileContains(buffer.array(), file.getPath());
        }
    }

    @Test
    public void testAppendWithTwoSeparateCalls() throws IOException, HorizonDBException, InterruptedException {

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        MemTimeSeries memTimeSeries2 = new MemTimeSeries(this.configuration, this.definition);

        Collection<TimeSeriesRecord> records = new RecordListMultimapBuilder(this.definition).newRecord("exchangeState")
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

        Collection<TimeSeriesRecord> records2 = new RecordListMultimapBuilder(this.definition).newRecord("exchangeState")
                                                                               .setTimestampInNanos(0, TIME_IN_NANOS + 13014400)
                                                                               .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                               .setByte(2, 2)
                                                                               .newRecord("exchangeState")
                                                                               .setTimestampInNanos(0, TIME_IN_NANOS + 14000900)
                                                                               .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                                               .setByte(2, 3)
                                                                               .build();

        FileMetaData fileMetaData = new FileMetaData(this.databaseDefinition.getName(),
                                                     this.definition.getName(),
                                                     this.metadata.getRange());

        Buffer buffer = Buffers.allocate(fileMetaData.computeSerializedSize() 
                                         + RecordUtils.computeSerializedSize(records)
                                         + RecordUtils.computeSerializedSize(records2));

        buffer.writeObject(fileMetaData);
        RecordUtils.writeRecords(buffer, records);
        RecordUtils.writeRecords(buffer, records2);

        ReplayPosition replayPosition = new ReplayPosition(1, RecordUtils.computeSerializedSize(records));

        DefaultRecordIterator iterator = new DefaultRecordIterator(records);
        
        memTimeSeries = memTimeSeries.write(allocator, iterator, Futures.immediateFuture(replayPosition));

        replayPosition = new ReplayPosition(1, RecordUtils.computeSerializedSize(records)
                                            + RecordUtils.computeSerializedSize(records2));
        
        DefaultRecordIterator iterator2 = new DefaultRecordIterator(records2);
        
        memTimeSeries2 = memTimeSeries2.write(allocator, iterator2, Futures.immediateFuture(replayPosition));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition.getName(), 
                                                       this.definition, 
                                                       this.metadata)) {

            file.append(Arrays.<TimeSeriesElement> asList(memTimeSeries))
                .append(Arrays.<TimeSeriesElement> asList(memTimeSeries2));

            AssertFiles.assertFileContains(buffer.array(), file.getPath());
        }
    }

    @Test
    public void testNewInput() throws IOException, HorizonDBException, InterruptedException {

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        Collection<TimeSeriesRecord> records = new RecordListMultimapBuilder(this.definition).newRecord("exchangeState")
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

        Buffer buffer = Buffers.allocate(RecordUtils.computeSerializedSize(records));
        RecordUtils.writeRecords(buffer, records);

        DefaultRecordIterator iterator = new DefaultRecordIterator(records);
        
        memTimeSeries = memTimeSeries.write(allocator, iterator, Futures.immediateFuture(new ReplayPosition(1, 0)));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition.getName(),
                                                       this.definition, 
                                                       this.metadata)) {

            try (SeekableFileDataInput input = file.append(Arrays.<TimeSeriesElement> asList(memTimeSeries)).newInput()) {

                ReadableBuffer content = input.slice((int) input.size());
                Assert.assertEquals(buffer, content);
            }
        }
    }

    @Test
    public void testNewInputWithEmptyFile() throws IOException {

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition.getName(), 
                                                       this.definition, 
                                                       this.metadata)) {

            try (SeekableFileDataInput input = file.newInput()) {

                Assert.assertFalse(input.isReadable());
            }
        }
    }
}
