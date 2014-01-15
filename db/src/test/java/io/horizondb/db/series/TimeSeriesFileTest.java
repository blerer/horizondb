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
import io.horizondb.model.DatabaseDefinition;
import io.horizondb.model.FieldType;
import io.horizondb.model.RecordTypeDefinition;
import io.horizondb.model.TimeRange;
import io.horizondb.model.TimeSeriesDefinition;
import io.horizondb.model.TimeSeriesRecordIterator;
import io.horizondb.test.AssertFiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.Futures;

import static io.horizondb.db.utils.TimeUtils.getTime;

import static java.util.Arrays.asList;

public class TimeSeriesFileTest {

    /**
     * The test directory.
     */
    private Path testDirectory;

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

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        this.definition = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                            .timeUnit(TimeUnit.NANOSECONDS)
                                            .addRecordType(recordTypeDefinition)
                                            .build();

        TimeRange range = new TimeRange(getTime("2013.11.26 00:00:00.000"), getTime("2013.11.26 23:59:59.999"));

        this.metadata = TimeSeriesPartitionMetaData.newBuilder(range).build();
    }

    @After
    public void tearDown() throws Exception {

        this.definition = null;
        FileUtils.forceDelete(this.testDirectory);
        this.testDirectory = null;
    }

    @Test
    public void testAppend() throws IOException, HorizonDBException, InterruptedException {

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(this.definition)
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

        FileMetaData fileMetaData = new FileMetaData(this.definition.getDatabaseName(),
                                                     this.definition.getSeriesName(),
                                                     this.metadata.getRange());

        Buffer buffer = Buffers.allocate(fileMetaData.computeSerializedSize() + iterator.computeSerializedSize());
        buffer.writeObject(fileMetaData).writeObject(iterator);

        memTimeSeries = memTimeSeries.write(allocator, iterator, Futures.immediateFuture(new ReplayPosition(1, 0)));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, this.definition, this.metadata)) {

            file.append(asList((TimeSeriesElement) memTimeSeries));

            AssertFiles.assertFileContains(buffer.array(), file.getPath());
        }
    }

    @Test
    public void testDuplicateAppend() throws IOException, HorizonDBException, InterruptedException {

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(this.definition)
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

        FileMetaData fileMetaData = new FileMetaData(this.definition.getDatabaseName(),
                                                     this.definition.getSeriesName(),
                                                     this.metadata.getRange());

        Buffer buffer = Buffers.allocate(fileMetaData.computeSerializedSize() + iterator.computeSerializedSize());
        buffer.writeObject(fileMetaData).writeObject(iterator);

        memTimeSeries = memTimeSeries.write(allocator, iterator, Futures.immediateFuture(new ReplayPosition(1, 0)));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, this.definition, this.metadata)) {

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

        TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(this.definition)
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

        TimeSeriesRecordIterator iterator2 = TimeSeriesRecordIterator.newBuilder(this.definition)
                                                                     .newRecord("exchangeState")
                                                                     .setTimestampInNanos(0, 13014400)
                                                                     .setTimestampInMillis(1, 13)
                                                                     .setByte(2, 2)
                                                                     .newRecord("exchangeState")
                                                                     .setTimestampInNanos(0, 14000900)
                                                                     .setTimestampInMillis(1, 14)
                                                                     .setByte(2, 3)
                                                                     .build();

        FileMetaData fileMetaData = new FileMetaData(this.definition.getDatabaseName(),
                                                     this.definition.getSeriesName(),
                                                     this.metadata.getRange());

        Buffer buffer = Buffers.allocate(fileMetaData.computeSerializedSize() + iterator.computeSerializedSize()
                + iterator2.computeSerializedSize());

        buffer.writeObject(fileMetaData).writeObject(iterator).writeObject(iterator2);

        ReplayPosition replayPosition = new ReplayPosition(1, iterator.computeSerializedSize());

        memTimeSeries = memTimeSeries.write(allocator, iterator, Futures.immediateFuture(replayPosition));

        replayPosition = new ReplayPosition(1, iterator.computeSerializedSize() + iterator2.computeSerializedSize());

        memTimeSeries2 = memTimeSeries2.write(allocator, iterator2, Futures.immediateFuture(replayPosition));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, this.definition, this.metadata)) {

            file.append(Arrays.<TimeSeriesElement> asList(memTimeSeries, memTimeSeries2));

            AssertFiles.assertFileContains(buffer.array(), file.getPath());
        }
    }

    @Test
    public void testAppendWithTwoSeparateCalls() throws IOException, HorizonDBException, InterruptedException {

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        MemTimeSeries memTimeSeries2 = new MemTimeSeries(this.configuration, this.definition);

        TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(this.definition)
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

        TimeSeriesRecordIterator iterator2 = TimeSeriesRecordIterator.newBuilder(this.definition)
                                                                     .newRecord("exchangeState")
                                                                     .setTimestampInNanos(0, 13014400)
                                                                     .setTimestampInMillis(1, 13)
                                                                     .setByte(2, 2)
                                                                     .newRecord("exchangeState")
                                                                     .setTimestampInNanos(0, 14000900)
                                                                     .setTimestampInMillis(1, 14)
                                                                     .setByte(2, 3)
                                                                     .build();

        FileMetaData fileMetaData = new FileMetaData(this.definition.getDatabaseName(),
                                                     this.definition.getSeriesName(),
                                                     this.metadata.getRange());

        Buffer buffer = Buffers.allocate(fileMetaData.computeSerializedSize() + iterator.computeSerializedSize()
                + iterator2.computeSerializedSize());

        buffer.writeObject(fileMetaData).writeObject(iterator).writeObject(iterator2);

        ReplayPosition replayPosition = new ReplayPosition(1, iterator.computeSerializedSize());

        memTimeSeries = memTimeSeries.write(allocator, iterator, Futures.immediateFuture(replayPosition));

        replayPosition = new ReplayPosition(1, iterator.computeSerializedSize() + iterator2.computeSerializedSize());
        memTimeSeries2 = memTimeSeries2.write(allocator, iterator2, Futures.immediateFuture(replayPosition));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, this.definition, this.metadata)) {

            file.append(Arrays.<TimeSeriesElement> asList(memTimeSeries))
                .append(Arrays.<TimeSeriesElement> asList(memTimeSeries2));

            AssertFiles.assertFileContains(buffer.array(), file.getPath());
        }
    }

    @Test
    public void testNewInput() throws IOException, HorizonDBException, InterruptedException {

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        TimeSeriesRecordIterator iterator = TimeSeriesRecordIterator.newBuilder(this.definition)
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

        Buffer buffer = Buffers.allocate(iterator.computeSerializedSize());
        buffer.writeObject(iterator);

        memTimeSeries = memTimeSeries.write(allocator, iterator, Futures.immediateFuture(new ReplayPosition(1, 0)));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, this.definition, this.metadata)) {

            try (SeekableFileDataInput input = file.append(Arrays.<TimeSeriesElement> asList(memTimeSeries)).newInput()) {

                ReadableBuffer content = input.slice((int) input.size());
                Assert.assertEquals(buffer, content);
            }
        }
    }

    @Test
    public void testNewInputWithEmptyFile() throws IOException {

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, this.definition, this.metadata)) {

            try (SeekableFileDataInput input = file.newInput()) {

                Assert.assertFalse(input.isReadable());
            }
        }
    }
}