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
import io.horizondb.model.core.Field;
import io.horizondb.model.core.RecordListBuilder;
import io.horizondb.model.core.RecordUtils;
import io.horizondb.model.core.records.BlockHeaderUtils;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;
import io.horizondb.test.AssertFiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.Futures;

import static java.util.Arrays.asList;

public class TimeSeriesFileTest {

    /**
     * The time reference.
     */
    private static long TIME_IN_MILLIS = TimeUtils.parseDateTime("2013-11-26 12:00:00.000");
    
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

        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        Range<Field> range = prototype.range(TimeZone.getDefault(), 
                                             "'2013-11-26 00:00:00.000'", 
                                             "'2013-11-27 00:00:00.000'");

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

        List<TimeSeriesRecord> records = new RecordListBuilder(this.definition)
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

        int blockSize = RecordUtils.computeSerializedSize(records);
        
        TimeSeriesRecord blockHeader = this.definition.newBlockHeader();
        BlockHeaderUtils.setFirstTimestampInNanos(blockHeader, TIME_IN_NANOS + 12000700);
        BlockHeaderUtils.setLastTimestampInNanos(blockHeader, TIME_IN_NANOS + 13004400);
        BlockHeaderUtils.setBlockSize(blockHeader, blockSize);
        BlockHeaderUtils.setRecordCount(blockHeader, 0, 3);
                
        Buffer buffer = Buffers.allocate(fileMetaData.computeSerializedSize() 
                                         + RecordUtils.computeSerializedSize(blockHeader) 
                                         + blockSize);
        
        buffer.writeObject(fileMetaData);
        RecordUtils.writeRecord(buffer, blockHeader);      
        RecordUtils.writeRecords(buffer, records);
        
        memTimeSeries = memTimeSeries.write(allocator, records, Futures.immediateFuture(new ReplayPosition(1, 0)));

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

        List<TimeSeriesRecord> records = new RecordListBuilder(this.definition).newRecord("exchangeState")
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

        int blockSize = RecordUtils.computeSerializedSize(records);
        
        TimeSeriesRecord blockHeader = this.definition.newBlockHeader();
        BlockHeaderUtils.setFirstTimestampInNanos(blockHeader, TIME_IN_NANOS + 12000700);
        BlockHeaderUtils.setLastTimestampInNanos(blockHeader, TIME_IN_NANOS + 13004400);
        BlockHeaderUtils.setBlockSize(blockHeader, blockSize);
        BlockHeaderUtils.setRecordCount(blockHeader, 0, 3);
                
        Buffer buffer = Buffers.allocate(fileMetaData.computeSerializedSize() 
                                         + RecordUtils.computeSerializedSize(blockHeader) 
                                         + blockSize);
        
        buffer.writeObject(fileMetaData);
        RecordUtils.writeRecord(buffer, blockHeader);      
        RecordUtils.writeRecords(buffer, records);
        
        memTimeSeries = memTimeSeries.write(allocator, records, Futures.immediateFuture(new ReplayPosition(1, 0)));

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

        List<TimeSeriesRecord> records = new RecordListBuilder(this.definition).newRecord("exchangeState")
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

        List<TimeSeriesRecord> records2 = new RecordListBuilder(this.definition).newRecord("exchangeState")
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


        int blockSize = RecordUtils.computeSerializedSize(records);
        
        TimeSeriesRecord blockHeader = this.definition.newBlockHeader();
        BlockHeaderUtils.setFirstTimestampInNanos(blockHeader, TIME_IN_NANOS + 12000700);
        BlockHeaderUtils.setLastTimestampInNanos(blockHeader, TIME_IN_NANOS + 13004400);
        BlockHeaderUtils.setBlockSize(blockHeader, blockSize);
        BlockHeaderUtils.setRecordCount(blockHeader, 0, 3);
        
        int blockSize2 = RecordUtils.computeSerializedSize(records2);
        
        TimeSeriesRecord blockHeader2 = this.definition.newBlockHeader();
        BlockHeaderUtils.setFirstTimestampInNanos(blockHeader2, TIME_IN_NANOS + 13014400);
        BlockHeaderUtils.setLastTimestampInNanos(blockHeader2, TIME_IN_NANOS + 14000900);
        BlockHeaderUtils.setBlockSize(blockHeader2, blockSize2);
        BlockHeaderUtils.setRecordCount(blockHeader2, 0, 2);
                
        Buffer buffer = Buffers.allocate(fileMetaData.computeSerializedSize() 
                                         + RecordUtils.computeSerializedSize(blockHeader) 
                                         + blockSize
                                         + RecordUtils.computeSerializedSize(blockHeader2) 
                                         + blockSize2);
        
        buffer.writeObject(fileMetaData);
        RecordUtils.writeRecord(buffer, blockHeader);      
        RecordUtils.writeRecords(buffer, records);
        RecordUtils.writeRecord(buffer, blockHeader2);   
        RecordUtils.writeRecords(buffer, records2);

        ReplayPosition replayPosition = new ReplayPosition(1, RecordUtils.computeSerializedSize(records));
        
        memTimeSeries = memTimeSeries.write(allocator, records, Futures.immediateFuture(replayPosition));

        replayPosition = new ReplayPosition(1, RecordUtils.computeSerializedSize(records) + RecordUtils.computeSerializedSize(records2));

        memTimeSeries2 = memTimeSeries2.write(allocator, records2, Futures.immediateFuture(replayPosition));

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

        List<TimeSeriesRecord> records = new RecordListBuilder(this.definition).newRecord("exchangeState")
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

        List<TimeSeriesRecord> records2 = new RecordListBuilder(this.definition).newRecord("exchangeState")
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

        int blockSize = RecordUtils.computeSerializedSize(records);
        
        TimeSeriesRecord blockHeader = this.definition.newBlockHeader();
        BlockHeaderUtils.setFirstTimestampInNanos(blockHeader, TIME_IN_NANOS + 12000700);
        BlockHeaderUtils.setLastTimestampInNanos(blockHeader, TIME_IN_NANOS + 13004400);
        BlockHeaderUtils.setBlockSize(blockHeader, blockSize);
        BlockHeaderUtils.setRecordCount(blockHeader, 0, 3);
        
        int blockSize2 = RecordUtils.computeSerializedSize(records2);
        
        TimeSeriesRecord blockHeader2 = this.definition.newBlockHeader();
        BlockHeaderUtils.setFirstTimestampInNanos(blockHeader2, TIME_IN_NANOS + 13014400);
        BlockHeaderUtils.setLastTimestampInNanos(blockHeader2, TIME_IN_NANOS + 14000900);
        BlockHeaderUtils.setBlockSize(blockHeader2, blockSize2);
        BlockHeaderUtils.setRecordCount(blockHeader2, 0, 2);
                
        Buffer buffer = Buffers.allocate(fileMetaData.computeSerializedSize() 
                                         + RecordUtils.computeSerializedSize(blockHeader) 
                                         + blockSize
                                         + RecordUtils.computeSerializedSize(blockHeader2) 
                                         + blockSize2);
        
        buffer.writeObject(fileMetaData);
        RecordUtils.writeRecord(buffer, blockHeader);      
        RecordUtils.writeRecords(buffer, records);
        RecordUtils.writeRecord(buffer, blockHeader2);   
        RecordUtils.writeRecords(buffer, records2);

        ReplayPosition replayPosition = new ReplayPosition(1, RecordUtils.computeSerializedSize(records));

        memTimeSeries = memTimeSeries.write(allocator, records, Futures.immediateFuture(replayPosition));

        replayPosition = new ReplayPosition(1, RecordUtils.computeSerializedSize(records)
                                            + RecordUtils.computeSerializedSize(records2));
        
        memTimeSeries2 = memTimeSeries2.write(allocator, records2, Futures.immediateFuture(replayPosition));

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

        List<TimeSeriesRecord> records = new RecordListBuilder(this.definition).newRecord("exchangeState")
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

        int blockSize = RecordUtils.computeSerializedSize(records);
        
        TimeSeriesRecord blockHeader = this.definition.newBlockHeader();
        BlockHeaderUtils.setFirstTimestampInNanos(blockHeader, TIME_IN_NANOS + 12000700);
        BlockHeaderUtils.setLastTimestampInNanos(blockHeader, TIME_IN_NANOS + 13004400);
        BlockHeaderUtils.setBlockSize(blockHeader, blockSize);
        BlockHeaderUtils.setRecordCount(blockHeader, 0, 3);
        
        
        Buffer buffer = Buffers.allocate(RecordUtils.computeSerializedSize(blockHeader) + blockSize);
        RecordUtils.writeRecord(buffer, blockHeader);      
        RecordUtils.writeRecords(buffer, records);

        memTimeSeries = memTimeSeries.write(allocator, records, Futures.immediateFuture(new ReplayPosition(1, 0)));

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
