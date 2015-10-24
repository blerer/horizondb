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
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.HorizonDBFiles;
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.io.Buffer;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.buffers.CompositeBuffer;
import io.horizondb.io.files.FileUtils;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordUtils;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.core.ResourceIteratorUtils;
import io.horizondb.model.core.blocks.DataBlockBuilder;
import io.horizondb.model.core.iterators.BinaryTimeSeriesRecordIterator;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.BlockPosition;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;
import io.horizondb.test.AssertFiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.Futures;

import static io.horizondb.db.series.FileMetaData.METADATA_LENGTH;
import static io.horizondb.model.core.iterators.BlockIterators.compress;
import static io.horizondb.model.core.iterators.BlockIterators.iterator;
import static io.horizondb.model.core.records.BlockHeaderUtils.getCompressedBlockSize;
import static io.horizondb.model.core.records.BlockHeaderUtils.getFirstTimestamp;
import static io.horizondb.model.core.records.BlockHeaderUtils.getLastTimestamp;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        this.databaseDefinition = new DatabaseDefinition("test");

        this.definition = this.databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                 .timeUnit(TimeUnit.NANOSECONDS)
                                                 .addRecordType(recordTypeDefinition)
                                                 .build();

        Path seriesDirectory = HorizonDBFiles.getTimeSeriesDirectory(this.configuration,
                                                                     this.databaseDefinition,
                                                                     this.definition);

        Files.createDirectories(seriesDirectory);

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

        DataBlock block = new DataBlockBuilder(this.definition).newRecord("exchangeState")
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

        byte[] expectedFileContent = expectedFileContent(block);
        LinkedHashMap<Range<Field>, BlockPosition> expectedBlockPositions = expectedBlockPositions(block);
        
        memTimeSeries = memTimeSeries.write(allocator, block, Futures.immediateFuture(new ReplayPosition(1, 0)));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition, 
                                                       this.definition, 
                                                       this.metadata)) {

            TimeSeriesFile newFile = file.append(asList((TimeSeriesElement) memTimeSeries));

            assertEquals(expectedBlockPositions, newFile.getBlockPositions());
            AssertFiles.assertFileContains(expectedFileContent, newFile.getPath());
        }
    }

    

    @Test
    public void testDuplicateAppend() throws IOException, HorizonDBException, InterruptedException {

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        DataBlock block = new DataBlockBuilder(this.definition).newRecord("exchangeState")
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

        byte[] expectedFileContent = expectedFileContent(block);
        
        memTimeSeries = memTimeSeries.write(allocator, block, Futures.immediateFuture(new ReplayPosition(1, 0)));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition, 
                                                       this.definition, 
                                                       this.metadata)) {

            file.append(asList((TimeSeriesElement) memTimeSeries));
            file.append(asList((TimeSeriesElement) memTimeSeries));

            AssertFiles.assertFileContains(expectedFileContent, file.getPath());
        }
    }

    @Test
    public void testAppendWithTwoTimeSeriesInTheSameCall() throws IOException, HorizonDBException, InterruptedException {

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        MemTimeSeries memTimeSeries2 = new MemTimeSeries(this.configuration, this.definition);

        DataBlock block = new DataBlockBuilder(this.definition).newRecord("exchangeState")
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

        DataBlock block2 = new DataBlockBuilder(this.definition).newRecord("exchangeState")
                                                                .setTimestampInNanos(0, TIME_IN_NANOS + 13014400)
                                                                .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                .setByte(2, 2)
                                                                .newRecord("exchangeState")
                                                                .setTimestampInNanos(0, TIME_IN_NANOS + 14000900)
                                                                .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                                .setByte(2, 3)
                                                                .build();

        byte[] expectedFileContent = expectedFileContent(block, block2);
        LinkedHashMap<Range<Field>, BlockPosition> expectedBlockPositions = expectedBlockPositions(block, block2);
        
        ReplayPosition replayPosition = new ReplayPosition(1, block.computeSerializedSize());
        
        memTimeSeries = memTimeSeries.write(allocator, block, Futures.immediateFuture(replayPosition));

        replayPosition = new ReplayPosition(1, block.computeSerializedSize() + block2.computeSerializedSize());

        memTimeSeries2 = memTimeSeries2.write(allocator, block2, Futures.immediateFuture(replayPosition));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition, 
                                                       this.definition, 
                                                       this.metadata)) {

            TimeSeriesFile newFile = file.append(Arrays.<TimeSeriesElement> asList(memTimeSeries, memTimeSeries2));

            assertEquals(expectedBlockPositions, newFile.getBlockPositions());
            AssertFiles.assertFileContains(expectedFileContent, file.getPath());
        }
    }

    @Test
    public void testAppendWithTwoSeparateCalls() throws IOException, HorizonDBException, InterruptedException {

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        MemTimeSeries memTimeSeries2 = new MemTimeSeries(this.configuration, this.definition);

        DataBlock block = new DataBlockBuilder(this.definition).newRecord("exchangeState")
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

        DataBlock block2 = new DataBlockBuilder(this.definition).newRecord("exchangeState")
                                                                .setTimestampInNanos(0, TIME_IN_NANOS + 13014400)
                                                                .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                .setByte(2, 2)
                                                                .newRecord("exchangeState")
                                                                .setTimestampInNanos(0, TIME_IN_NANOS + 14000900)
                                                                .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                                .setByte(2, 3)
                                                                .build();

        byte[] expectedFileContent = expectedFileContent(block, block2);
        LinkedHashMap<Range<Field>, BlockPosition> expectedBlockPositions = expectedBlockPositions(block, block2);

        ReplayPosition replayPosition = new ReplayPosition(1, block.computeSerializedSize());

        memTimeSeries = memTimeSeries.write(allocator, block, Futures.immediateFuture(replayPosition));

        replayPosition = new ReplayPosition(1, block.computeSerializedSize()
                                            + block2.computeSerializedSize());
        
        memTimeSeries2 = memTimeSeries2.write(allocator, block2, Futures.immediateFuture(replayPosition));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition, 
                                                       this.definition, 
                                                       this.metadata)) {

            TimeSeriesFile newFile = file.append(Arrays.<TimeSeriesElement> asList(memTimeSeries))
                                         .append(Arrays.<TimeSeriesElement> asList(memTimeSeries2));
            
            assertEquals(expectedBlockPositions, newFile.getBlockPositions());
            AssertFiles.assertFileContains(expectedFileContent, file.getPath());
        }
    }

    @Test
    public void testNewInputWithTwoBlocks() throws IOException, HorizonDBException, InterruptedException {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        this.definition = this.databaseDefinition.newTimeSeriesDefinitionBuilder("test2")
                                                 .timeUnit(TimeUnit.NANOSECONDS)
                                                 .blockSize(35)
                                                 .addRecordType(recordTypeDefinition)
                                                 .build();
        
        Path seriesDirectory = HorizonDBFiles.getTimeSeriesDirectory(this.configuration,
                                                                     this.databaseDefinition,
                                                                     this.definition);

        Files.createDirectories(seriesDirectory);
        
        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        DataBlock block = new DataBlockBuilder(this.definition).newRecord("exchangeState")
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

        DataBlock block2 = new DataBlockBuilder(this.definition).newRecord("exchangeState")
                                                                .setTimestampInNanos(0, TIME_IN_NANOS + 13014400)
                                                                .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                .setByte(2, 1)
                                                                .build();

        byte[] expectedFileContent = expectedFileContent(block, block2);
        LinkedHashMap<Range<Field>, BlockPosition> expectedBlockPositions = expectedBlockPositions(block, block2);

        ReplayPosition replayPosition = new ReplayPosition(1, block.computeSerializedSize());

        memTimeSeries = memTimeSeries.write(allocator, block, Futures.immediateFuture(replayPosition));

        replayPosition = new ReplayPosition(1, block.computeSerializedSize()
                                            + block2.computeSerializedSize());

        memTimeSeries = memTimeSeries.write(allocator, block2, Futures.immediateFuture(replayPosition));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition, 
                                                       this.definition, 
                                                       this.metadata)) {

            TimeSeriesFile newFile = file.append(Arrays.<TimeSeriesElement> asList(memTimeSeries));
            assertEquals(expectedBlockPositions, newFile.getBlockPositions());
            AssertFiles.assertFileContains(expectedFileContent, file.getPath());
            
            try (ResourceIterator<BinaryTimeSeriesRecord> readIterator = new BinaryTimeSeriesRecordIterator(this.definition, 
                                                                                  newFile.iterator())) {

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
                assertEquals(TIME_IN_NANOS + 13014400, actual.getTimestampInNanos(0));
                assertEquals(TIME_IN_MILLIS + 13, actual.getTimestampInMillis(1));
                assertEquals(1, actual.getByte(2));

                assertFalse(readIterator.hasNext());
            }
        }
    }
    
    @Test
    public void testAppendWithTwoBlocksWithOverlappingRange() throws IOException, HorizonDBException, InterruptedException {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        this.definition = this.databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                 .timeUnit(TimeUnit.NANOSECONDS)
                                                 .blockSize(32)
                                                 .addRecordType(recordTypeDefinition)
                                                 .build();

        Files.createDirectories(HorizonDBFiles.getTimeSeriesDirectory(this.configuration,
                                                                      this.databaseDefinition,
                                                                      this.definition));

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        DataBlock block = new DataBlockBuilder(this.definition).newRecord("exchangeState")
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

        DataBlock block2 = new DataBlockBuilder(this.definition).newRecord("exchangeState")
                                                                .setTimestampInNanos(0, TIME_IN_NANOS + 13004400)
                                                                .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                .setByte(2, 1)
                                                                .build();

        byte[] expectedFileContent = expectedFileContent(block, block2);
        LinkedHashMap<Range<Field>, BlockPosition> expectedBlockPositions = expectedBlockPositions(block, block2);

        ReplayPosition replayPosition = new ReplayPosition(1, block.computeSerializedSize());

        memTimeSeries = memTimeSeries.write(allocator, block, Futures.immediateFuture(replayPosition));

        replayPosition = new ReplayPosition(1, block.computeSerializedSize()
                                            + block2.computeSerializedSize());
        
        memTimeSeries = memTimeSeries.write(allocator, block2, Futures.immediateFuture(replayPosition));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition, 
                                                       this.definition, 
                                                       this.metadata)) {

            TimeSeriesFile newFile = file.append(Arrays.<TimeSeriesElement> asList(memTimeSeries));

            assertEquals(expectedBlockPositions, newFile.getBlockPositions());
            AssertFiles.assertFileContains(expectedFileContent, file.getPath());
            
            try (ResourceIterator<BinaryTimeSeriesRecord> readIterator = new BinaryTimeSeriesRecordIterator(this.definition, 
                                                                                                            newFile.iterator())) {
                
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
                assertEquals(TIME_IN_NANOS + 13004400, actual.getTimestampInNanos(0));
                assertEquals(TIME_IN_MILLIS + 13, actual.getTimestampInMillis(1));
                assertEquals(1, actual.getByte(2));

                assertFalse(readIterator.hasNext());
            }
        }
    }
    
    @Test
    public void testNewInputWithTwoBlocksAndOnlyFirstOneHit() throws IOException, HorizonDBException, InterruptedException {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        this.definition = this.databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                 .timeUnit(TimeUnit.NANOSECONDS)
                                                 .blockSize(35)
                                                 .addRecordType(recordTypeDefinition)
                                                 .build();

        Files.createDirectories(HorizonDBFiles.getTimeSeriesDirectory(this.configuration,
                                                                      this.databaseDefinition,
                                                                      this.definition));

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        DataBlock block = new DataBlockBuilder(this.definition).newRecord("exchangeState")
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

        DataBlock block2 = new DataBlockBuilder(this.definition).newRecord("exchangeState")
                                                                .setTimestampInNanos(0, TIME_IN_NANOS + 13014400)
                                                                .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                .setByte(2, 1)
                                                                .build();

        byte[] expectedFileContent = expectedFileContent(block, block2);
        LinkedHashMap<Range<Field>, BlockPosition> expectedBlockPositions = expectedBlockPositions(block, block2);

        ReplayPosition replayPosition = new ReplayPosition(1, block.computeSerializedSize());

        memTimeSeries = memTimeSeries.write(allocator, block, Futures.immediateFuture(replayPosition));

        replayPosition = new ReplayPosition(1, block.computeSerializedSize()
                                            + block2.computeSerializedSize());
        
        memTimeSeries = memTimeSeries.write(allocator, block2, Futures.immediateFuture(replayPosition));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition, 
                                                       this.definition, 
                                                       this.metadata)) {

            TimeSeriesFile newFile = file.append(Arrays.<TimeSeriesElement> asList(memTimeSeries));
            
            assertEquals(expectedBlockPositions, newFile.getBlockPositions());
            AssertFiles.assertFileContains(expectedFileContent, file.getPath());
            
            RangeSet<Field> rangeSet = ImmutableRangeSet.of(newTimestampRange(TIME_IN_NANOS + 13000000, TIME_IN_NANOS + 13006000));
            
            try (ResourceIterator<BinaryTimeSeriesRecord> readIterator = new BinaryTimeSeriesRecordIterator(this.definition, 
                                                                                                            newFile.iterator(rangeSet))) {
                
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
    public void testNewInputWithTwoBlocksAndOnlySecondOneHit() throws IOException, HorizonDBException, InterruptedException {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        this.definition = this.databaseDefinition.newTimeSeriesDefinitionBuilder("test")
                                                 .timeUnit(TimeUnit.NANOSECONDS)
                                                 .blockSize(35)
                                                 .addRecordType(recordTypeDefinition)
                                                 .build();

        Files.createDirectories(HorizonDBFiles.getTimeSeriesDirectory(this.configuration,
                                                                      this.databaseDefinition,
                                                                      this.definition));

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        DataBlock block = new DataBlockBuilder(this.definition).newRecord("exchangeState")
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

        DataBlock block2 = new DataBlockBuilder(this.definition).newRecord("exchangeState")
                                                                .setTimestampInNanos(0, TIME_IN_NANOS + 13014400)
                                                                .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                                .setByte(2, 1)
                                                                .build();

        byte[] expectedFileContent = expectedFileContent(block, block2);
        LinkedHashMap<Range<Field>, BlockPosition> expectedBlockPositions = expectedBlockPositions(block, block2);

        ReplayPosition replayPosition = new ReplayPosition(1, block.computeSerializedSize());

        memTimeSeries = memTimeSeries.write(allocator, block, Futures.immediateFuture(replayPosition));

        replayPosition = new ReplayPosition(1, block.computeSerializedSize()
                                            + block2.computeSerializedSize());
        
        memTimeSeries = memTimeSeries.write(allocator, block2, Futures.immediateFuture(replayPosition));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition, 
                                                       this.definition, 
                                                       this.metadata)) {

            TimeSeriesFile newFile = file.append(Arrays.<TimeSeriesElement> asList(memTimeSeries));
            
            assertEquals(expectedBlockPositions, newFile.getBlockPositions());
            AssertFiles.assertFileContains(expectedFileContent, file.getPath());
            
            RangeSet<Field> rangeSet = ImmutableRangeSet.of(newTimestampRange(TIME_IN_NANOS + 13006000, TIME_IN_NANOS + 14000000));
            
            try (ResourceIterator<BinaryTimeSeriesRecord> readIterator = new BinaryTimeSeriesRecordIterator(this.definition, 
                                                                                                            newFile.iterator(rangeSet))) {
                
                assertTrue(readIterator.hasNext());
                Record actual = readIterator.next();

                assertFalse(actual.isDelta());
                assertEquals(TIME_IN_NANOS + 13014400, actual.getTimestampInNanos(0));
                assertEquals(TIME_IN_MILLIS + 13, actual.getTimestampInMillis(1));
                assertEquals(1, actual.getByte(2));

                assertFalse(readIterator.hasNext());
            }
        }
    }
    
    @Test
    public void testNewInput() throws IOException, HorizonDBException, InterruptedException {

        SlabAllocator allocator = new SlabAllocator(this.configuration.getMemTimeSeriesSize());

        MemTimeSeries memTimeSeries = new MemTimeSeries(this.configuration, this.definition);

        DataBlock block = new DataBlockBuilder(this.definition).newRecord("exchangeState")
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

        byte[] expectedFileContent = expectedFileContent(block);

        memTimeSeries = memTimeSeries.write(allocator, block, Futures.immediateFuture(new ReplayPosition(1, 0)));

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition,
                                                       this.definition, 
                                                       this.metadata)) {

            try (SeekableFileDataInput input = file.append(Arrays.<TimeSeriesElement> asList(memTimeSeries)).newInput()) {

                ReadableBuffer content = input.slice((int) input.size());
                
                Assert.assertEquals(Buffers.wrap(expectedFileContent, METADATA_LENGTH, expectedFileContent.length - METADATA_LENGTH), content);
            }
        }
    }

    @Test
    public void testNewInputWithEmptyFile() throws IOException {

        try (TimeSeriesFile file = TimeSeriesFile.open(this.configuration, 
                                                       this.databaseDefinition, 
                                                       this.definition, 
                                                       this.metadata)) {

            try (SeekableFileDataInput input = file.newInput()) {

                Assert.assertFalse(input.isReadable());
            }
        }
    }
    
    /**
     * Creates a new range of timestamps.
     * 
     * @param fromInNanos the lower end point of the range in nanoseconds (inclusive)
     * @param toInNanos the upper end point of the range in nanoseconds (inclusive)
     * @return a new range of timestamps.
     */
    private static Range<Field> newTimestampRange(long fromInNanos, long toInNanos) {
        
        Field from = FieldType.NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(fromInNanos);
        Field to = FieldType.NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(toInNanos);
        
        return Range.closed(from, to);
    }
    
    /**
     * Returns the expected block positions for the blocks containing the specified records.
     * 
     * @param blocks the blocks of records
     * @return the expected block positions for the blocks containing the specified records.
     * @throws IOException if an I/O problem occurs
     */
    @SafeVarargs
    private final LinkedHashMap<Range<Field>, BlockPosition> expectedBlockPositions(DataBlock... blocks) throws IOException {
        
        int position = FileMetaData.METADATA_LENGTH;
        LinkedHashMap<Range<Field>, BlockPosition> map = new LinkedHashMap<>();

        try (ResourceIterator<DataBlock> iterator = compress(this.definition.getCompressionType(), iterator(blocks))) {
            while (iterator.hasNext()) {

                DataBlock block = iterator.next();
                Record header = block.getHeader();

                Range<Field> range = newTimestampRange(getFirstTimestamp(header), getLastTimestamp(header));

                int length = RecordUtils.computeSerializedSize(header) + getCompressedBlockSize(header);

                map.put(range, new BlockPosition(position, length));
                position += length;
            }
        }

        return map;
    }
    
    /**
     * Returns the expected file content for the specified blocks of records.
     * 
     * @param records the blocks of records that the file will contains
     * @return the expected file content for the specified blocks of records
     * @throws IOException if an I/O problem occurs
     */
    @SafeVarargs
    private final byte[] expectedFileContent(DataBlock... blocks) throws IOException {

        FileMetaData fileMetaData = new FileMetaData(this.databaseDefinition.getName(),
                                                     this.definition.getName(),
                                                     this.metadata.getRange());

        return toFileContent(fileMetaData, Arrays.asList(blocks));
    }

    private byte[] toFileContent(FileMetaData fileMetaData,
                                 List<DataBlock> blocks) throws IOException {

        CompositeBuffer buffer = new CompositeBuffer();

        Buffer metadata = Buffers.allocate(fileMetaData.computeSerializedSize());
        fileMetaData.writeTo(metadata);
        buffer.addBytes(metadata);

        try (ResourceIterator<DataBlock> iterator = compress(this.definition.getCompressionType(), iterator(blocks))) {
            ReadableBuffer data = ResourceIteratorUtils.toBytes(iterator);
            buffer.addBytes(data);
        }
        return Buffers.toArray(buffer);
    }
}
