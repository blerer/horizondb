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
import io.horizondb.model.core.Field;
import io.horizondb.model.core.RecordListBuilder;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static io.horizondb.model.schema.FieldType.MILLISECONDS_TIMESTAMP;
import static org.junit.Assert.assertEquals;

/**
 * @author Benjamin
 * 
 */
public class TimeSeriesPartitionManagerCachesTest {

    private Path testDirectory;

    private Configuration configuration;

    @Before
    public void setUp() throws IOException {

        this.testDirectory = Files.createTempDirectory("test");

        this.configuration = Configuration.newBuilder()
                                          .dataDirectory(this.testDirectory.resolve("data"))
                                          .memTimeSeriesSize(70)
                                          .maximumMemoryUsageByMemTimeSeries(100)
                                          .cachesConcurrencyLevel(1)
                                          .build();
    }

    @After
    public void tearDown() throws IOException {

        FileUtils.forceDelete(this.testDirectory);

        this.testDirectory = null;
        this.configuration = null;
    }

    @Test
    public void testGlobalCacheWithWriteAndRead() throws InterruptedException, IOException, HorizonDBException {

        DefaultTimeSeriesPartitionManager partitionManager = new DefaultTimeSeriesPartitionManager(this.configuration);

        TimeSeriesPartitionManagerCaches caches = new TimeSeriesPartitionManagerCaches(this.configuration,
                                                                                       partitionManager);
        caches.start();

        try {

            Range<Field> range = MILLISECONDS_TIMESTAMP.range("'2013-11-26 00:00:00.000'", 
                                                              "'2013-11-27 00:00:00.000'");

            RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                            .addField("timestampInMillis",
                                                                                      FieldType.MILLISECONDS_TIMESTAMP)
                                                                            .addField("status", FieldType.BYTE)
                                                                            .build();

            Files.createDirectory(this.testDirectory.resolve("test"));

            DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

            TimeSeriesDefinition definition = databaseDefinition.newTimeSeriesDefinitionBuilder("DAX")
                                                                .timeUnit(TimeUnit.NANOSECONDS)
                                                                .addRecordType(recordTypeDefinition)
                                                                .build();

            PartitionId id = new PartitionId("test", "DAX", range);

            caches.getPartitionForWrite(id, definition);

            assertEquals(1, caches.globalCacheSize());
            assertEquals(0, caches.readCacheSize());
            assertEquals(1, caches.writeCacheSize());

            assertEquals(1, caches.globalCacheStats().loadCount());
            assertEquals(0, caches.globalCacheStats().hitCount());
            assertEquals(1, caches.globalCacheStats().missCount());

            assertEquals(1, caches.writeCacheStats().loadCount());
            assertEquals(0, caches.writeCacheStats().hitCount());
            assertEquals(1, caches.writeCacheStats().missCount());

            assertEquals(0, caches.readCacheStats().loadCount());
            assertEquals(0, caches.readCacheStats().hitCount());
            assertEquals(0, caches.readCacheStats().missCount());

            caches.getPartitionForRead(id, definition);

            assertEquals(1, caches.globalCacheSize());
            assertEquals(1, caches.readCacheSize());
            assertEquals(1, caches.writeCacheSize());

            assertEquals(1, caches.globalCacheStats().loadCount());
            assertEquals(1, caches.globalCacheStats().hitCount());
            assertEquals(1, caches.globalCacheStats().missCount());

            assertEquals(1, caches.writeCacheStats().loadCount());
            assertEquals(0, caches.writeCacheStats().hitCount());
            assertEquals(1, caches.writeCacheStats().missCount());

            assertEquals(1, caches.readCacheStats().loadCount());
            assertEquals(0, caches.readCacheStats().hitCount());
            assertEquals(1, caches.readCacheStats().missCount());

        } finally {

            caches.shutdown();
        }
    }

    @Test
    public void testGlobalCacheWeakReferencesWithRead() throws InterruptedException, IOException, HorizonDBException  {

        DefaultTimeSeriesPartitionManager partitionManager = new DefaultTimeSeriesPartitionManager(this.configuration);

        TimeSeriesPartitionManagerCaches caches = new TimeSeriesPartitionManagerCaches(this.configuration,
                                                                                       partitionManager);
        caches.start();

        try {

            Range<Field> range = MILLISECONDS_TIMESTAMP.range("'2013-11-26 00:00:00.000'", 
                                                              "'2013-11-27 00:00:00.000'");

            RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                            .addField("timestampInMillis",
                                                                                      FieldType.MILLISECONDS_TIMESTAMP)
                                                                            .addField("status", FieldType.BYTE)
                                                                            .build();

            Files.createDirectory(this.testDirectory.resolve("test"));

            DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

            TimeSeriesDefinition definition = databaseDefinition.newTimeSeriesDefinitionBuilder("DAX")
                                                                .timeUnit(TimeUnit.NANOSECONDS)
                                                                .addRecordType(recordTypeDefinition)
                                                                .build();

            PartitionId id = new PartitionId("test", "DAX", range);

            caches.getPartitionForRead(id, definition);

            assertEquals(1, caches.globalCacheSize());
            assertEquals(1, caches.readCacheSize());
            assertEquals(0, caches.writeCacheSize());

            assertEquals(1, caches.globalCacheStats().loadCount());
            assertEquals(0, caches.globalCacheStats().hitCount());
            assertEquals(1, caches.globalCacheStats().missCount());

            assertEquals(0, caches.writeCacheStats().loadCount());
            assertEquals(0, caches.writeCacheStats().hitCount());
            assertEquals(0, caches.writeCacheStats().missCount());

            assertEquals(1, caches.readCacheStats().loadCount());
            assertEquals(0, caches.readCacheStats().hitCount());
            assertEquals(1, caches.readCacheStats().missCount());

            caches.evictFromReadCache(id);

            System.gc();

            caches.getPartitionForRead(id, definition);

            assertEquals(1, caches.readCacheSize());
            assertEquals(1, caches.globalCacheSize());
            assertEquals(0, caches.writeCacheSize());

            assertEquals(2, caches.globalCacheStats().loadCount());
            assertEquals(0, caches.globalCacheStats().hitCount());
            assertEquals(2, caches.globalCacheStats().missCount());

            assertEquals(0, caches.writeCacheStats().loadCount());
            assertEquals(0, caches.writeCacheStats().hitCount());
            assertEquals(0, caches.writeCacheStats().missCount());

            assertEquals(2, caches.readCacheStats().loadCount());
            assertEquals(0, caches.readCacheStats().hitCount());
            assertEquals(2, caches.readCacheStats().missCount());

        } finally {

            caches.shutdown();
        }
    }

    @Test
    public void testGlobalCacheWithReadAndWrite() throws InterruptedException, IOException, HorizonDBException  {

        DefaultTimeSeriesPartitionManager partitionManager = new DefaultTimeSeriesPartitionManager(this.configuration);

        TimeSeriesPartitionManagerCaches caches = new TimeSeriesPartitionManagerCaches(this.configuration,
                                                                                       partitionManager);
        caches.start();

        try {

            Range<Field> range = MILLISECONDS_TIMESTAMP.range("'2013-11-26 00:00:00.000'", 
                                                              "'2013-11-27 00:00:00.000'");

            RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                            .addField("timestampInMillis",
                                                                                      FieldType.MILLISECONDS_TIMESTAMP)
                                                                            .addField("status", FieldType.BYTE)
                                                                            .build();

            Files.createDirectory(this.testDirectory.resolve("test"));

            DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

            TimeSeriesDefinition definition = databaseDefinition.newTimeSeriesDefinitionBuilder("DAX")
                                                                .timeUnit(TimeUnit.NANOSECONDS)
                                                                .addRecordType(recordTypeDefinition)
                                                                .build();

            PartitionId id = new PartitionId("test", "DAX", range);

            caches.getPartitionForRead(id, definition);

            assertEquals(1, caches.globalCacheSize());
            assertEquals(1, caches.readCacheSize());
            assertEquals(0, caches.writeCacheSize());

            assertEquals(1, caches.globalCacheStats().loadCount());
            assertEquals(0, caches.globalCacheStats().hitCount());
            assertEquals(1, caches.globalCacheStats().missCount());

            assertEquals(1, caches.readCacheStats().loadCount());
            assertEquals(0, caches.readCacheStats().hitCount());
            assertEquals(1, caches.readCacheStats().missCount());

            assertEquals(0, caches.writeCacheStats().loadCount());
            assertEquals(0, caches.writeCacheStats().hitCount());
            assertEquals(0, caches.writeCacheStats().missCount());

            caches.getPartitionForWrite(id, definition);

            assertEquals(1, caches.globalCacheSize());
            assertEquals(1, caches.readCacheSize());
            assertEquals(1, caches.writeCacheSize());

            assertEquals(1, caches.globalCacheStats().loadCount());
            assertEquals(1, caches.globalCacheStats().hitCount());
            assertEquals(1, caches.globalCacheStats().missCount());

            assertEquals(1, caches.writeCacheStats().loadCount());
            assertEquals(0, caches.writeCacheStats().hitCount());
            assertEquals(1, caches.writeCacheStats().missCount());

            assertEquals(1, caches.readCacheStats().loadCount());
            assertEquals(0, caches.readCacheStats().hitCount());
            assertEquals(1, caches.readCacheStats().missCount());

        } finally {

            caches.shutdown();
        }
    }

    @Test
    public void testForceFlush() throws Exception {

        Files.createDirectories(this.configuration.getDataDirectory().resolve("test"));

        DefaultTimeSeriesPartitionManager partitionManager = new DefaultTimeSeriesPartitionManager(this.configuration);

        TimeSeriesPartitionManagerCaches caches = new TimeSeriesPartitionManagerCaches(this.configuration,
                                                                                       partitionManager);
        caches.start();

        try {

            RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                            .addField("timestampInMillis",
                                                                                      FieldType.MILLISECONDS_TIMESTAMP)
                                                                            .addField("status", FieldType.BYTE)
                                                                            .build();

            Files.createDirectory(this.testDirectory.resolve("test"));

            DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

            TimeSeriesDefinition daxDefinition = databaseDefinition.newTimeSeriesDefinitionBuilder("DAX")
                                                                   .timeUnit(TimeUnit.NANOSECONDS)
                                                                   .addRecordType(recordTypeDefinition)
                                                                   .build();

            TimeSeriesDefinition cacDefinition = databaseDefinition.newTimeSeriesDefinitionBuilder("CAC40")
                                                                   .timeUnit(TimeUnit.NANOSECONDS)
                                                                   .addRecordType(recordTypeDefinition)
                                                                   .build();

            Range<Field> range = MILLISECONDS_TIMESTAMP.range("'2013-11-26 00:00:00.000'", 
                                                              "'2013-11-27 00:00:00.000'");
            
            PartitionId daxPartitionId = new PartitionId("test", "DAX", range);

            TimeSeriesPartition daxPartition = caches.getPartitionForWrite(daxPartitionId, daxDefinition);

            long timestamp = TimeUtils.parseDateTime("2013-11-26 12:32:12.000");

            List<TimeSeriesRecord> recordIterator = new RecordListBuilder(daxDefinition)
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

            daxPartition.write(recordIterator, Futures.immediateFuture(new ReplayPosition(0, 0)));

            PartitionId cacPartitionId = new PartitionId("test", "CAC40", range);

            TimeSeriesPartition cacPartition = caches.getPartitionForWrite(cacPartitionId, cacDefinition);

            assertEquals(2, caches.globalCacheSize());
            assertEquals(0, caches.readCacheSize());
            assertEquals(2, caches.writeCacheSize());

            assertEquals(2, caches.globalCacheStats().loadCount());
            assertEquals(0, caches.globalCacheStats().hitCount());
            assertEquals(2, caches.globalCacheStats().missCount());

            assertEquals(2, caches.writeCacheStats().loadCount());
            assertEquals(0, caches.writeCacheStats().hitCount());
            assertEquals(2, caches.writeCacheStats().missCount());
            assertEquals(0, caches.writeCacheStats().evictionCount());

            assertEquals(0, caches.readCacheStats().loadCount());
            assertEquals(0, caches.readCacheStats().hitCount());
            assertEquals(0, caches.readCacheStats().missCount());

            recordIterator = new RecordListBuilder(cacDefinition)
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

            cacPartition.write(recordIterator, newFuture());

            partitionManager.sync();

            assertEquals(2, caches.globalCacheSize());
            assertEquals(0, caches.readCacheSize());
            assertEquals(1, caches.writeCacheSize());

            assertEquals(2, caches.globalCacheStats().loadCount());
            assertEquals(0, caches.globalCacheStats().hitCount());
            assertEquals(2, caches.globalCacheStats().missCount());
            assertEquals(0, caches.globalCacheStats().evictionCount());

            assertEquals(2, caches.writeCacheStats().loadCount());
            assertEquals(0, caches.writeCacheStats().hitCount());
            assertEquals(3, caches.writeCacheStats().missCount());
            assertEquals(1, caches.writeCacheStats().evictionCount());

            assertEquals(0, caches.readCacheStats().loadCount());
            assertEquals(0, caches.readCacheStats().hitCount());
            assertEquals(0, caches.readCacheStats().missCount());

            daxPartition = null;

            partitionManager.sync();
            System.gc();

            daxPartition = caches.getPartitionForRead(daxPartitionId, daxDefinition);
            assertEquals(null, daxPartition.getFirstSegmentContainingNonPersistedData());
            
            assertEquals(2, caches.globalCacheSize());
            assertEquals(1, caches.readCacheSize());
            assertEquals(1, caches.writeCacheSize());

            assertEquals(3, caches.globalCacheStats().loadCount());
            assertEquals(0, caches.globalCacheStats().hitCount());
            assertEquals(3, caches.globalCacheStats().missCount());
            assertEquals(1, caches.globalCacheStats().evictionCount());

            assertEquals(2, caches.writeCacheStats().loadCount());
            assertEquals(0, caches.writeCacheStats().hitCount());
            assertEquals(3, caches.writeCacheStats().missCount());
            assertEquals(1, caches.writeCacheStats().evictionCount());

            assertEquals(1, caches.readCacheStats().loadCount());
            assertEquals(0, caches.readCacheStats().hitCount());
            assertEquals(1, caches.readCacheStats().missCount());

        } finally {

            caches.shutdown();
        }
    }

    private static ListenableFuture<ReplayPosition> newFuture() {

        return Futures.immediateCheckedFuture(new ReplayPosition(0, 0));
    }
}
