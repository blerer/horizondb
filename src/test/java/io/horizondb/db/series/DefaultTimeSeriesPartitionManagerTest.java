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
import io.horizondb.db.btree.KeyValueIterator;
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.io.files.FileUtils;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.blocks.DataBlockBuilder;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.Futures;

import static io.horizondb.model.schema.FieldType.MILLISECONDS_TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultTimeSeriesPartitionManagerTest {

    private Path testDirectory;

    private Configuration configuration;

    @Before
    public void setUp() throws IOException {

        this.testDirectory = Files.createTempDirectory("test");

        this.configuration = Configuration.newBuilder().dataDirectory(this.testDirectory.resolve("data")).build();
    }

    @After
    public void tearDown() throws IOException {

        FileUtils.forceDelete(this.testDirectory);

        this.testDirectory = null;
        this.configuration = null;
    }

    @Test
    public void testGetRangeForReadWithNoValues() throws InterruptedException, IOException {

        AbstractTimeSeriesPartitionManager partitionManager = new OnDiskTimeSeriesPartitionManager(this.configuration);

        partitionManager.start();

        try {
            RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                            .addField("timestampInMillis",
                                                                                      FieldType.MILLISECONDS_TIMESTAMP)
                                                                            .addField("status", FieldType.BYTE)
                                                                            .build();

            DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

            TimeSeriesDefinition definition = databaseDefinition.newTimeSeriesDefinitionBuilder("DAX")
                                                                .timeUnit(TimeUnit.NANOSECONDS)
                                                                .addRecordType(recordTypeDefinition)
                                                                .build();

            Files.createDirectories(HorizonDBFiles.getTimeSeriesDirectory(this.configuration,
                                                                          databaseDefinition,
                                                                          definition));
            
            Range<Field> range = MILLISECONDS_TIMESTAMP.range("'2013-11-26'", "'2013-11-27'");
            
            PartitionId id = new PartitionId(databaseDefinition, definition, range);

            KeyValueIterator<PartitionId, TimeSeriesPartition> iterator = 
                    partitionManager.getRangeForRead(id, id, definition);
            
            assertFalse(iterator.next());

        } finally {

            partitionManager.shutdown();
        }
    }

    @Test
    public void testSave() throws InterruptedException, IOException, HorizonDBException, ExecutionException {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition definition = databaseDefinition.newTimeSeriesDefinitionBuilder("DAX")
                                                            .timeUnit(TimeUnit.NANOSECONDS)
                                                            .addRecordType(recordTypeDefinition)
                                                            .build();

        Files.createDirectories(HorizonDBFiles.getTimeSeriesDirectory(this.configuration,
                                                                      databaseDefinition,
                                                                      definition));

        Range<Field> range = MILLISECONDS_TIMESTAMP.range("'2013-11-26'", "'2013-11-27'");

        PartitionId id = new PartitionId(databaseDefinition, definition, range);

        OnDiskTimeSeriesPartitionManager partitionManager = new OnDiskTimeSeriesPartitionManager(this.configuration);

        try {

            partitionManager.start();

            TimeSeriesPartition partition = partitionManager.getPartitionForWrite(id, definition);

            long timestamp = TimeUtils.parseDateTime("2013-11-26 12:32:12.000");

            DataBlock records = new DataBlockBuilder(definition).newRecord("exchangeState")
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
            
            partition.write(records, Futures.immediateFuture(new ReplayPosition(1, 2)));
            partition.forceFlush();

        } finally {

            partitionManager.shutdown();
        }

        partitionManager = new OnDiskTimeSeriesPartitionManager(this.configuration);

        try {

            partitionManager.start();

            KeyValueIterator<PartitionId, TimeSeriesPartition> iterator = 
                    partitionManager.getRangeForRead(id, id, definition);
            
            assertTrue(iterator.next());
            
            TimeSeriesPartition partition = iterator.getValue();

            assertEquals(id, partition.getId());
            assertEquals(new ReplayPosition(1, 2), partition.getFuture().get());
            assertEquals(1087, partition.getMetaData().getFileSize());
            
            assertFalse(iterator.next());

        } finally {

            partitionManager.shutdown();
        }
    }
}
