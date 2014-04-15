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
import io.horizondb.io.files.FileUtils;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.schema.DatabaseDefinition;
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

/**
 * @author Benjamin
 * 
 */
public class DefaultTimeSeriesManagerTest {
    
    private Path testDirectory;

    private Configuration configuration;

    private TimeSeriesPartitionManager partitionManager;

    @Before
    public void setUp() throws IOException {

        this.testDirectory = Files.createTempDirectory("test");

        this.configuration = Configuration.newBuilder().dataDirectory(this.testDirectory.resolve("data")).build();

        this.partitionManager = EasyMock.createNiceMock(TimeSeriesPartitionManager.class);
    }

    @After
    public void tearDown() throws IOException {

        FileUtils.forceDelete(this.testDirectory);

        this.testDirectory = null;
        this.configuration = null;
        this.partitionManager = null;
    }

    @Test
    public void testCreateTimeSeries() throws IOException, InterruptedException, HorizonDBException {

        TimeSeriesManager manager = new DefaultTimeSeriesManager(this.partitionManager, this.configuration);

        RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                         .addDecimalField("bestBid")
                                                         .addDecimalField("bestAsk")
                                                         .addIntegerField("bidVolume")
                                                         .addIntegerField("askVolume")
                                                         .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition definition = databaseDefinition.newTimeSeriesDefinitionBuilder("DAX")
                                                            .timeUnit(TimeUnit.NANOSECONDS)
                                                            .addRecordType(quote)
                                                            .build();

        manager.start();

        manager.createTimeSeries("test", definition, true);

        manager.getTimeSeries("test", "DAX");

        manager.shutdown();
    }

    @Test
    public void testGetTimeSeriesWithUnknownTimeSeries() throws IOException, InterruptedException {

        TimeSeriesManager manager = new DefaultTimeSeriesManager(this.partitionManager, this.configuration);
        manager.start();

        try {

            manager.getTimeSeries("test", "DAX");
            Assert.fail();

        } catch (HorizonDBException e) {

            Assert.assertEquals(ErrorCodes.UNKNOWN_TIMESERIES, e.getCode());
        }

        manager.shutdown();
    }

    @Test
    public void testCreateTimeSeriesWithExistingTimeSeries() throws IOException, InterruptedException {

        TimeSeriesManager manager = new DefaultTimeSeriesManager(this.partitionManager, this.configuration);

        manager.start();

        try {

            RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                             .addDecimalField("bestBid")
                                                             .addDecimalField("bestAsk")
                                                             .addIntegerField("bidVolume")
                                                             .addIntegerField("askVolume")
                                                             .build();

            DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

            TimeSeriesDefinition definition = databaseDefinition.newTimeSeriesDefinitionBuilder("DAX")
                                                                .timeUnit(TimeUnit.NANOSECONDS)
                                                                .addRecordType(quote)
                                                                .build();

            manager.createTimeSeries("test", definition, true);

            TimeSeriesDefinition definition2 = databaseDefinition.newTimeSeriesDefinitionBuilder("dax")
                                                                 .timeUnit(TimeUnit.NANOSECONDS)
                                                                 .addRecordType(quote)
                                                                 .build();

            manager.createTimeSeries("test", definition2, true);
            Assert.fail();

        } catch (HorizonDBException e) {

            Assert.assertEquals(ErrorCodes.DUPLICATE_TIMESERIES, e.getCode());
        }

        manager.shutdown();
    }

    @Test
    public void testCreateTimeSeriesWithExistingTimeSeriesAndThrowExceptionFalse() throws IOException,
                                                                                  InterruptedException,
                                                                                  HorizonDBException {

        TimeSeriesManager manager = new DefaultTimeSeriesManager(this.partitionManager, this.configuration);

        manager.start();

        RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                         .addDecimalField("bestBid")
                                                         .addDecimalField("bestAsk")
                                                         .addIntegerField("bidVolume")
                                                         .addIntegerField("askVolume")
                                                         .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        TimeSeriesDefinition definition = databaseDefinition.newTimeSeriesDefinitionBuilder("DAX")
                                                            .timeUnit(TimeUnit.NANOSECONDS)
                                                            .addRecordType(quote)
                                                            .build();

        manager.createTimeSeries("test", definition, true);

        TimeSeriesDefinition definition2 = databaseDefinition.newTimeSeriesDefinitionBuilder("dax")
                                                             .timeUnit(TimeUnit.NANOSECONDS)
                                                             .addRecordType(quote)
                                                             .build();

        manager.createTimeSeries("test", definition2, false);
        manager.shutdown();
    }
}
