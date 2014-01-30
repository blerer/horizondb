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
package io.horizondb.db.databases;

import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.series.TimeSeriesManager;
import io.horizondb.io.files.FileUtils;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.test.AssertFiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Benjamin
 * 
 */
public class DefaultDatabaseManagerTest {

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
    }

    @Test
    public void testCreateDatabase() throws IOException, InterruptedException, HorizonDBException {

        TimeSeriesManager timeSeriesManager = EasyMock.createNiceMock(TimeSeriesManager.class);

        DatabaseManager manager = new DefaultDatabaseManager(this.configuration, timeSeriesManager);

        manager.start();

        manager.createDatabase(new DatabaseDefinition("test"), null, true);

        Database database = manager.getDatabase("Test");

        assertEquals("test", database.getName());

        AssertFiles.assertFileExists(this.configuration.getDataDirectory().resolve("test"));

        manager.shutdown();
    }

    @Test
    public void testGetDatabaseWithUnknownDatabase() throws IOException, InterruptedException {

        TimeSeriesManager timeSeriesManager = EasyMock.createNiceMock(TimeSeriesManager.class);

        DatabaseManager manager = new DefaultDatabaseManager(this.configuration, timeSeriesManager);
        manager.start();

        try {

            manager.getDatabase("Test");
            Assert.fail();

        } catch (HorizonDBException e) {

            Assert.assertEquals(ErrorCodes.UNKNOWN_DATABASE, e.getCode());
        }

        manager.shutdown();
    }

    @Test
    public void testCreateDatabaseWithExistingDatabase() throws IOException, InterruptedException {

        TimeSeriesManager timeSeriesManager = EasyMock.createNiceMock(TimeSeriesManager.class);

        DatabaseManager manager = new DefaultDatabaseManager(this.configuration, timeSeriesManager);

        manager.start();

        try {

            manager.createDatabase(new DatabaseDefinition("test"), null, true);
            manager.createDatabase(new DatabaseDefinition("Test"), null, true);
            Assert.fail();

        } catch (HorizonDBException e) {

            Assert.assertEquals(ErrorCodes.DUPLICATE_DATABASE, e.getCode());
        }

        manager.shutdown();
    }

    @Test
    public void testCreateDatabaseWithExistingDatabaseAndNoExceptionThrown() throws IOException,
                                                                            InterruptedException,
                                                                            HorizonDBException {

        TimeSeriesManager timeSeriesManager = EasyMock.createNiceMock(TimeSeriesManager.class);

        DatabaseManager manager = new DefaultDatabaseManager(this.configuration, timeSeriesManager);

        manager.start();

        manager.createDatabase(new DatabaseDefinition("test"), null, true);
        manager.createDatabase(new DatabaseDefinition("Test"), null, false);

        manager.shutdown();
    }
}
