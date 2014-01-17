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
import io.horizondb.model.DatabaseDefinition;
import io.horizondb.model.ErrorCodes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.cache.CacheStats;

import static org.junit.Assert.assertEquals;

/**
 * @author Benjamin
 * 
 */
public class DatabaseManagerCacheTest {

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
    public void testCreateDatabase() throws IOException, HorizonDBException, InterruptedException {

        TimeSeriesManager timeSeriesManager = EasyMock.createNiceMock(TimeSeriesManager.class);

        DatabaseManager manager = EasyMock.createMock(DatabaseManager.class);

        manager.start();
        manager.createDatabase(new DatabaseDefinition("test"), true);
        Database database = new Database(this.configuration, new DatabaseDefinition("test"), timeSeriesManager);
        EasyMock.expect(manager.getDatabase("test")).andReturn(database);
        manager.shutdown();

        EasyMock.replay(manager);

        DatabaseManagerCache cache = new DatabaseManagerCache(this.configuration, manager);
        cache.start();

        cache.createDatabase(new DatabaseDefinition("test"), true);

        Database firstCall = cache.getDatabase("Test");

        assertEquals("test", firstCall.getName());

        Database secondCall = cache.getDatabase("test");

        assertEquals(firstCall, secondCall);

        Database thirdCall = cache.getDatabase("test");

        assertEquals(firstCall, thirdCall);

        CacheStats stats = cache.stats();

        assertEquals(1, stats.loadCount());
        assertEquals(1, stats.missCount());
        assertEquals(2, stats.hitCount());

        assertEquals(1, cache.size());

        cache.shutdown();

        EasyMock.verify(manager);
    }

    @Test
    public void testGetDatabaseWithUnknownDatabase() throws IOException, HorizonDBException, InterruptedException {

        TimeSeriesManager timeSeriesManager = EasyMock.createNiceMock(TimeSeriesManager.class);

        DatabaseManager manager = EasyMock.createMock(DatabaseManager.class);

        EasyMock.expect(manager.getDatabase("test")).andThrow(new HorizonDBException(ErrorCodes.UNKNOWN_DATABASE,
                                                                                     "boom"));

        manager.start();
        manager.createDatabase(new DatabaseDefinition("test"), true);
        Database database = new Database(this.configuration, new DatabaseDefinition("test"), timeSeriesManager);
        EasyMock.expect(manager.getDatabase("test")).andReturn(database);
        manager.shutdown();

        EasyMock.replay(manager);

        DatabaseManagerCache cache = new DatabaseManagerCache(this.configuration, manager);
        cache.start();

        try {

            cache.getDatabase("Test");
            Assert.fail();

        } catch (HorizonDBException e) {

            Assert.assertEquals(ErrorCodes.UNKNOWN_DATABASE, e.getCode());
        }

        cache.createDatabase(new DatabaseDefinition("test"), true);

        Database firstCall = cache.getDatabase("Test");

        assertEquals("test", firstCall.getName());

        CacheStats stats = cache.stats();

        assertEquals(2, stats.loadCount());
        assertEquals(1, stats.loadExceptionCount());
        assertEquals(2, stats.missCount());
        assertEquals(0, stats.hitCount());

        assertEquals(1, cache.size());

        cache.shutdown();

        EasyMock.verify(manager);
    }

    @Test
    public void testCreateDatabaseWithExistingDatabase() throws IOException, HorizonDBException, InterruptedException {

        DatabaseManager manager = EasyMock.createMock(DatabaseManager.class);
        manager.start();
        manager.createDatabase(new DatabaseDefinition("test"), true);
        manager.createDatabase(new DatabaseDefinition("test"), true);
        EasyMock.expectLastCall().andThrow(new HorizonDBException(ErrorCodes.DUPLICATE_DATABASE, "boom"));
        manager.shutdown();

        EasyMock.replay(manager);

        DatabaseManagerCache cache = new DatabaseManagerCache(this.configuration, manager);
        cache.start();

        try {

            cache.createDatabase(new DatabaseDefinition("test"), true);
            cache.createDatabase(new DatabaseDefinition("test"), true);
            Assert.fail();

        } catch (HorizonDBException e) {

            Assert.assertEquals(ErrorCodes.DUPLICATE_DATABASE, e.getCode());
        }
        cache.shutdown();

        EasyMock.verify(manager);
    }

}
