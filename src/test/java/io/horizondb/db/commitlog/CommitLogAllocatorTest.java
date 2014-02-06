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
package io.horizondb.db.commitlog;

import io.horizondb.db.Configuration;
import io.horizondb.db.DatabaseEngine;
import io.horizondb.io.files.FileUtils;

import java.nio.file.Files;
import java.nio.file.Path;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.ListenableFuture;

import static io.horizondb.io.files.FileUtils.ONE_KB;
import static io.horizondb.test.AssertCollections.assertIterableContains;
import static io.horizondb.test.AssertFiles.assertFileDoesNotExists;
import static io.horizondb.test.AssertFiles.assertFileExists;

/**
 * @author Benjamin
 * 
 */
public class CommitLogAllocatorTest {
    /**
     * The test directory.
     */
    private Path testDirectory;

    /**
     * The configuration used during the tests.
     */
    private Configuration configuration;

    /**
     * The mock database engine.
     */
    private DatabaseEngine databaseEngine;

    /**
     * The class under test.
     */
    private CommitLogAllocator allocator;

    @Before
    public void setUp() throws Exception {

        this.testDirectory = Files.createTempDirectory("test");
        this.configuration = Configuration.newBuilder()
                                          .commitLogDirectory(this.testDirectory)
                                          .commitLogSegmentSize(8 * ONE_KB)
                                          .maximumNumberOfCommitLogSegments(3)
                                          .build();

        this.databaseEngine = EasyMock.createMock(DatabaseEngine.class);
    }

    @After
    public void tearDown() throws Exception {

        this.allocator.shutdown();
        this.allocator = null;

        FileUtils.forceDelete(this.testDirectory);
        this.testDirectory = null;
        this.configuration = null;
        this.databaseEngine = null;
    }

    @Test
    public void testFetchSegment() throws Exception {

        long id = IdFactory.nextId();

        ListenableFuture<Boolean> forceFlushFuture = EasyMock.createMock(ListenableFuture.class);

        EasyMock.expect(forceFlushFuture.get()).andReturn(Boolean.TRUE).anyTimes();
        EasyMock.expect(this.databaseEngine.forceFlush(id + 1)).andReturn(forceFlushFuture);
        EasyMock.expect(this.databaseEngine.forceFlush(id + 2)).andReturn(forceFlushFuture);

        EasyMock.replay(this.databaseEngine, forceFlushFuture);

        this.allocator = new CommitLogAllocator(this.configuration, this.databaseEngine);
        this.allocator.start();

        CommitLogSegment firstSegment = this.allocator.fetchSegment();

        assertFileExists(firstSegment.getPath());
        assertIterableContains(this.allocator.getActiveSegments(), firstSegment);

        CommitLogSegment secondSegment = this.allocator.fetchSegment();

        assertFileExists(secondSegment.getPath());
        assertIterableContains(this.allocator.getActiveSegments(), firstSegment, secondSegment);

        CommitLogSegment thirdSegment = this.allocator.fetchSegment();

        assertFileExists(thirdSegment.getPath());

        this.allocator.sync();

        assertIterableContains(this.allocator.getActiveSegments(), secondSegment, thirdSegment);

        CommitLogSegment fourthSegment = this.allocator.fetchSegment();

        assertFileExists(fourthSegment.getPath());
        assertFileDoesNotExists(firstSegment.getPath());

        this.allocator.sync();

        assertIterableContains(this.allocator.getActiveSegments(), thirdSegment, fourthSegment);

        EasyMock.verify(this.databaseEngine, forceFlushFuture);
    }
}
