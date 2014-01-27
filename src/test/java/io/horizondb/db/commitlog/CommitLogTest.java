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
import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.files.FileUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Future;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.horizondb.db.commitlog.CommitLogSegment.LOG_OVERHEAD_SIZE;
import static io.horizondb.io.files.FileUtils.ONE_KB;
import static io.horizondb.test.AssertFiles.assertFileContainsAt;
import static org.easymock.EasyMock.eq;
import static org.junit.Assert.assertEquals;

/**
 * 
 * @author Benjamin
 * 
 */
public class CommitLogTest {

    /**
     * The test directory.
     */
    private Path testDirectory;

    /**
     * The configuration used during the tests.
     */
    private Configuration configuration;

    /**
     * The class under test.
     */
    private CommitLog commitLog;

    /**
     * The database engine.
     */
    private DatabaseEngine databaseEngine;

    @Before
    public void setUp() throws Exception {

        this.testDirectory = Files.createTempDirectory(this.getClass().getSimpleName());
        this.configuration = Configuration.newBuilder()
                                          .commitLogDirectory(this.testDirectory)
                                          .commitLogSegmentSize(8 * ONE_KB)
                                          .commitLogFlushPeriodInMillis(1000)
                                          .build();

        this.databaseEngine = EasyMock.createMock(DatabaseEngine.class);

        this.commitLog = new CommitLog(this.configuration, this.databaseEngine);
    }

    @After
    public void tearDown() throws Exception {

        this.commitLog.shutdown();
        this.commitLog = null;

        FileUtils.forceDelete(this.testDirectory);
        this.testDirectory = null;
        this.configuration = null;
    }

    /**
     * Test method for {@link io.horizondb.db.commitlog.CommitLog#add(io.netty.buffer.ByteBuf)}.
     */
    @Test
    public void testAdd() throws Exception {

        long expectedId = IdFactory.nextId() + 1;

        int position = 0;

        Buffer firstBuffer = Buffers.wrap(new byte[] { 1, 123, 12, 37 });
        position += (firstBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition firstPosition = new ReplayPosition(expectedId, position);

        Buffer secondBuffer = Buffers.wrap(new byte[] { -121, 5, 0, 30, 14, 56 });
        position += (secondBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition secondPosition = new ReplayPosition(expectedId, position);

        EasyMock.replay(this.databaseEngine);

        this.commitLog.start();

        assertEquals(firstPosition, this.commitLog.write(firstBuffer).get());

        Path expectedFile = this.testDirectory.resolve("CommitLog-" + expectedId + ".log");

        assertFileContainsAt(0, new byte[] { 4, 0, 0, 0 }, expectedFile);
        assertFileContainsAt(12, firstBuffer.array(), expectedFile);
        assertFileContainsAt(24, new byte[] { 0, 0, 0, 0 }, expectedFile);

        assertEquals(secondPosition, this.commitLog.write(secondBuffer).get());

        assertFileContainsAt(24, new byte[] { 6, 0, 0, 0 }, expectedFile);
        assertFileContainsAt(36, secondBuffer.array(), expectedFile);
        assertFileContainsAt(50, new byte[] { 0, 0, 0, 0 }, expectedFile);

        EasyMock.verify(this.databaseEngine);
    }
    
    /**
     * Test method for the recovery process.
     */
    @Test
    public void testSegmentSwitchAndRecovery() throws Exception {

        this.configuration = Configuration.newBuilder()
                                          .commitLogDirectory(this.testDirectory)
                                          .commitLogSegmentSize(50)
                                          .maximumNumberOfCommitLogSegments(3)
                                          .build();

        this.commitLog = new CommitLog(this.configuration, this.databaseEngine);

        long firstSegment = IdFactory.nextId() + 1;
        long secondSegment = firstSegment + 1;
        long thirdSegment = secondSegment + 1;

        int position = 0;

        Buffer firstBuffer = Buffers.wrap(new byte[] { 1, 123, 12, 37 });
        position += (firstBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition firstPosition = new ReplayPosition(firstSegment, position);

        Buffer secondBuffer = Buffers.wrap(new byte[] { -121, 5, 0, 30, 14, 56 });
        position += (secondBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition secondPosition = new ReplayPosition(firstSegment, position);

        position = 0; // Segment switch

        Buffer thirdBuffer = Buffers.wrap(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 });
        position += (thirdBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition thirdPosition = new ReplayPosition(secondSegment, position);

        position = 0; // Segment switch

        Buffer fourthBuffer = Buffers.wrap(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
        position += (fourthBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition fourthPosition = new ReplayPosition(thirdSegment, position);

        Future<Boolean> future = EasyMock.createMock(Future.class);

        EasyMock.expect(this.databaseEngine.forceFlush(firstSegment)).andReturn(future);
        EasyMock.expect(future.get()).andReturn(Boolean.TRUE);

        this.databaseEngine.replay(EasyMock.eq(thirdPosition), eq(thirdBuffer.duplicate()));
        this.databaseEngine.replay(EasyMock.eq(fourthPosition), eq(fourthBuffer.duplicate()));

        EasyMock.expect(this.databaseEngine.forceFlush(secondSegment)).andReturn(future);
        EasyMock.expect(future.get()).andReturn(Boolean.TRUE);

        EasyMock.replay(this.databaseEngine, future);

        this.commitLog.start();

        assertEquals(firstPosition, this.commitLog.write(firstBuffer).get());
        assertEquals(secondPosition, this.commitLog.write(secondBuffer).get());
        assertEquals(thirdPosition, this.commitLog.write(thirdBuffer).get());
        assertEquals(fourthPosition, this.commitLog.write(fourthBuffer).get());

        this.commitLog.shutdown();

        this.commitLog = new CommitLog(this.configuration, this.databaseEngine);
        this.commitLog.start();

        Thread.sleep(100);

        EasyMock.verify(this.databaseEngine, future);
    }
}
