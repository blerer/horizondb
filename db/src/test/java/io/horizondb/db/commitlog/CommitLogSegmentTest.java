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
import io.horizondb.test.AssertFiles;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.horizondb.db.commitlog.CommitLogSegment.LOG_OVERHEAD_SIZE;
import static io.horizondb.io.files.FileUtils.ONE_KB;
import static io.horizondb.test.AssertFiles.assertFileContainsAt;
import static io.horizondb.test.AssertFiles.assertFileDoesNotExists;
import static io.horizondb.test.AssertFiles.assertFileExists;
import static io.horizondb.test.AssertFiles.assertFileSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CommitLogSegmentTest {

    /**
     * The test directory.
     */
    private Path testDirectory;

    /**
     * The configuration used during the tests.
     */
    private Configuration configuration;

    @Before
    public void setUp() throws IOException {

        this.testDirectory = Files.createTempDirectory("test");
        this.configuration = Configuration.newBuilder()
                                          .commitLogDirectory(this.testDirectory)
                                          .commitLogSegmentSize(8 * ONE_KB)
                                          .build();
    }

    @After
    public void tearDown() throws IOException {

        FileUtils.forceDelete(this.testDirectory);
        this.testDirectory = null;
        this.configuration = null;
    }

    @Test
    public void testFreshSegment() throws Exception {

        long expectedId = IdFactory.nextId() + 1;

        try (CommitLogSegment segment = CommitLogSegment.freshSegment(this.configuration)) {

            segment.flush();

            Path expectedFile = this.testDirectory.resolve("CommitLog-" + expectedId + ".log");
            assertFileExists(expectedFile);
            assertFileSize(8 * ONE_KB, expectedFile);
        }
    }

    @Test
    public void testRecycle() throws Exception {

        long expectedId = IdFactory.nextId() + 1;

        CommitLogSegment segment = CommitLogSegment.freshSegment(this.configuration);
        segment.flush();

        Path expectedFile = this.testDirectory.resolve("CommitLog-" + expectedId + ".log");
        assertFileExists(expectedFile);
        assertFileSize(8 * ONE_KB, expectedFile);

        try (CommitLogSegment recycledSegment = CommitLogSegment.recycleSegment(segment)) {

            recycledSegment.flush();

            assertFileDoesNotExists(expectedFile);

            expectedFile = this.testDirectory.resolve("CommitLog-" + (expectedId + 1) + ".log");
            assertFileExists(expectedFile);
            assertFileSize(8 * ONE_KB, expectedFile);
        }
    }

    @Test
    public void testIsCommitLogSegment() {

        Path path = this.testDirectory.resolve("CommitLog-1456.log");
        assertTrue(CommitLogSegment.isCommitLogSegment(path));

        path = this.testDirectory.resolve("commitlog-1456.log");
        assertFalse(CommitLogSegment.isCommitLogSegment(path));

        path = this.testDirectory.resolve("commitlog-.log");
        assertFalse(CommitLogSegment.isCommitLogSegment(path));
    }

    @Test
    public void testWriteWithEmptyBuffer() throws Exception {

        long expectedId = IdFactory.nextId() + 1;

        try (CommitLogSegment segment = CommitLogSegment.freshSegment(this.configuration)) {

            segment.write(Buffers.EMPTY_BUFFER);
            Path expectedFile = this.testDirectory.resolve("CommitLog-" + expectedId + ".log");

            AssertFiles.assertFileContainsAt(0, new byte[] { 0, 0, 0, 0 }, expectedFile);
        }
    }

    @Test
    public void testWrite() throws Exception {

        long expectedId = IdFactory.nextId() + 1;

        int position = 0;

        try (CommitLogSegment segment = CommitLogSegment.freshSegment(this.configuration)) {

            byte[] byteArray = new byte[] { 1, 123, 12, 37 };
            Buffer buffer = Buffers.wrap(byteArray);

            position += (buffer.readableBytes() + LOG_OVERHEAD_SIZE);

            assertEquals(new ReplayPosition(expectedId, position), segment.write(buffer));
            segment.flush();

            Path expectedFile = this.testDirectory.resolve("CommitLog-" + expectedId + ".log");

            assertFileContainsAt(0, new byte[] { 4, 0, 0, 0 }, expectedFile);
            assertFileContainsAt(12, byteArray, expectedFile);
            assertFileContainsAt(24, new byte[] { 0, 0, 0, 0 }, expectedFile);

            byteArray = new byte[] { 6, 21, 17, 9 };
            buffer = Buffers.wrap(byteArray);

            position += (buffer.readableBytes() + LOG_OVERHEAD_SIZE);

            assertEquals(new ReplayPosition(expectedId, position), segment.write(buffer));
            segment.flush();

            assertFileContainsAt(24, new byte[] { 4, 0, 0, 0 }, expectedFile);
            assertFileContainsAt(36, byteArray, expectedFile);
            assertFileContainsAt(48, new byte[] { 0, 0, 0, 0 }, expectedFile);
        }
    }

    @Test
    public void testReplayWithNoData() throws Exception {

        DatabaseEngine databaseEngine = EasyMock.createMock(DatabaseEngine.class);

        EasyMock.replay(databaseEngine);

        Path path;

        try (CommitLogSegment segment = CommitLogSegment.freshSegment(this.configuration)) {

            path = segment.getPath();
        }

        try (CommitLogSegment segment = CommitLogSegment.loadFromFile(this.configuration, path)) {

            segment.replay(databaseEngine);
        }

        EasyMock.verify(databaseEngine);
    }

    @Test
    public void testReplay() throws Exception {

        long segmentId = IdFactory.nextId() + 1;
        int position = 0;

        Buffer firstBuffer = Buffers.wrap(new byte[] { 1, 123, 12, 37 });
        position += (firstBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition firstPosition = new ReplayPosition(segmentId, position);

        Buffer secondBuffer = Buffers.wrap(new byte[] { -121, 5, 0, 30, 14, 56 });
        position += (secondBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition secondPosition = new ReplayPosition(segmentId, position);

        Buffer thirdBuffer = Buffers.wrap(new byte[] { 4, 85, 0 });
        position += (thirdBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition thirdPosition = new ReplayPosition(segmentId, position);

        DatabaseEngine databaseEngine = EasyMock.createMock(DatabaseEngine.class);

        databaseEngine.replay(firstPosition, firstBuffer.duplicate());
        databaseEngine.replay(secondPosition, secondBuffer.duplicate());
        databaseEngine.replay(thirdPosition, thirdBuffer.duplicate());

        EasyMock.replay(databaseEngine);

        Path path;

        try (CommitLogSegment segment = CommitLogSegment.freshSegment(this.configuration)) {

            path = segment.getPath();

            assertEquals(firstPosition, segment.write(firstBuffer));
            assertEquals(secondPosition, segment.write(secondBuffer));
            assertEquals(thirdPosition, segment.write(thirdBuffer));

            segment.flush();
        }

        try (CommitLogSegment segment = CommitLogSegment.loadFromFile(this.configuration, path)) {

            segment.replay(databaseEngine);
        }

        EasyMock.verify(databaseEngine);
    }

    /**
     * Test the replay when the file has been truncated.
     * 
     * @throws Exception if a problem occurs.
     */
    @Test
    public void testReplayWithTruncatedFile() throws Exception {

        long segmentId = IdFactory.nextId() + 1;
        int position = 0;

        Buffer firstBuffer = Buffers.wrap(new byte[] { 1, 123, 12, 37 });
        position += (firstBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition firstPosition = new ReplayPosition(segmentId, position);

        Buffer secondBuffer = Buffers.wrap(new byte[] { -121, 5, 0, 30, 14, 56 });
        position += (secondBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition secondPosition = new ReplayPosition(segmentId, position);

        Buffer thirdBuffer = Buffers.wrap(new byte[] { 4, 85, 0 });
        position += (thirdBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition thirdPosition = new ReplayPosition(segmentId, position);

        long tuncationPoint = position - (CommitLogSegment.CHECKSUM_SIZE + 2);

        DatabaseEngine databaseEngine = EasyMock.createMock(DatabaseEngine.class);

        databaseEngine.replay(firstPosition, firstBuffer.duplicate());
        databaseEngine.replay(secondPosition, secondBuffer.duplicate());

        EasyMock.replay(databaseEngine);

        Path path;

        try (CommitLogSegment segment = CommitLogSegment.freshSegment(this.configuration)) {

            path = segment.getPath();

            assertEquals(firstPosition, segment.write(firstBuffer));
            assertEquals(secondPosition, segment.write(secondBuffer));
            assertEquals(thirdPosition, segment.write(thirdBuffer));

            segment.flush();
        }

        try (RandomAccessFile file = FileUtils.openRandomAccessFile(path)) {

            FileUtils.extendsOrTruncate(file, tuncationPoint);
        }

        try (CommitLogSegment segment = CommitLogSegment.loadFromFile(this.configuration, path)) {

            segment.replay(databaseEngine);
        }

        EasyMock.verify(databaseEngine);
    }

    /**
     * Test the replay when the data or the CRC of the data of the entry has not been written to the disk properly.
     * 
     * @throws Exception if a problem occurs.
     */
    @Test
    public void testReplayWithCorruptedData() throws Exception {

        long segmentId = IdFactory.nextId() + 1;
        int position = 0;

        Buffer firstBuffer = Buffers.wrap(new byte[] { 1, 123, 12, 37 });
        position += (firstBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition firstPosition = new ReplayPosition(segmentId, position);

        Buffer secondBuffer = Buffers.wrap(new byte[] { -121, 5, 0, 30, 14, 56 });
        position += (secondBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition secondPosition = new ReplayPosition(segmentId, position);

        Buffer thirdBuffer = Buffers.wrap(new byte[] { 4, 85, 0 });
        position += (thirdBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition thirdPosition = new ReplayPosition(segmentId, position);

        DatabaseEngine databaseEngine = EasyMock.createMock(DatabaseEngine.class);

        databaseEngine.replay(firstPosition, firstBuffer.duplicate());
        databaseEngine.replay(secondPosition, secondBuffer.duplicate());

        EasyMock.replay(databaseEngine);

        Path path;

        try (CommitLogSegment segment = CommitLogSegment.freshSegment(this.configuration)) {

            path = segment.getPath();

            assertEquals(firstPosition, segment.write(firstBuffer));
            assertEquals(secondPosition, segment.write(secondBuffer));
            assertEquals(thirdPosition, segment.write(thirdBuffer));

            segment.flush();
        }

        try (RandomAccessFile file = FileUtils.openRandomAccessFile(path)) {

            file.seek(position - CommitLogSegment.CHECKSUM_SIZE);
            file.writeLong(1);
        }

        try (CommitLogSegment segment = CommitLogSegment.loadFromFile(this.configuration, path)) {

            segment.replay(databaseEngine);
        }

        EasyMock.verify(databaseEngine);
    }

    /**
     * Test the replay when the length or the CRC of the length of the entry has not been written to the disk properly.
     * 
     * @throws Exception if a problem occurs.
     */
    @Test
    public void testReplayWithCorruptedLenght() throws Exception {

        long segmentId = IdFactory.nextId() + 1;
        int position = 0;

        Buffer firstBuffer = Buffers.wrap(new byte[] { 1, 123, 12, 37 });
        position += (firstBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition firstPosition = new ReplayPosition(segmentId, position);

        Buffer secondBuffer = Buffers.wrap(new byte[] { -121, 5, 0, 30, 14, 56 });
        position += (secondBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition secondPosition = new ReplayPosition(segmentId, position);

        Buffer thirdBuffer = Buffers.wrap(new byte[] { 4, 85, 0 });
        position += (thirdBuffer.readableBytes() + LOG_OVERHEAD_SIZE);
        ReplayPosition thirdPosition = new ReplayPosition(segmentId, position);

        DatabaseEngine databaseEngine = EasyMock.createMock(DatabaseEngine.class);

        databaseEngine.replay(firstPosition, firstBuffer.duplicate());
        databaseEngine.replay(secondPosition, secondBuffer.duplicate());

        EasyMock.replay(databaseEngine);

        Path path;

        try (CommitLogSegment segment = CommitLogSegment.freshSegment(this.configuration)) {

            path = segment.getPath();

            assertEquals(firstPosition, segment.write(firstBuffer));
            assertEquals(secondPosition, segment.write(secondBuffer));
            assertEquals(thirdPosition, segment.write(thirdBuffer));

            segment.flush();
        }

        try (RandomAccessFile file = FileUtils.openRandomAccessFile(path)) {

            file.seek(secondPosition.getPosition());
            file.writeInt(1);
        }

        try (CommitLogSegment segment = CommitLogSegment.loadFromFile(this.configuration, path)) {

            segment.replay(databaseEngine);
        }

        EasyMock.verify(databaseEngine);
    }

}
