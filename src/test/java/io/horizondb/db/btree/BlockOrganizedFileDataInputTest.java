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
package io.horizondb.db.btree;

import io.horizondb.db.btree.BlockOrganizedFileDataInput;
import io.horizondb.db.btree.BlockOrganizedFileDataOutput;
import io.horizondb.db.btree.BlockOrganizedReadableBuffer;
import io.horizondb.io.ByteReader;
import io.horizondb.io.files.DirectFileDataOutput;
import io.horizondb.io.files.DirectSeekableFileDataInput;
import io.horizondb.io.files.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Benjamin
 * 
 */
public class BlockOrganizedFileDataInputTest {

    /**
	 * 
	 */
    private static final int BLOCK_SIZE = 5;

    /**
     * The test directory.
     */
    private Path testDirectory;

    /**
     * The path to the file used during the tests.
     */
    private Path path;

    @Before
    public void setUp() throws IOException {

        this.testDirectory = Files.createTempDirectory(this.getClass().getSimpleName());
        this.path = this.testDirectory.resolve("test.md");
    }

    @After
    public void tearDown() throws IOException {

        FileUtils.forceDelete(this.testDirectory);
        this.path = null;
        this.testDirectory = null;
    }

    @Test
    public void testSeek() throws IOException {

        createFile();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 BLOCK_SIZE))) {

            assertTrue(input.isDataBlock());
            input.seek(21);
            assertEquals(21, input.getPosition());
            assertTrue(input.isHeaderBlock());

            input.seek(5);
            assertEquals(5, input.getPosition());
            assertTrue(input.isDataBlock());
        }
    }

    @Test
    public void testSeekOutsideFileBoundary() throws IOException {

        createFile();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 BLOCK_SIZE))) {

            try {
                input.seek(150);
                fail();
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
    }

    @Test
    public void testSkip() throws IOException {

        createFile();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 BLOCK_SIZE))) {

            assertEquals(0, input.getPosition());
            assertTrue(input.isDataBlock());

            input.skipBytes(1);
            input.skipBytes(1);
            input.skipBytes(1);
            input.skipBytes(1);

            assertEquals(4, input.getPosition());
            assertTrue(input.isDataBlock());

            input.skipBytes(20);

            assertEquals(24, input.getPosition());
            assertTrue(input.isHeaderBlock());
        }
    }

    @Test
    public void testReadByte() throws IOException {

        createFile();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 BLOCK_SIZE))) {

            assertEquals(0, input.getPosition());
            assertTrue(input.isDataBlock());

            assertEquals(0, input.readByte());
            assertEquals(1, input.readByte());
            assertEquals(2, input.readByte());
            assertEquals(3, input.readByte());

            assertEquals(4, input.getPosition());
            assertTrue(input.isDataBlock());

            assertEquals(4, input.readByte());
            assertEquals(5, input.readByte());
            assertEquals(6, input.readByte());
            assertEquals(0, input.readByte());

            assertEquals(8, input.getPosition());
            assertTrue(input.isDataBlock());

            assertEquals(10, input.readByte());
            assertEquals(9, input.getPosition());
            assertTrue(input.isHeaderBlock());
            assertEquals(11, input.readByte());
            assertTrue(input.isReadable());
        }
    }

    @Test
    public void testSize() throws IOException {

        createFile();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 BLOCK_SIZE))) {

            assertEquals(6 * (BLOCK_SIZE - 1), input.size());
        }
    }

    @Test
    public void testSizeWithEmptyFile() throws IOException {

        createEmptyFile();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 BLOCK_SIZE))) {

            assertEquals(0, input.size());
            assertTrue(input.isDataBlock());
        }
    }

    @Test
    public void testSizeWithPartialBlock() throws IOException {

        createFileWithUncompletedBlock();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 BLOCK_SIZE))) {

            assertEquals((6 * (BLOCK_SIZE - 1)) + 2, input.size());
        }
    }

    @Test
    public void testReadBytes() throws IOException {

        createFile();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 BLOCK_SIZE))) {

            assertEquals(0, input.getPosition());
            assertTrue(input.isDataBlock());

            byte[] bytes = new byte[6];

            input.readBytes(bytes, 0, 1);

            assertArrayEquals(new byte[] { 0, 0, 0, 0, 0, 0 }, bytes);
            assertTrue(input.isReadable());

            input.readBytes(bytes);

            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5, 6 }, bytes);
            assertTrue(input.isReadable());

            input.seek(14);

            input.readBytes(bytes);

            assertArrayEquals(new byte[] { 2, 8, 9, 0, 0, 0 }, bytes);
            assertTrue(input.isReadable());
        }
    }

    @Test
    public void testSlice() throws IOException {

        createFile();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 2 * BLOCK_SIZE))) {

            BlockOrganizedReadableBuffer slice = (BlockOrganizedReadableBuffer) input.slice(4);
            assertTrue(input.isDataBlock());
            assertTrue(slice.isDataBlock());
            assertEquals(4, slice.readableBytes());

            assertEquals(0, slice.getByte(0));
            assertEquals(1, slice.getByte(1));
            assertEquals(2, slice.getByte(2));
            assertEquals(3, slice.getByte(3));

            byte[] bytes = new byte[4];

            assertTrue(slice.isReadable());
            assertEquals(0, slice.readerIndex());
            slice.readBytes(bytes);
            assertEquals(0, slice.readableBytes());
            assertFalse(slice.isReadable());
            assertEquals(4, slice.readerIndex());

            assertArrayEquals(new byte[] { 0, 1, 2, 3 }, bytes);

            slice = (BlockOrganizedReadableBuffer) input.slice(6);
            assertEquals(6, slice.readableBytes());
            assertTrue(input.isHeaderBlock());
            assertTrue(slice.isDataBlock());
            assertEquals(0, slice.readerIndex());

            assertTrue(slice.isReadable());
            slice.readBytes(bytes);
            assertEquals(2, slice.readableBytes());
            assertTrue(slice.isReadable());
            assertEquals(4, slice.readerIndex());

            assertEquals(4, slice.getByte(0));
            assertEquals(5, slice.getByte(1));
            assertEquals(6, slice.getByte(2));
            assertEquals(0, slice.getByte(3));
            assertEquals(10, slice.getByte(4));
            assertEquals(11, slice.getByte(5));

            assertArrayEquals(new byte[] { 4, 5, 6, 0 }, bytes);

            slice.readBytes(bytes, 1, 2);
            assertEquals(0, slice.readableBytes());
            assertEquals(6, slice.readerIndex());
            assertArrayEquals(new byte[] { 4, 10, 11, 0 }, bytes);
            assertFalse(slice.isReadable());

            slice = (BlockOrganizedReadableBuffer) input.slice(2);
            assertTrue(input.isHeaderBlock());
            assertTrue(slice.isHeaderBlock());
            assertEquals(0, slice.readerIndex());
            assertEquals(2, slice.readableBytes());

            assertTrue(slice.isReadable());
            slice.readBytes(bytes, 1, 2);
            assertEquals(2, slice.readerIndex());
            assertEquals(0, slice.readableBytes());
            assertArrayEquals(new byte[] { 4, 12, 0, 0 }, bytes);
            assertFalse(slice.isReadable());
            assertTrue(slice.isHeaderBlock());

            slice = (BlockOrganizedReadableBuffer) input.slice(2);
            assertTrue(input.isDataBlock());
            assertTrue(slice.isHeaderBlock());
            assertEquals(0, slice.readerIndex());

            assertTrue(slice.isReadable());
            assertEquals(0, slice.readByte());
            assertEquals(1, slice.readerIndex());
            assertTrue(slice.isReadable());
            assertEquals(1, slice.readByte());
            assertEquals(2, slice.readerIndex());
            assertFalse(slice.isReadable());

            slice.readerIndex(0);

            assertTrue(input.isDataBlock());
            assertTrue(slice.isHeaderBlock());

            assertTrue(slice.isReadable());
            assertEquals(0, slice.readByte());
            assertTrue(slice.isReadable());
            assertEquals(1, slice.readByte());
            assertFalse(slice.isReadable());

            slice = (BlockOrganizedReadableBuffer) input.slice(8);
            ByteReader subSlice = slice.slice(4);

            assertTrue(subSlice.isReadable());
            assertEquals(2, subSlice.readByte());
            assertTrue(subSlice.isReadable());
            assertEquals(8, subSlice.readByte());
            assertTrue(subSlice.isReadable());

            subSlice.readBytes(bytes, 0, 2);
            assertArrayEquals(new byte[] { 9, 0, 0, 0 }, bytes);
        }
    }

    @Test
    public void testSliceGetWithMultipleBytes() throws IOException {

        createFile();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 2 * BLOCK_SIZE))) {

            BlockOrganizedReadableBuffer slice = (BlockOrganizedReadableBuffer) input.slice(4);
            assertTrue(input.isDataBlock());
            assertTrue(slice.isDataBlock());
            assertEquals(4, slice.readableBytes());

            byte[] bytes = new byte[4];

            slice.getBytes(0, bytes, 0, 4);

            assertArrayEquals(new byte[] { 0, 1, 2, 3 }, bytes);
            assertEquals(4, slice.readableBytes());

            slice.getBytes(1, bytes, 0, 2);

            assertArrayEquals(new byte[] { 1, 2, 2, 3 }, bytes);
            assertEquals(4, slice.readableBytes());

            slice = (BlockOrganizedReadableBuffer) input.slice(6);

            slice.getBytes(0, bytes, 0, 4);

            assertArrayEquals(new byte[] { 4, 5, 6, 0 }, bytes);
            assertEquals(6, slice.readableBytes());

            slice.getBytes(2, bytes, 0, 4);

            System.out.println(Arrays.toString(bytes));

            assertArrayEquals(new byte[] { 6, 0, 10, 11 }, bytes);
            assertEquals(6, slice.readableBytes());

        }
    }

    @Test
    public void testChangingIndexReaderWithinSlice() throws IOException {

        createFile();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 2 * BLOCK_SIZE))) {

            BlockOrganizedReadableBuffer slice = (BlockOrganizedReadableBuffer) input.slice(4);
            assertTrue(input.isDataBlock());
            assertTrue(slice.isDataBlock());

            byte[] bytes = new byte[4];

            assertTrue(slice.isReadable());
            assertEquals(0, slice.readerIndex());
            slice.readBytes(bytes);
            assertFalse(slice.isReadable());
            assertEquals(4, slice.readerIndex());

            assertArrayEquals(new byte[] { 0, 1, 2, 3 }, bytes);

            slice.readerIndex(2);

            assertTrue(slice.isDataBlock());
            assertTrue(slice.isReadable());
            assertEquals(2, slice.readerIndex());
            slice.readBytes(bytes, 0, 2);
            assertFalse(slice.isReadable());
            assertEquals(4, slice.readerIndex());

            assertArrayEquals(new byte[] { 2, 3, 2, 3 }, bytes);

            slice = (BlockOrganizedReadableBuffer) input.slice(6);
            assertTrue(slice.isDataBlock());
            assertEquals(0, slice.readerIndex());

            assertTrue(slice.isReadable());
            slice.readBytes(bytes);
            assertTrue(slice.isReadable());
            assertEquals(4, slice.readerIndex());

            assertArrayEquals(new byte[] { 4, 5, 6, 0 }, bytes);

            slice.readBytes(bytes, 1, 2);
            assertEquals(6, slice.readerIndex());
            assertArrayEquals(new byte[] { 4, 10, 11, 0 }, bytes);
            assertFalse(slice.isReadable());
            assertTrue(slice.isHeaderBlock());

            slice.readerIndex(2);
            assertTrue(slice.isDataBlock());
            slice.readBytes(bytes);

            assertArrayEquals(new byte[] { 6, 0, 10, 11 }, bytes);
            assertTrue(slice.isHeaderBlock());
            assertEquals(6, slice.readerIndex());
        }
    }

    @Test
    public void testSliceBlockType() throws IOException {

        createFile();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 2 * BLOCK_SIZE))) {

            BlockOrganizedReadableBuffer slice = (BlockOrganizedReadableBuffer) input.slice(4);
            assertTrue(input.isDataBlock());
            assertTrue(slice.isDataBlock());

            slice = (BlockOrganizedReadableBuffer) input.slice(6);
            assertTrue(input.isHeaderBlock());
            assertTrue(slice.isDataBlock());

            slice = (BlockOrganizedReadableBuffer) input.slice(2);
            assertTrue(input.isHeaderBlock());
            assertTrue(slice.isHeaderBlock());

            slice = (BlockOrganizedReadableBuffer) input.slice(2);
            assertTrue(input.isDataBlock());
            assertTrue(slice.isHeaderBlock());

            assertTrue(slice.isReadable());
            assertEquals(0, slice.readByte());
            assertTrue(slice.isReadable());
            assertEquals(1, slice.readByte());
            assertFalse(slice.isReadable());
        }
    }

    @Test
    public void testSeekHeader() throws IOException {

        createFile();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 2 * BLOCK_SIZE))) {

            assertTrue(input.seekHeader());
            assertEquals(20, input.readByte());
        }
    }

    @Test
    public void testSeekHeaderWithUncompletedBlock() throws IOException {

        createFileWithUncompletedBlock();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 2 * BLOCK_SIZE))) {

            assertTrue(input.seekHeader());
            assertEquals(20, input.readByte());
        }
    }

    @Test
    public void testSeekHeaderWithNoHeader() throws IOException {

        createFileWithNoHeader();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 2 * BLOCK_SIZE))) {

            assertFalse(input.seekHeader());
        }
    }

    @Test
    public void testSeekHeaderWithEmptyFile() throws IOException {

        createEmptyFile();

        try (BlockOrganizedFileDataInput input = new BlockOrganizedFileDataInput(BLOCK_SIZE,
                                                                                 new DirectSeekableFileDataInput(this.path,
                                                                                                                 2 * BLOCK_SIZE))) {

            assertFalse(input.seekHeader());
        }
    }

    /**
     * Creates the file used by the tests.
     * 
     * @throws IOException if a problem occurs while creating the file.
     */
    private void createFile() throws IOException {

        try (BlockOrganizedFileDataOutput output = new BlockOrganizedFileDataOutput(BLOCK_SIZE,
                                                                                    new DirectFileDataOutput(this.path))) {

            output.writeBytes(new byte[] { 0, 1, 2, 3, 4, 5, 6 });
            output.switchBlockType();
            output.writeBytes(new byte[] { 10, 11, 12 });
            output.switchBlockType();
            output.writeBytes(new byte[] { 0, 1, 2, 8, 9 });
            output.switchBlockType();
            output.writeBytes(new byte[] { 20, 21, 22 });
            output.switchBlockType();
            output.flush();
        }
    }

    /**
     * Creates the file used by the tests with an uncompleted block.
     * 
     * @throws IOException if a problem occurs while creating the file.
     */
    private void createFileWithUncompletedBlock() throws IOException {

        try (BlockOrganizedFileDataOutput output = new BlockOrganizedFileDataOutput(BLOCK_SIZE,
                                                                                    new DirectFileDataOutput(this.path))) {

            output.writeBytes(new byte[] { 0, 1, 2, 3, 4, 5, 6 });
            output.switchBlockType();
            output.writeBytes(new byte[] { 10, 11, 12 });
            output.switchBlockType();
            output.writeBytes(new byte[] { 0, 1, 2, 8, 9 });
            output.switchBlockType();
            output.writeBytes(new byte[] { 20, 21, 22 });
            output.switchBlockType();
            output.writeBytes(new byte[] { 30, 31 });
            output.flush();
        }
    }

    /**
     * Creates the file used by the tests.
     * 
     * @throws IOException if a problem occurs while creating the file.
     */
    private void createFileWithNoHeader() throws IOException {

        try (BlockOrganizedFileDataOutput output = new BlockOrganizedFileDataOutput(BLOCK_SIZE,
                                                                                    new DirectFileDataOutput(this.path))) {

            output.writeBytes(new byte[] { 0, 1, 2, 3, 4, 5, 6 });
            output.flush();
        }
    }

    private void createEmptyFile() throws IOException {

        try (BlockOrganizedFileDataOutput output = new BlockOrganizedFileDataOutput(BLOCK_SIZE,
                                                                                    new DirectFileDataOutput(this.path))) {
        }
    }
}
