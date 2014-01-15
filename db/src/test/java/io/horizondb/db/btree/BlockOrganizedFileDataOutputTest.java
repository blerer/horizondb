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

import io.horizondb.db.btree.BlockOrganizedFileDataOutput;
import io.horizondb.io.files.FileDataOutput;

import java.io.IOException;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

/**
 * @author Benjamin
 * 
 */
public class BlockOrganizedFileDataOutputTest {

    /**
     * The decorated output.
     */
    private FileDataOutput mockOutput;

    @Before
    public void setUp() {

        this.mockOutput = EasyMock.createMock(FileDataOutput.class);
    }

    @After
    public void tearDown() {

        this.mockOutput = null;
    }

    @SuppressWarnings("boxing")
    @Test
    public void testWriteByte() throws IOException {

        expect(this.mockOutput.getPosition()).andReturn(0L).times(3);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeByte(1)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(2L).times(2);
        expect(this.mockOutput.writeByte(2)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(3L).times(2);
        expect(this.mockOutput.writeByte(3)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(4L).times(2);
        expect(this.mockOutput.writeByte(4)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(5L).times(2);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeByte(5)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(7L).times(2);
        expect(this.mockOutput.writeByte(6)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(8L);
        this.mockOutput.close();

        EasyMock.replay(this.mockOutput);

        try (BlockOrganizedFileDataOutput output = new BlockOrganizedFileDataOutput(5, this.mockOutput)) {

            output.writeByte(1);
            assertEquals(1, output.getPosition());
            output.writeByte(2);
            assertEquals(2, output.getPosition());
            output.writeByte(3);
            assertEquals(3, output.getPosition());
            output.writeByte(4);
            assertEquals(4, output.getPosition());
            output.writeByte(5);
            assertEquals(5, output.getPosition());
            output.writeByte(6);
            assertEquals(6, output.getPosition());
        }
        EasyMock.verify(this.mockOutput);
    }

    @SuppressWarnings("boxing")
    @Test
    public void testWriteByteWithExistingBlocks() throws IOException {

        expect(this.mockOutput.getPosition()).andReturn(10L).times(3);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeByte(1)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(12L).times(2);
        expect(this.mockOutput.writeByte(2)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(13L).times(2);
        expect(this.mockOutput.writeByte(3)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(14L).times(2);
        expect(this.mockOutput.writeByte(4)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(15L).times(2);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeByte(5)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(17L).times(2);
        expect(this.mockOutput.writeByte(6)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(18L);
        this.mockOutput.close();

        EasyMock.replay(this.mockOutput);

        try (BlockOrganizedFileDataOutput output = new BlockOrganizedFileDataOutput(5, this.mockOutput)) {

            output.writeByte(1);
            assertEquals(9, output.getPosition());
            output.writeByte(2);
            assertEquals(10, output.getPosition());
            output.writeByte(3);
            assertEquals(11, output.getPosition());
            output.writeByte(4);
            assertEquals(12, output.getPosition());
            output.writeByte(5);
            assertEquals(13, output.getPosition());
            output.writeByte(6);
            assertEquals(14, output.getPosition());
        }
        EasyMock.verify(this.mockOutput);
    }

    @SuppressWarnings("boxing")
    @Test
    public void testWriteByteWithAnExistingPartialBlock() throws IOException {

        expect(this.mockOutput.getPosition()).andReturn(7L).times(2);
        expect(this.mockOutput.writeZeroBytes(3)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(10L);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeByte(1)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(12L).times(2);
        expect(this.mockOutput.writeByte(2)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(13L).times(2);
        expect(this.mockOutput.writeByte(3)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(14L).times(2);
        expect(this.mockOutput.writeByte(4)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(15L).times(2);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeByte(5)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(17L).times(2);
        expect(this.mockOutput.writeByte(6)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(18L);
        this.mockOutput.close();

        EasyMock.replay(this.mockOutput);

        try (BlockOrganizedFileDataOutput output = new BlockOrganizedFileDataOutput(5, this.mockOutput)) {

            output.writeByte(1);
            assertEquals(9, output.getPosition());
            output.writeByte(2);
            assertEquals(10, output.getPosition());
            output.writeByte(3);
            assertEquals(11, output.getPosition());
            output.writeByte(4);
            assertEquals(12, output.getPosition());
            output.writeByte(5);
            assertEquals(13, output.getPosition());
            output.writeByte(6);
            assertEquals(14, output.getPosition());
        }
        EasyMock.verify(this.mockOutput);
    }

    @SuppressWarnings("boxing")
    @Test
    public void testSwitchBlock() throws IOException {

        expect(this.mockOutput.getPosition()).andReturn(0L).times(3);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeByte(1)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(2L).times(2);
        expect(this.mockOutput.writeByte(2)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(3L).times(2);
        expect(this.mockOutput.writeByte(3)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(4L).times(2);
        expect(this.mockOutput.writeZeroBytes(1)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(5L).times(2);
        expect(this.mockOutput.writeByte(1)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeByte(4)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(7L).times(2);
        expect(this.mockOutput.writeByte(5)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(8L);
        this.mockOutput.close();

        EasyMock.replay(this.mockOutput);

        try (BlockOrganizedFileDataOutput output = new BlockOrganizedFileDataOutput(5, this.mockOutput)) {

            output.writeByte(1);
            assertEquals(1, output.getPosition());
            output.writeByte(2);
            assertEquals(2, output.getPosition());
            output.writeByte(3);
            assertEquals(3, output.getPosition());
            output.switchBlockType();
            assertEquals(4, output.getPosition());
            output.writeByte(4);
            assertEquals(5, output.getPosition());
            output.writeByte(5);
            assertEquals(6, output.getPosition());
        }
        EasyMock.verify(this.mockOutput);
    }

    @SuppressWarnings("boxing")
    @Test
    public void testWriteBytes() throws IOException {

        expect(this.mockOutput.getPosition()).andReturn(0L).times(3);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeBytes(aryEq(new byte[] { 1, 2, 3 }), eq(0), eq(3))).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(4L).times(2);
        expect(this.mockOutput.writeBytes(aryEq(new byte[] { 4, 5, 6, 7 }), eq(0), eq(1))).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(5L);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeBytes(aryEq(new byte[] { 4, 5, 6, 7 }), eq(1), eq(3))).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(9L);
        this.mockOutput.close();

        EasyMock.replay(this.mockOutput);

        try (BlockOrganizedFileDataOutput output = new BlockOrganizedFileDataOutput(5, this.mockOutput)) {

            output.writeBytes(new byte[] { 1, 2, 3 });
            assertEquals(3, output.getPosition());
            output.writeBytes(new byte[] { 4, 5, 6, 7 });
            assertEquals(7, output.getPosition());
        }
        EasyMock.verify(this.mockOutput);
    }

    @SuppressWarnings("boxing")
    @Test
    public void testWriteBytesWithMultipleBlocks() throws IOException {

        expect(this.mockOutput.getPosition()).andReturn(0L).times(3);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeBytes(aryEq(new byte[] { 1, 2, 3 }), eq(0), eq(3))).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(4L).times(2);
        expect(this.mockOutput.writeBytes(aryEq(new byte[] { 4, 5, 6, 7, 8, 9 }), eq(0), eq(1))).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(5L);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeBytes(aryEq(new byte[] { 4, 5, 6, 7, 8, 9 }), eq(1), eq(4))).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(10L);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeBytes(aryEq(new byte[] { 4, 5, 6, 7, 8, 9 }), eq(5), eq(1))).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(12L);
        this.mockOutput.close();

        EasyMock.replay(this.mockOutput);

        try (BlockOrganizedFileDataOutput output = new BlockOrganizedFileDataOutput(5, this.mockOutput)) {

            output.writeBytes(new byte[] { 1, 2, 3 });
            assertEquals(3, output.getPosition());
            output.writeBytes(new byte[] { 4, 5, 6, 7, 8, 9 });
            assertEquals(9, output.getPosition());
        }
        EasyMock.verify(this.mockOutput);
    }

    @SuppressWarnings("boxing")
    @Test
    public void testWriteBytesWithOffsetAndLength() throws IOException {

        expect(this.mockOutput.getPosition()).andReturn(0L).times(3);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeBytes(aryEq(new byte[] { 1, 2, 3 }), eq(0), eq(2))).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(3L).times(2);
        expect(this.mockOutput.writeBytes(aryEq(new byte[] { 4, 5, 6, 7 }), eq(1), eq(2))).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(5L);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeBytes(aryEq(new byte[] { 4, 5, 6, 7 }), eq(3), eq(1))).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(7L);
        this.mockOutput.close();

        EasyMock.replay(this.mockOutput);

        try (BlockOrganizedFileDataOutput output = new BlockOrganizedFileDataOutput(5, this.mockOutput)) {

            output.writeBytes(new byte[] { 1, 2, 3 }, 0, 2);
            assertEquals(2, output.getPosition());
            output.writeBytes(new byte[] { 4, 5, 6, 7 }, 1, 3);
            assertEquals(5, output.getPosition());
        }
        EasyMock.verify(this.mockOutput);
    }

    @SuppressWarnings("boxing")
    @Test
    public void testWriteBytesWithSameNumberOfBytes() throws IOException {

        expect(this.mockOutput.getPosition()).andReturn(0L).times(3);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeBytes(aryEq(new byte[] { 1, 2, 3, 4 }), eq(0), eq(4))).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(5L).times(2);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeBytes(aryEq(new byte[] { 5, 6, 7 }), eq(0), eq(3))).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(9L);
        this.mockOutput.close();

        EasyMock.replay(this.mockOutput);

        try (BlockOrganizedFileDataOutput output = new BlockOrganizedFileDataOutput(5, this.mockOutput)) {

            output.writeBytes(new byte[] { 1, 2, 3, 4 });
            assertEquals(4, output.getPosition());
            output.writeBytes(new byte[] { 5, 6, 7 });
            assertEquals(7, output.getPosition());
        }
        EasyMock.verify(this.mockOutput);
    }

    @SuppressWarnings("boxing")
    @Test
    public void testWriteZero() throws IOException {

        expect(this.mockOutput.getPosition()).andReturn(0L).times(3);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeZeroBytes(3)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(4L).times(2);
        expect(this.mockOutput.writeZeroBytes(1)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(5L);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeZeroBytes(4)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(10L);
        expect(this.mockOutput.writeByte(0)).andReturn(this.mockOutput);
        expect(this.mockOutput.writeZeroBytes(2)).andReturn(this.mockOutput);
        expect(this.mockOutput.getPosition()).andReturn(13L);
        this.mockOutput.close();

        EasyMock.replay(this.mockOutput);

        try (BlockOrganizedFileDataOutput output = new BlockOrganizedFileDataOutput(5, this.mockOutput)) {

            output.writeZeroBytes(3);
            assertEquals(3, output.getPosition());
            output.writeZeroBytes(7);
            assertEquals(10, output.getPosition());
        }
        EasyMock.verify(this.mockOutput);
    }
}
