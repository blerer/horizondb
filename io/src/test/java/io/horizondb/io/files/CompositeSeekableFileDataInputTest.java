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
package io.horizondb.io.files;

import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;

/**
 * @author Benjamin
 *
 */
public class CompositeSeekableFileDataInputTest {
	
	@Test
	public void testRead() throws IOException {

		try (CompositeSeekableFileDataInput input = new CompositeSeekableFileDataInput()) {
		
			input.add(toInput(new byte[]{2, -120, 0, 0, 0}));
			input.add(toInput(new byte[]{4, 5, 6}));
			input.add(toInput(new byte[]{7, 6}));
			
			assertTrue(input.isReadable());
			assertEquals(2, input.readByte());
			assertTrue(input.isReadable());
			
			assertEquals(-120, input.readByte());
			assertTrue(input.isReadable());
			
			assertEquals(0, input.readByte());
			assertTrue(input.isReadable());
			
			assertEquals(0, input.readByte());
			assertTrue(input.isReadable());
			
			assertEquals(0, input.readByte());
			assertTrue(input.isReadable());
			
			assertEquals(4, input.readByte());
			assertTrue(input.isReadable());
			
			assertEquals(5, input.readByte());
			assertTrue(input.isReadable());
			
			assertEquals(6, input.readByte());
			assertTrue(input.isReadable());
			
			assertEquals(7, input.readByte());
			assertTrue(input.isReadable());
			
			assertEquals(6, input.readByte());
			assertFalse(input.isReadable());
		}
	}

	/**
	 * @return
	 */
    private static SeekableFileDataInput toInput(byte[] bytes) {
	    return SeekableFileDataInputs.toSeekableFileDataInput(Buffers.wrap(bytes));
    }	

	
	@Test
	public void testSkip() throws IOException {

		try (CompositeSeekableFileDataInput input = new CompositeSeekableFileDataInput()) {

			input.add(toInput(new byte[] { 2, -120, 0, 0, 0 }));
			input.add(toInput(new byte[] { 4, 5, 6 }));
			input.add(toInput(new byte[] { 7, 6 }));

			assertEquals(10, input.readableBytes());

			input.skipBytes(1);
			assertTrue(input.isReadable());
			assertEquals(9, input.readableBytes());

			input.skipBytes(4);
			assertTrue(input.isReadable());
			assertEquals(5, input.readableBytes());

			assertEquals(4, input.readByte());
			assertTrue(input.isReadable());
			assertEquals(4, input.readableBytes());

			input.skipBytes(3);
			assertTrue(input.isReadable());
			assertEquals(1, input.readableBytes());

			assertEquals(6, input.readByte());
			assertFalse(input.isReadable());
			assertEquals(0, input.readableBytes());
		}
	}	
	
	@Test
	public void testSkipOverMultipleInputs() throws IOException {

		try (CompositeSeekableFileDataInput buffer = new CompositeSeekableFileDataInput()) {
		
			buffer.add(toInput(new byte[] { 2, -120, 0, 0, 0 }));
			buffer.add(toInput(new byte[] { 4, 5, 6 }));
			buffer.add(toInput(new byte[] { 7, 6 }));

			assertEquals(0, buffer.getPosition());
			assertEquals(10, buffer.readableBytes());

			buffer.skipBytes(9);
			assertEquals(9, buffer.getPosition());
			assertEquals(1, buffer.readableBytes());

			assertEquals(6, buffer.readByte());
			assertFalse(buffer.isReadable());
			assertEquals(10, buffer.getPosition());
			assertEquals(0, buffer.readableBytes());
		}
	}	
	
	@Test
	public void testReadBytes() throws IOException {

		try (CompositeSeekableFileDataInput input = new CompositeSeekableFileDataInput()) {

			input.add(toInput(new byte[] { 2, -120, 0, 0, 0 }));
			input.add(toInput(new byte[] { 4, 5, 6 }));
			input.add(toInput(new byte[] { 7, 6 }));

			assertEquals(0, input.getPosition());
			assertEquals(10, input.readableBytes());

			byte[] bytes = new byte[4];

			input.readBytes(bytes, 0, 4);

			assertArrayEquals(new byte[] { 2, -120, 0, 0 }, bytes);
			assertTrue(input.isReadable());
			assertEquals(4, input.getPosition());
			assertEquals(6, input.readableBytes());

			input.readBytes(bytes);

			assertArrayEquals(new byte[] { 0, 4, 5, 6 }, bytes);
			assertTrue(input.isReadable());
			assertEquals(8, input.getPosition());
			assertEquals(2, input.readableBytes());

			input.readBytes(bytes, 1, 2);

			assertArrayEquals(new byte[] { 0, 7, 6, 6 }, bytes);
			assertFalse(input.isReadable());
			assertEquals(10, input.getPosition());
			assertEquals(0, input.readableBytes());
		}
	}	
	
	@Test
	public void testReadBytesWithMultipleInputReadAtOnce() throws IOException {

		try (CompositeSeekableFileDataInput input = new CompositeSeekableFileDataInput()) {

			input.add(toInput(new byte[] { 2, -120, 0, 0, 0 }));
			input.add(toInput(new byte[] { 4, 5, 6 }));
			input.add(toInput(new byte[] { 7, 6 }));

			assertEquals(0, input.getPosition());
			assertEquals(10, input.readableBytes());

			byte[] bytes = new byte[10];

			input.readBytes(bytes);

			assertArrayEquals(new byte[] { 2, -120, 0, 0, 0, 4, 5, 6, 7, 6 }, bytes);
			assertEquals(10, input.getPosition());
			assertEquals(0, input.readableBytes());
		}
	}	
	
	@Test
	public void testSlice() throws IOException {

		try (CompositeSeekableFileDataInput input = new CompositeSeekableFileDataInput()) {

			input.add(toInput(new byte[] { 2, -120, 0, 0, 0 }));
			input.add(toInput(new byte[] { 4, 5, 6 }));
			input.add(toInput(new byte[] { 7, 6 }));

			assertEquals(0, input.getPosition());
			assertEquals(10, input.readableBytes());

			assertEquals(2, input.readByte());
			assertTrue(input.isReadable());
			assertEquals(1, input.getPosition());
			assertEquals(9, input.readableBytes());

			ReadableBuffer slice = input.slice(8);

			assertTrue(input.isReadable());
			assertEquals(9, input.getPosition());
			assertEquals(1, input.readableBytes());

			assertTrue(slice.isReadable());
			assertEquals(0, slice.readerIndex());
			assertEquals(8, slice.readableBytes());

			assertEquals(-120, slice.readByte());
			assertTrue(slice.isReadable());
			assertEquals(1, slice.readerIndex());
			assertEquals(7, slice.readableBytes());

			assertEquals(0, slice.readByte());
			assertTrue(slice.isReadable());
			assertEquals(2, slice.readerIndex());
			assertEquals(6, slice.readableBytes());

			slice.skipBytes(3);
			assertTrue(slice.isReadable());
			assertEquals(5, slice.readerIndex());
			assertEquals(3, slice.readableBytes());

			assertEquals(5, slice.readByte());
			assertTrue(slice.isReadable());
			assertEquals(6, slice.readerIndex());
			assertEquals(2, slice.readableBytes());

			byte[] bytes = new byte[4];
			slice.readBytes(bytes, 2, 2);

			assertArrayEquals(new byte[] { 0, 0, 6, 7 }, bytes);
			assertFalse(slice.isReadable());
			assertEquals(8, slice.readerIndex());
			assertEquals(0, slice.readableBytes());

			try {
				slice.readByte();
				Assert.fail();
			} catch (IndexOutOfBoundsException e) {
				assertTrue(true);
			}
		}
	}
	
	@Test
	public void testSliceWithMatchingInputEnd() throws IOException {

		try (CompositeSeekableFileDataInput input = new CompositeSeekableFileDataInput()) {

			input.add(toInput(new byte[] { 2, -120, 0, 0, 0 }));
			input.add(toInput(new byte[] { 4, 5, 6 }));
			input.add(toInput(new byte[] { 7, 6 }));

			assertEquals(0, input.getPosition());
			assertEquals(10, input.readableBytes());

			assertEquals(2, input.readByte());
			assertTrue(input.isReadable());
			assertEquals(1, input.getPosition());
			assertEquals(9, input.readableBytes());

			ReadableBuffer slice = input.slice(4);
			assertEquals(-120, slice.readByte());
			slice = input.slice(3);
			assertEquals(4, slice.readByte());
			slice = input.slice(2);
			assertEquals(7, slice.readByte());
			assertFalse(input.isReadable());
		}
	}
	
	@Test
	public void testSeek() throws IOException {

		try (CompositeSeekableFileDataInput buffer = new CompositeSeekableFileDataInput()) {
		
			buffer.add(toInput(new byte[] { 2, -120, 0, 0, 0 }));
			buffer.add(toInput(new byte[] { 4, 5, 6 }));
			buffer.add(toInput(new byte[] { 7, 6 }));

			assertEquals(0, buffer.getPosition());
			assertEquals(10, buffer.readableBytes());

			buffer.skipBytes(9);
			assertEquals(9, buffer.getPosition());
			assertEquals(1, buffer.readableBytes());
			
			buffer.seek(0);
			assertEquals(0, buffer.getPosition());
			assertEquals(10, buffer.readableBytes());

			assertEquals(2, buffer.readByte());
			
			buffer.seek(6);
			assertEquals(6, buffer.getPosition());
			assertEquals(4, buffer.readableBytes());
			assertEquals(5, buffer.readByte());
			
			buffer.seek(10);
			assertFalse(buffer.isReadable());
			assertEquals(10, buffer.getPosition());
			assertEquals(0, buffer.readableBytes());
		}
	}
}
