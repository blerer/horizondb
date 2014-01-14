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
package io.horizondb.io.buffers;

import io.horizondb.io.Buffer;
import io.horizondb.io.ByteReader;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;


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
public class DirectBufferTest {

	@Test
	public void testWrap() {
		
		ByteBuffer directBuffer = ByteBuffer.allocateDirect(10)
				                            .put(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6}); 
		
		DirectBuffer buffer = new DirectBuffer(directBuffer);
		
		assertTrue(buffer.isReadable());
		assertFalse(buffer.isWriteable());
		assertEquals(10, buffer.readableBytes());
		assertEquals(0, buffer.writeableBytes());
		assertEquals(10, buffer.capacity());
		
		assertFalse(buffer.hasArray());
		assertTrue(buffer.isDirect());
		
		byte[] actual = new byte[10];
		buffer.readBytes(actual);
		
		assertArrayEquals(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6}, actual);
	}
	
	@Test
	public void testDuplicateSlice() throws IOException {
		
		ByteBuffer directBuffer = ByteBuffer.allocateDirect(10)
                .put(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6}); 

		DirectBuffer buffer = new DirectBuffer(directBuffer);
		
		Buffer slice = buffer.readerIndex(3).slice(2);
		
		ReadableBuffer duplicate = slice.duplicate();
				
		assertEquals(2, duplicate.readableBytes());
		assertEquals(0, duplicate.readerIndex());
		assertEquals(0, duplicate.readByte());
		assertEquals(0, duplicate.readByte());
	}
	
	@Test
	public void testAllocate() {
		
		DirectBuffer buffer = new DirectBuffer(10);
		
		assertFalse(buffer.isReadable());
		assertTrue(buffer.isWriteable());
		assertEquals(0, buffer.readableBytes());
		assertEquals(10, buffer.writeableBytes());
		assertEquals(10, buffer.capacity());
		
		assertFalse(buffer.hasArray());
	}
	
	@Test
	public void testWrite() {

		DirectBuffer buffer = new DirectBuffer(10);
		
		buffer.writeByte(2);
		assertTrue(buffer.isWriteable());
		assertTrue(buffer.isReadable());
		assertEquals(1, buffer.readableBytes());
		assertEquals(9, buffer.writeableBytes());
				
		buffer.writeByte(-120);
		buffer.writeZeroBytes(3);
		buffer.writeBytes(new byte[]{4, 5, 6, 7});
		buffer.writeBytes(new byte[]{4, 5, 6, 7}, 2, 1);
		
		assertFalse(buffer.isWriteable());
		assertTrue(buffer.isReadable());
		assertEquals(buffer.writeableBytes(), 0);
		assertEquals(buffer.readableBytes(), 10);
		
		assertEquals(0, buffer.readerIndex());
		assertEquals(10, buffer.writerIndex());
		
		byte[] actual = new byte[10];
		buffer.readBytes(actual);

		assertArrayEquals(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6}, actual);
	}

	@Test
	public void testRead() {

		ByteBuffer directBuffer = ByteBuffer.allocateDirect(10)
                                            .put(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6}); 

		DirectBuffer buffer = new DirectBuffer(directBuffer);

		assertEquals(2, buffer.readByte());
		assertTrue(buffer.isReadable());
		assertEquals(-120, buffer.readByte());
		assertTrue(buffer.isReadable());
		buffer.skipBytes(3);
		assertTrue(buffer.isReadable());
		
		byte[] bytes = new byte[4];
		buffer.readBytes(bytes);
		assertArrayEquals(new byte[]{4, 5, 6, 7}, bytes);
		assertTrue(buffer.isReadable());
						
		buffer.readBytes(bytes, 0, 1);
		assertArrayEquals(new byte[]{6, 5, 6, 7}, bytes);
		assertFalse(buffer.isReadable());
	}	
	
	@Test
	public void testReaderIndexRead() {

		ByteBuffer directBuffer = ByteBuffer.allocateDirect(10)
                                            .put(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6}); 

		DirectBuffer buffer = new DirectBuffer(directBuffer);

		assertEquals(2, buffer.readByte());
		assertTrue(buffer.isReadable());
		assertEquals(-120, buffer.readByte());
		assertTrue(buffer.isReadable());
		buffer.skipBytes(3);
		assertTrue(buffer.isReadable());
		
		buffer.readerIndex(0);
		
		byte[] bytes = new byte[4];
		buffer.readBytes(bytes);
		assertArrayEquals(new byte[]{2, -120, 0, 0}, bytes);
		assertTrue(buffer.isReadable());
						
		buffer.readerIndex(8);
		
		buffer.readBytes(bytes, 0, 1);
		assertArrayEquals(new byte[]{7, -120, 0, 0}, bytes);
		assertTrue(buffer.isReadable());
		
		try {
			
			buffer.readerIndex(12);
			fail();
			
		} catch (IndexOutOfBoundsException e) {
			
			assertTrue(true); 
		}
	}
	
	@Test
	public void testGet() {

		ByteBuffer directBuffer = ByteBuffer.allocateDirect(10)
                                            .put(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6}); 

		DirectBuffer buffer = new DirectBuffer(directBuffer);

		assertEquals(2, buffer.getByte(0));
		assertEquals(-120, buffer.getByte(1));
		assertEquals(2, buffer.readByte());
		assertEquals(6, buffer.getByte(9));
		
		assertTrue(buffer.isReadable());
		
		try {
		
			 buffer.getByte(10);
			 fail();
			 
		} catch (IndexOutOfBoundsException e) {
			assertTrue(true);
		}
	}	
	
	@Test
	public void testGetMultipleBytes() {

		ByteBuffer directBuffer = ByteBuffer.allocateDirect(10)
                                            .put(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6}); 
		
		DirectBuffer buffer = new DirectBuffer(directBuffer);

		byte[] bytes = new byte[4];
		
		buffer.getBytes(0, bytes, 0, 4);
		assertArrayEquals(new byte[]{2, -120, 0, 0}, bytes);
		
		buffer.getBytes(1, bytes, 0, 4);
		assertArrayEquals(new byte[]{-120, 0, 0, 0}, bytes);
		assertEquals(2, buffer.readByte());
		buffer.getBytes(9, bytes, 3, 1);
		assertArrayEquals(new byte[]{-120, 0, 0, 6}, bytes);
		
		assertTrue(buffer.isReadable());
		
		try {
		
			buffer.getBytes(9, bytes, 3, 2);
			fail();
			 
		} catch (IndexOutOfBoundsException e) {
			assertTrue(true);
		}
	}	
	
	@Test
	public void testReadSlice() throws IOException {

		ByteBuffer directBuffer = ByteBuffer.allocateDirect(10)
                                            .put(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6}); 

		DirectBuffer buffer = new DirectBuffer(directBuffer);
		
		ByteReader slice = buffer.slice(2);
		
		assertEquals(2, slice.readByte());
		assertTrue(slice.isReadable());
		assertEquals(-120, slice.readByte());
		
		assertFalse(slice.isReadable());
		
		assertTrue(buffer.isReadable());
		slice = buffer.slice(8);

		assertFalse(buffer.isReadable());
		
		slice.skipBytes(3);
		assertTrue(slice.isReadable());
		
		byte[] bytes = new byte[4];
		slice.readBytes(bytes);
		assertArrayEquals(new byte[]{4, 5, 6, 7}, bytes);
		assertTrue(slice.isReadable());
						
		slice.readBytes(bytes, 0, 1);
		assertArrayEquals(new byte[]{6, 5, 6, 7}, bytes);
		assertFalse(slice.isReadable());
	}	
	
	@Test
	public void testSliceDuplicate() {

		ByteBuffer directBuffer = ByteBuffer.allocateDirect(10)
                .put(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6}); 

		DirectBuffer buffer = new DirectBuffer(directBuffer);
				
		Buffer slice = (Buffer) buffer.slice(5).duplicate();
		
		assertFalse(slice.isWriteable());
		assertTrue(slice.isReadable());
		assertEquals(0, slice.writeableBytes());
		assertEquals(5, slice.readableBytes());
		
		slice = (Buffer) buffer.slice(3).duplicate();
		
		assertFalse(slice.isWriteable());
		assertTrue(slice.isReadable());
		assertEquals(0, slice.writeableBytes());
		assertEquals(3, slice.readableBytes());
	}
	
	@Test
	public void testWriteWithinSlice() {

		DirectBuffer buffer = new DirectBuffer(10);
		buffer.writerIndex(10);
		
		Buffer slice = buffer.slice(5);
		slice.writerIndex(0);
		
		slice.writeByte(2);
		slice.writeByte(-120);
		slice.writeZeroBytes(2);
		slice.writeByte(3);
		
		assertFalse(slice.isWriteable());
		assertTrue(slice.isReadable());
		assertEquals(0, slice.writeableBytes());
		assertEquals(5, slice.readableBytes());
		
		byte[] bytes = new byte[10];
		buffer.getBytes(0, bytes);
				
		assertArrayEquals(new byte[]{2, -120, 0, 0, 3, 0, 0, 0, 0, 0}, bytes);
				
		slice = buffer.slice(3);
		slice.writerIndex(0);
		
		slice.writeByte(4);
		slice.writeByte(5);
		slice.writeByte(6);
		
		assertFalse(slice.isWriteable());
		assertTrue(slice.isReadable());
		assertEquals(0, slice.writeableBytes());
		assertEquals(3, slice.readableBytes());
		
		bytes = new byte[10];
		buffer.getBytes(0, bytes);
		
		assertArrayEquals(new byte[]{2, -120, 0, 0, 3, 4, 5, 6, 0, 0}, bytes);
	}
}
