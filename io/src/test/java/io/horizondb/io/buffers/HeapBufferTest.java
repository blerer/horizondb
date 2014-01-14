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

import java.io.IOException;

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
public class HeapBufferTest {

	@Test
	public void testWrap() {
		
		byte[] array = new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6};
		
		HeapBuffer buffer = new HeapBuffer(array);
		
		assertTrue(buffer.isReadable());
		assertFalse(buffer.isWriteable());
		assertEquals(10, buffer.readableBytes());
		assertEquals(0, buffer.writeableBytes());
		assertEquals(10, buffer.capacity());
		
		assertTrue(buffer.hasArray());
		assertEquals(0, buffer.arrayOffset());
		assertArrayEquals(array, buffer.array());
	}
	
	@Test
	public void testDuplicateSlice() throws IOException {
		
		byte[] array = new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6};
		
		HeapBuffer buffer = new HeapBuffer(array);
		
		Buffer slice = buffer.readerIndex(3).slice(2);
		
		ReadableBuffer duplicate = slice.duplicate();
				
		assertEquals(2, duplicate.readableBytes());
		assertEquals(0, duplicate.readerIndex());
		assertEquals(0, duplicate.readByte());
		assertEquals(0, duplicate.readByte());
	}
	
	@Test
	public void testAllocate() {
		
		HeapBuffer buffer = new HeapBuffer(10);
		
		assertFalse(buffer.isReadable());
		assertTrue(buffer.isWriteable());
		assertEquals(0, buffer.readableBytes());
		assertEquals(10, buffer.writeableBytes());
		assertEquals(10, buffer.capacity());
		
		assertTrue(buffer.hasArray());
		assertEquals(0, buffer.arrayOffset());
	}
	
	@Test
	public void testWrite() {

		HeapBuffer buffer = new HeapBuffer(10);
		
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
		
		assertArrayEquals(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6}, buffer.array());
		assertEquals(0, buffer.readerIndex());
		assertEquals(10, buffer.writerIndex());
	}
	
	@Test
	public void testWriteWithinSlice() {

		HeapBuffer buffer = new HeapBuffer(10);
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
		assertArrayEquals(new byte[]{2, -120, 0, 0, 3, 0, 0, 0, 0, 0}, buffer.array());
				
		slice = buffer.slice(3);
		slice.writerIndex(0);
		
		slice.writeByte(4);
		slice.writeByte(5);
		slice.writeByte(6);
		
		assertFalse(slice.isWriteable());
		assertTrue(slice.isReadable());
		assertEquals(0, slice.writeableBytes());
		assertEquals(3, slice.readableBytes());
		assertArrayEquals(new byte[]{2, -120, 0, 0, 3, 4, 5, 6, 0, 0}, buffer.array());
	}
	
	@Test
	public void testSliceDuplicate() {

		byte[] array = new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6};
		
		HeapBuffer buffer = new HeapBuffer(array);
				
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
	public void testToStringWithSlice() {

		HeapBuffer buffer = new HeapBuffer(10);
		buffer.writerIndex(10);
		
		Buffer slice = buffer.slice(5);
		slice.writerIndex(0);
		
		slice.writeByte(2);
		slice.writeByte(-120);
		slice.writeZeroBytes(2);
		slice.writeByte(3);
		
		assertEquals("HeapBuffer[offset=0,length=5,readerIndex=0,writerIndex=5,bytes={2,-120,0,0,3}]", slice.toString());

		slice = buffer.slice(3);
		slice.writerIndex(0);
		
		slice.writeByte(4);
		slice.writeByte(5);
		slice.writeByte(6);
		
		assertEquals("HeapBuffer[offset=5,length=3,readerIndex=0,writerIndex=3,bytes={4,5,6}]", slice.toString());

		assertArrayEquals(new byte[]{2, -120, 0, 0, 3, 4, 5, 6, 0, 0}, buffer.array());
		assertEquals("HeapBuffer[offset=0,length=10,readerIndex=8,writerIndex=10,bytes={0,0}]", buffer.toString());
		buffer.readerIndex(0);
		assertEquals("HeapBuffer[offset=0,length=10,readerIndex=0,writerIndex=10," +
				"bytes={2,-120,0,0,3,4,5,6,0,0}]", buffer.toString());
	}
	
	@Test
	public void testRead() {

		byte[] array = new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6};
		
		HeapBuffer buffer = new HeapBuffer(array);

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
	public void testGet() {

		byte[] array = new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6};
		
		HeapBuffer buffer = new HeapBuffer(array);

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
	public void testReadSlice() throws IOException {

		byte[] array = new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6};
		
		HeapBuffer buffer = new HeapBuffer(array);

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
	public void testSliceWithFullBuffer() throws IOException {

		byte[] array = new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6};
		
		HeapBuffer buffer = new HeapBuffer(array);

		ByteReader slice = buffer.slice(buffer.readableBytes());

		assertEquals(2, slice.readByte());
		assertTrue(slice.isReadable());
		assertEquals(-120, slice.readByte());
		
	}	
	
	@Test
	public void testReaderIndexRead() {

		byte[] array = new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6};
		
		HeapBuffer buffer = new HeapBuffer(array);

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
	public void testGetMultipleBytes() {

		byte[] array = new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6};
		
		HeapBuffer buffer = new HeapBuffer(array);

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
}
