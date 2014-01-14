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
package io.horizondb.io.encoding;

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Benjamin
 *
 */
public class BigEndianTest {

	@Test
	public void testWriteShort() throws IOException {
	
		BigEndian bigEndian = new BigEndian();
		
		Buffer buffer = Buffers.allocate(10);
		bigEndian.writeShort(buffer, (short) 0);
		bigEndian.writeShort(buffer, (short) 3);
		bigEndian.writeShort(buffer, (short) -3);
		bigEndian.writeShort(buffer, Short.MAX_VALUE);
		bigEndian.writeShort(buffer, Short.MIN_VALUE);
		
		assertEquals(0, bigEndian.readShort(buffer));
		assertEquals(3, bigEndian.readShort(buffer));
		assertEquals(-3, bigEndian.readShort(buffer));
		assertEquals(Short.MAX_VALUE, bigEndian.readShort(buffer));
		assertEquals(Short.MIN_VALUE, bigEndian.readShort(buffer));
				
		ByteBuffer expected = ByteBuffer.allocate(10).order(ByteOrder.BIG_ENDIAN);
		expected.putShort((short) 0);
		expected.putShort((short) 3);
		expected.putShort((short) -3);
		expected.putShort(Short.MAX_VALUE);
		expected.putShort(Short.MIN_VALUE);
		
		assertArrayEquals(expected.array(), buffer.array());
	}

	@Test
	public void testWriteUnsignedShort() throws IOException {
	
		BigEndian bigEndian = new BigEndian();
		
		Buffer buffer = Buffers.allocate(2);
		
		int unsignedShort = Short.MAX_VALUE + 10;
		
		bigEndian.writeUnsignedShort(buffer, unsignedShort);
		
		assertEquals(unsignedShort, bigEndian.readUnsignedShort(buffer));
	}
	
	@Test
	public void testSetUnsignedShort() {
	
		BigEndian bigEndian = new BigEndian();
		
		Buffer buffer = Buffers.allocate(2);
		
		int unsignedShort = Short.MAX_VALUE + 10;
		
		bigEndian.setUnsignedShort(buffer, 0, unsignedShort);
		
		assertEquals(unsignedShort, bigEndian.getUnsignedShort(buffer, 0));
	}

	@Test
	public void testWriteUnsignedInt() throws IOException {
	
		BigEndian bigEndian = new BigEndian();
		
		Buffer buffer = Buffers.allocate(4);
		
		long unsignedInt = Integer.MAX_VALUE + 10L;
		
		bigEndian.writeUnsignedInt(buffer, unsignedInt);
		
		assertEquals(unsignedInt, bigEndian.readUnsignedInt(buffer));
	}
	
	@Test
	public void testSetUnsignedInt() {
	
		BigEndian bigEndian = new BigEndian();
		
		Buffer buffer = Buffers.allocate(4);
		
		long unsignedInt = Integer.MAX_VALUE + 10L;
		
		bigEndian.setUnsignedInt(buffer, 0, unsignedInt);
		
		assertEquals(unsignedInt, bigEndian.getUnsignedInt(buffer, 0));
	}
	
	@Test
	public void testSetShort() {
		
		BigEndian bigEndian = new BigEndian();
		
		Buffer buffer = Buffers.allocate(10);
		bigEndian.setShort(buffer, 0, (short) 0);
		bigEndian.setShort(buffer, 2, (short) 3);
		bigEndian.setShort(buffer, 4, (short) -3);
		bigEndian.setShort(buffer, 6, Short.MAX_VALUE);
		bigEndian.setShort(buffer, 8, Short.MIN_VALUE);
		
		assertEquals(0, bigEndian.getShort(buffer, 0));
		assertEquals(3, bigEndian.getShort(buffer, 2));
		assertEquals(-3, bigEndian.getShort(buffer, 4));
		assertEquals(Short.MAX_VALUE, bigEndian.getShort(buffer, 6));
		assertEquals(Short.MIN_VALUE, bigEndian.getShort(buffer, 8));
				
		ByteBuffer expected = ByteBuffer.allocate(10).order(ByteOrder.BIG_ENDIAN);
		expected.putShort((short) 0);
		expected.putShort((short) 3);
		expected.putShort((short) -3);
		expected.putShort(Short.MAX_VALUE);
		expected.putShort(Short.MIN_VALUE);
		
		assertArrayEquals(expected.array(), buffer.array());
	}

	@Test
	public void testWriteInt() throws IOException {
		
		BigEndian bigEndian = new BigEndian();
		
		Buffer buffer = Buffers.allocate(20);
		bigEndian.writeInt(buffer, 0);
		bigEndian.writeInt(buffer, 3);
		bigEndian.writeInt(buffer, -3);
		bigEndian.writeInt(buffer, Integer.MAX_VALUE);
		bigEndian.writeInt(buffer, Integer.MIN_VALUE);
		
		assertEquals(0, bigEndian.readInt(buffer));
		assertEquals(3, bigEndian.readInt(buffer));
		assertEquals(-3, bigEndian.readInt(buffer));
		assertEquals(Integer.MAX_VALUE, bigEndian.readInt(buffer));
		assertEquals(Integer.MIN_VALUE, bigEndian.readInt(buffer));
				
		ByteBuffer expected = ByteBuffer.allocate(20).order(ByteOrder.BIG_ENDIAN);
		expected.putInt(0);
		expected.putInt(3);
		expected.putInt(-3);
		expected.putInt(Integer.MAX_VALUE);
		expected.putInt(Integer.MIN_VALUE);
		
		assertArrayEquals(expected.array(), buffer.array());
	}

	@Test
	public void testSetInt() {
		
		BigEndian bigEndian = new BigEndian();
		
		Buffer buffer = Buffers.allocate(20);
		bigEndian.setInt(buffer, 0, 0);
		bigEndian.setInt(buffer, 4, 3);
		bigEndian.setInt(buffer, 8, -3);
		bigEndian.setInt(buffer, 12, Integer.MAX_VALUE);
		bigEndian.setInt(buffer, 16, Integer.MIN_VALUE);
		
		assertEquals(0, bigEndian.getInt(buffer, 0));
		assertEquals(3, bigEndian.getInt(buffer, 4));
		assertEquals(-3, bigEndian.getInt(buffer, 8));
		assertEquals(Integer.MAX_VALUE, bigEndian.getInt(buffer, 12));
		assertEquals(Integer.MIN_VALUE, bigEndian.getInt(buffer, 16));
				
		ByteBuffer expected = ByteBuffer.allocate(20).order(ByteOrder.BIG_ENDIAN);
		expected.putInt(0);
		expected.putInt(3);
		expected.putInt(-3);
		expected.putInt(Integer.MAX_VALUE);
		expected.putInt(Integer.MIN_VALUE);
		
		assertArrayEquals(expected.array(), buffer.array());
	}

	@Test
	public void testSetLong() {
		BigEndian bigEndian = new BigEndian();
		
		Buffer buffer = Buffers.allocate(40);
		bigEndian.setLong(buffer, 0, 0);
		bigEndian.setLong(buffer, 8, 3);
		bigEndian.setLong(buffer, 16, -3);
		bigEndian.setLong(buffer, 24, Long.MAX_VALUE);
		bigEndian.setLong(buffer, 32, Long.MIN_VALUE);
		
		assertEquals(0L, bigEndian.getLong(buffer, 0));
		assertEquals(3L, bigEndian.getLong(buffer, 8));
		assertEquals(-3L, bigEndian.getLong(buffer, 16));
		assertEquals(Long.MAX_VALUE, bigEndian.getLong(buffer, 24));
		assertEquals(Long.MIN_VALUE, bigEndian.getLong(buffer, 32));
				
		ByteBuffer expected = ByteBuffer.allocate(40).order(ByteOrder.BIG_ENDIAN);
		expected.putLong(0);
		expected.putLong(3);
		expected.putLong(-3);
		expected.putLong(Long.MAX_VALUE);
		expected.putLong(Long.MIN_VALUE);
		
		assertArrayEquals(expected.array(), buffer.array());
	}

	@Test
	public void testWriteLong() throws IOException {
		
		BigEndian bigEndian = new BigEndian();
		
		Buffer buffer = Buffers.allocate(40);
		bigEndian.writeLong(buffer, 0);
		bigEndian.writeLong(buffer, 3);
		bigEndian.writeLong(buffer, -3);
		bigEndian.writeLong(buffer, Long.MAX_VALUE);
		bigEndian.writeLong(buffer, Long.MIN_VALUE);
		
		assertEquals(0L, bigEndian.readLong(buffer));
		assertEquals(3L, bigEndian.readLong(buffer));
		assertEquals(-3L, bigEndian.readLong(buffer));
		assertEquals(Long.MAX_VALUE, bigEndian.readLong(buffer));
		assertEquals(Long.MIN_VALUE, bigEndian.readLong(buffer));
				
		ByteBuffer expected = ByteBuffer.allocate(40).order(ByteOrder.BIG_ENDIAN);
		expected.putLong(0);
		expected.putLong(3);
		expected.putLong(-3);
		expected.putLong(Long.MAX_VALUE);
		expected.putLong(Long.MIN_VALUE);
		
		assertArrayEquals(expected.array(), buffer.array());
	}
}
