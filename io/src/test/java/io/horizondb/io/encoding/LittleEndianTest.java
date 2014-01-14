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

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.*;

/**
 * @author Benjamin
 *
 */
public class LittleEndianTest {

	@Test
	public void testWriteShort() throws IOException {
	
		LittleEndian littleEndian = new LittleEndian();
		
		Buffer buffer = Buffers.allocate(10);
		littleEndian.writeShort(buffer, (short) 0);
		littleEndian.writeShort(buffer, (short) 3);
		littleEndian.writeShort(buffer, (short) -3);
		littleEndian.writeShort(buffer, Short.MAX_VALUE);
		littleEndian.writeShort(buffer, Short.MIN_VALUE);
		
		assertEquals(0, littleEndian.readShort(buffer));
		assertEquals(3, littleEndian.readShort(buffer));
		assertEquals(-3, littleEndian.readShort(buffer));
		assertEquals(Short.MAX_VALUE, littleEndian.readShort(buffer));
		assertEquals(Short.MIN_VALUE, littleEndian.readShort(buffer));
				
		ByteBuffer expected = ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN);
		expected.putShort((short) 0);
		expected.putShort((short) 3);
		expected.putShort((short) -3);
		expected.putShort(Short.MAX_VALUE);
		expected.putShort(Short.MIN_VALUE);
		
		assertArrayEquals(expected.array(), buffer.array());
	}

	@Test
	public void testSetShort() {
		
		LittleEndian littleEndian = new LittleEndian();
		
		Buffer buffer = Buffers.allocate(10);
		littleEndian.setShort(buffer, 0, (short) 0);
		littleEndian.setShort(buffer, 2, (short) 3);
		littleEndian.setShort(buffer, 4, (short) -3);
		littleEndian.setShort(buffer, 6, Short.MAX_VALUE);
		littleEndian.setShort(buffer, 8, Short.MIN_VALUE);
		
		assertEquals(0, littleEndian.getShort(buffer, 0));
		assertEquals(3, littleEndian.getShort(buffer, 2));
		assertEquals(-3, littleEndian.getShort(buffer, 4));
		assertEquals(Short.MAX_VALUE, littleEndian.getShort(buffer, 6));
		assertEquals(Short.MIN_VALUE, littleEndian.getShort(buffer, 8));
				
		ByteBuffer expected = ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN);
		expected.putShort((short) 0);
		expected.putShort((short) 3);
		expected.putShort((short) -3);
		expected.putShort(Short.MAX_VALUE);
		expected.putShort(Short.MIN_VALUE);
		
		assertArrayEquals(expected.array(), buffer.array());
	}

	@Test
	public void testWriteInt() throws IOException {
		
		LittleEndian littleEndian = new LittleEndian();
		
		Buffer buffer = Buffers.allocate(20);
		littleEndian.writeInt(buffer, 0);
		littleEndian.writeInt(buffer, 3);
		littleEndian.writeInt(buffer, -3);
		littleEndian.writeInt(buffer, Integer.MAX_VALUE);
		littleEndian.writeInt(buffer, Integer.MIN_VALUE);
		
		assertEquals(0, littleEndian.readInt(buffer));
		assertEquals(3, littleEndian.readInt(buffer));
		assertEquals(-3, littleEndian.readInt(buffer));
		assertEquals(Integer.MAX_VALUE, littleEndian.readInt(buffer));
		assertEquals(Integer.MIN_VALUE, littleEndian.readInt(buffer));
				
		ByteBuffer expected = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
		expected.putInt(0);
		expected.putInt(3);
		expected.putInt(-3);
		expected.putInt(Integer.MAX_VALUE);
		expected.putInt(Integer.MIN_VALUE);
		
		assertArrayEquals(expected.array(), buffer.array());
	}

	@Test
	public void testWriteUnsignedShort() throws IOException {
	
		LittleEndian littleEndian = new LittleEndian();
		
		Buffer buffer = Buffers.allocate(2);
		
		int unsignedShort = Short.MAX_VALUE + 10;
		
		littleEndian.writeUnsignedShort(buffer, unsignedShort);
		
		assertEquals(unsignedShort, littleEndian.readUnsignedShort(buffer));
	}
	
	@Test
	public void testSetUnsignedShort() {
	
		LittleEndian littleEndian = new LittleEndian();
		
		Buffer buffer = Buffers.allocate(2);
		
		int unsignedShort = Short.MAX_VALUE + 10;
		
		littleEndian.setUnsignedShort(buffer, 0, unsignedShort);
		
		assertEquals(unsignedShort, littleEndian.getUnsignedShort(buffer, 0));
	}

	@Test
	public void testWriteUnsignedInt() throws IOException {
	
		LittleEndian littleEndian = new LittleEndian();
		
		Buffer buffer = Buffers.allocate(4);
		
		long unsignedInt = Integer.MAX_VALUE + 10L;
		
		littleEndian.writeUnsignedInt(buffer, unsignedInt);
		
		assertEquals(unsignedInt, littleEndian.readUnsignedInt(buffer));
	}
	
	@Test
	public void testSetUnsignedInt() {
	
		LittleEndian littleEndian = new LittleEndian();
		
		Buffer buffer = Buffers.allocate(4);
		
		long unsignedInt = Integer.MAX_VALUE + 10L;
		
		littleEndian.setUnsignedInt(buffer, 0, unsignedInt);
		
		assertEquals(unsignedInt, littleEndian.getUnsignedInt(buffer, 0));
	}
	
	@Test
	public void testSetInt() {
		
		LittleEndian littleEndian = new LittleEndian();
		
		Buffer buffer = Buffers.allocate(20);
		littleEndian.setInt(buffer, 0, 0);
		littleEndian.setInt(buffer, 4, 3);
		littleEndian.setInt(buffer, 8, -3);
		littleEndian.setInt(buffer, 12, Integer.MAX_VALUE);
		littleEndian.setInt(buffer, 16, Integer.MIN_VALUE);
		
		assertEquals(0, littleEndian.getInt(buffer, 0));
		assertEquals(3, littleEndian.getInt(buffer, 4));
		assertEquals(-3, littleEndian.getInt(buffer, 8));
		assertEquals(Integer.MAX_VALUE, littleEndian.getInt(buffer, 12));
		assertEquals(Integer.MIN_VALUE, littleEndian.getInt(buffer, 16));
				
		ByteBuffer expected = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
		expected.putInt(0);
		expected.putInt(3);
		expected.putInt(-3);
		expected.putInt(Integer.MAX_VALUE);
		expected.putInt(Integer.MIN_VALUE);
		
		assertArrayEquals(expected.array(), buffer.array());
	}

	@Test
	public void testSetLong() {
		LittleEndian littleEndian = new LittleEndian();
		
		Buffer buffer = Buffers.allocate(40);
		littleEndian.setLong(buffer, 0, 0);
		littleEndian.setLong(buffer, 8, 3);
		littleEndian.setLong(buffer, 16, -3);
		littleEndian.setLong(buffer, 24, Long.MAX_VALUE);
		littleEndian.setLong(buffer, 32, Long.MIN_VALUE);
		
		assertEquals(0L, littleEndian.getLong(buffer, 0));
		assertEquals(3L, littleEndian.getLong(buffer, 8));
		assertEquals(-3L, littleEndian.getLong(buffer, 16));
		assertEquals(Long.MAX_VALUE, littleEndian.getLong(buffer, 24));
		assertEquals(Long.MIN_VALUE, littleEndian.getLong(buffer, 32));
				
		ByteBuffer expected = ByteBuffer.allocate(40).order(ByteOrder.LITTLE_ENDIAN);
		expected.putLong(0);
		expected.putLong(3);
		expected.putLong(-3);
		expected.putLong(Long.MAX_VALUE);
		expected.putLong(Long.MIN_VALUE);
		
		assertArrayEquals(expected.array(), buffer.array());
	}

	@Test
	public void testWriteLong() throws IOException {
		
		LittleEndian littleEndian = new LittleEndian();
		
		Buffer buffer = Buffers.allocate(40);
		littleEndian.writeLong(buffer, 0);
		littleEndian.writeLong(buffer, 3);
		littleEndian.writeLong(buffer, -3);
		littleEndian.writeLong(buffer, Long.MAX_VALUE);
		littleEndian.writeLong(buffer, Long.MIN_VALUE);
		
		assertEquals(0L, littleEndian.readLong(buffer));
		assertEquals(3L, littleEndian.readLong(buffer));
		assertEquals(-3L, littleEndian.readLong(buffer));
		assertEquals(Long.MAX_VALUE, littleEndian.readLong(buffer));
		assertEquals(Long.MIN_VALUE, littleEndian.readLong(buffer));
				
		ByteBuffer expected = ByteBuffer.allocate(40).order(ByteOrder.LITTLE_ENDIAN);
		expected.putLong(0);
		expected.putLong(3);
		expected.putLong(-3);
		expected.putLong(Long.MAX_VALUE);
		expected.putLong(Long.MIN_VALUE);
		
		assertArrayEquals(expected.array(), buffer.array());
	}
}
