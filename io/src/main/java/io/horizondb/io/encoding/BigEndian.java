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

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.WritableBuffer;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * @author Benjamin
 *
 */
final class BigEndian extends Endianness {

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public ByteOrder order() {
	    return ByteOrder.BIG_ENDIAN;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeShort(ByteWriter writer, short s) throws IOException {
		writer.writeByte((byte) (s >>> 8))
		      .writeByte((byte) s);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setShort(WritableBuffer buffer, int index, short s) {
		buffer.setByte(index, (byte) (s >>> 8))
		      .setByte(index + 1, s);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeInt(ByteWriter writer, int i) throws IOException {
		
		writer.writeByte((byte) (i >>> 24))
		      .writeByte((byte) (i >>> 16))
		      .writeByte((byte) (i >>> 8))
		      .writeByte((byte) i);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setInt(WritableBuffer buffer, int index, int i) {
		
		buffer.setByte(index, (byte) (i >>> 24))
		      .setByte(index + 1, (byte) (i >>> 16))
		      .setByte(index + 2, (byte) (i >>> 8))
		      .setByte(index + 3, (byte) i);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeLong(ByteWriter writer, long l) throws IOException {
		
		writer.writeByte((byte) (l >>> 56))
		      .writeByte((byte) (l >>> 48))
		      .writeByte((byte) (l >>> 40))
		      .writeByte((byte) (l >>> 32))
		      .writeByte((byte) (l >>> 24))
		      .writeByte((byte) (l >>> 16))
		      .writeByte((byte) (l >>> 8))
		      .writeByte((byte) l);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public void setLong(WritableBuffer buffer, int index, long l) {
		
		buffer.setByte(index, (byte) (l >>> 56))
		      .setByte(index + 1, (byte) (l >>> 48))
		      .setByte(index + 2, (byte) (l >>> 40))
		      .setByte(index + 3, (byte) (l >>> 32))
		      .setByte(index + 4, (byte) (l >>> 24))
		      .setByte(index + 5, (byte) (l >>> 16))
		      .setByte(index + 6, (byte) (l >>> 8))
		      .setByte(index + 7, (byte) l);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public short readShort(ByteReader reader) throws IOException {
		
		return (short) (reader.readByte() << 8 | reader.readByte() & 0xff);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public short getShort(ReadableBuffer buffer, int index) {
		
		return (short) (buffer.getByte(index) << 8 | buffer.getByte(index + 1) & 0xff);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int readInt(ByteReader reader) throws IOException {
		
		return (reader.readByte() & 0xff) << 24 |
	           (reader.readByte() & 0xff) << 16 |	
	           (reader.readByte() & 0xff) << 8 |
			   (reader.readByte() & 0xff);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getInt(ReadableBuffer buffer, int index) {
		
		return (buffer.getByte(index) & 0xff) << 24 |
		       (buffer.getByte(index + 1) & 0xff) << 16 |	
		       (buffer.getByte(index + 2) & 0xff) << 8 |
			   (buffer.getByte(index + 3) & 0xff);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long readLong(ByteReader reader) throws IOException {
		
		return ((long) reader.readByte() & 0xff) << 56 |
		       ((long) reader.readByte() & 0xff) << 48 |	
	           ((long) reader.readByte() & 0xff) << 40 |
			   ((long) reader.readByte() & 0xff) << 32 |
		       ((long) reader.readByte() & 0xff) << 24 |
               ((long) reader.readByte() & 0xff) << 16 |	
               ((long) reader.readByte() & 0xff) << 8 |
			   ((long) reader.readByte() & 0xff);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getLong(ReadableBuffer buffer, int index) {
		
		return ((long) buffer.getByte(index) & 0xff) << 56 |
		       ((long) buffer.getByte(index + 1) & 0xff) << 48 |	
	           ((long) buffer.getByte(index + 2) & 0xff) << 40 |
			   ((long) buffer.getByte(index + 3) & 0xff) << 32 |
		       ((long) buffer.getByte(index + 4) & 0xff) << 24 |
               ((long) buffer.getByte(index + 5) & 0xff) << 16 |	
               ((long) buffer.getByte(index + 6) & 0xff) << 8 |
			   ((long) buffer.getByte(index + 7) & 0xff);
	}

}
