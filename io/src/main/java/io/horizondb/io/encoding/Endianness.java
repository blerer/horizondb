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
public abstract class Endianness {

	/**
	 * The big-endian byte order.
	 */
	private static final Endianness BIG_ENDIAN = new BigEndian();
	
	/**
	 * The little-endian byte order.
	 */
	private static final Endianness LITTLE_ENDIAN = new LittleEndian();

	/**
	 * The length of a short integer in number of bytes.
	 */
	public static final int SHORT_LENGTH = 2;
	
	/**
	 * The length of an integer in number of bytes.
	 */
	public static final int INT_LENGTH = 4;
	
	/**
	 * The length of a long integer in number of bytes.
	 */
	public static final int LONG_LENGTH = 8;
	
	/**
	 * Returns the endianness corresponding to the specified byte order.
	 * 
	 * @param order the byte order
	 * @return the endianness corresponding to the specified byte order.
	 */
	public static Endianness getEndianness(ByteOrder order) {
		
		if (order == BIG_ENDIAN.order()) {
			return BIG_ENDIAN;
		}
		
		return LITTLE_ENDIAN;
	}
	
	/**
	 * Returns the <code>ByteOrder</code>.
	 * 
	 * @return the <code>ByteOrder</code>.
	 */
	public abstract ByteOrder order();
	
    /**
     * Writes the specified 16-bit short integer into the specified writer.
     * 
     * @param writer the writer to write to.
     * @param s the short integer to write.
     * @throws IOException if a problem occurs while writing.
     */
	public abstract void writeShort(ByteWriter writer, short s) throws IOException;
	
    /**
     * Writes the specified 16-bit short integer into the specified buffer at the specified position.
     * 
     * @param buffer the buffer to write to.
     * @param index the position at which the short must be written.
     * @param s the short integer to write.
     */
	public abstract void setShort(WritableBuffer buffer, int index, short s);
	
    /**
     * Writes the specified unsigned 16-bit short integer into the specified writer.
     * 
     * @param writer the writer to write to.
     * @param s the unsigned short integer to write.
     * @throws IOException if a problem occurs while writing.
     */
	public final void writeUnsignedShort (ByteWriter writer, int s) throws IOException {
		
		writeShort(writer, (short)(s & 0xffff));
	}
	
    /**
     * Writes the specified 16-bit unsigned short integer into the specified buffer at the specified position.
     * 
     * @param buffer the buffer to write to.
     * @param index the position at which the short must be written.
     * @param s the short integer to write.
     */
	public final void setUnsignedShort(WritableBuffer buffer, int index, int s) {
		
		setShort(buffer, index, (short)(s & 0xffff));
	}
	
    /**
     * Writes the specified 32-bit integer into the specified writer.
     * 
     * @param writer the writer to write to.
     * @param i the integer to write.
     * @throws IOException if a problem occurs while writing.
     */
	public abstract void writeInt(ByteWriter writer, int i) throws IOException;
	
    /**
     * Writes the specified 32-bit integer into the specified buffer at the specified position.
     * 
     * @param buffer the buffer to write to.
     * @param index the position at which the integer must be written.
     * @param i the integer to write.
     */
	public abstract void setInt(WritableBuffer buffer, int index, int i);
	
    /**
     * Writes the specified unsigned 32-bit integer into the specified writer.
     * 
     * @param writer the writer to write to.
     * @param i the unsigned integer to write.
     * @throws IOException if a problem occurs while writing.
     */
	public final void writeUnsignedInt(ByteWriter writer, long l) throws IOException {
		
		writeInt(writer, (int)(l & 0xffffffffL));
	}
	
    /**
     * Writes the specified 64-bit integer into the specified buffer at the specified position.
     * 
     * @param buffer the buffer to write to.
     * @param index the position at which the long must be written.
     * @param l the long integer to write.
     */
	public abstract void setLong(WritableBuffer buffer, int index, long l);
	
    /**
     * Writes the specified 32-bit unsigned integer into the specified buffer at the specified position.
     * 
     * @param buffer the buffer to write to.
     * @param index the position at which the unsigned integer must be written.
     * @param i the integer to write.
     */
	public final void setUnsignedInt(WritableBuffer buffer, int index, long l) {
		setInt(buffer, index, (int)(l & 0xffffffffL));
	}
	
    /**
     * Writes the specified 64-bit long into the specified writer.
     * 
     * @param writer the writer to write to.
     * @param l the long.
     * @throws IOException if a problem occurs while writing.
     */
	public abstract void writeLong(ByteWriter writer, long l) throws IOException;
	
    /**
     * Reads a 16-bit short integer from the specified reader.
     * 
	 * @param reader the reader to read from.
	 * @return the short integer read.
	 * @throws IOException if a problem occurs while reading.
	 */
	public abstract short readShort(ByteReader reader) throws IOException;
	
    /**
     * Reads a 16-bit short integer from the specified position of the specified buffer.
     * 
	 * @param buffer the buffer to read from.
	 * @param index the buffer position to read from.
	 * @return the short integer read.
	 */
	public abstract short getShort(ReadableBuffer buffer, int index);
	
    /**
     * Reads a 32-bit integer from the specified reader.
     * 
	 * @param reader the reader to read from.
	 * @return the integer read.
	 * @throws IOException if a problem occurs while reading.
	 */
	public abstract int readInt(ByteReader reader) throws IOException;
	
    /**
     * Reads a 32-bit integer from the specified position of the specified buffer.
     * 
	 * @param buffer the buffer to read from.
	 * @param index the buffer position to read from.
	 * @return the integer read.
	 */
	public abstract int getInt(ReadableBuffer buffer, int index);
		
    /**
     * Reads a 64-bit long integer from the specified reader.
     * 
	 * @param reader the reader to read from.
	 * @return the long integer read.
	 * @throws IOException if a problem occurs while reading.
	 */
	public abstract long readLong(ByteReader reader) throws IOException;
	
    /**
     * Reads a 64-bit long integer from the specified position of the specified buffer.
     * 
	 * @param buffer the buffer to read from.
	 * @param index the buffer position to read from.
	 * @return the long integer read.
	 */
	public abstract long getLong(ReadableBuffer buffer, int index);
	
    /**
     * Reads a 16-bit unsigned short integer from the specified reader.
     * 
	 * @param reader the reader to read from.
	 * @return the unsigned short integer read.
	 * @throws IOException if a problem occurs while reading.
	 */
	public final int readUnsignedShort(ByteReader reader) throws IOException {
		
		return readShort(reader)& 0xffff;
	}

    /**
     * Reads a 16-bit unsigned short integer from the specified position of the specified buffer.
     * 
	 * @param buffer the buffer to read from.
	 * @param index the buffer position to read from.
	 * @return the unsigned short integer read.
	 */
	public final int getUnsignedShort(ReadableBuffer buffer, int index){
		
		return getShort(buffer, index)& 0xffff;
	}
	
    /**
     * Reads a 32-bit unsigned integer from the specified reader.
     * 
	 * @param reader the reader to read from.
	 * @return the unsigned integer read.
	 * @throws IOException if a problem occurs while reading.
	 */
	public final long readUnsignedInt (ByteReader reader) throws IOException {
		
		return readInt(reader) & 0xffffffffL;
	}
	
    /**
     * Reads a 32-bit unsigned integer from the specified position of the specified buffer.
     * 
	 * @param buffer the buffer to read from.
	 * @param index the buffer position to read from.
	 * @return the unsigned integer read.
	 */
	public final long getUnsignedInt(ReadableBuffer buffer, int index){
		
		return getInt(buffer, index)& 0xffffffffL;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public String toString() {
	   return order().toString();
    }
}
