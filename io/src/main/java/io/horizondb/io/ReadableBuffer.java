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
package io.horizondb.io;

import java.nio.ByteOrder;


/**
 * Readable buffer.
 * 
 * @author Benjamin
 * 
 */
public interface ReadableBuffer extends ByteReader {
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	ReadableBuffer order(ByteOrder order);
	
	/**
	 * Returns the reader index of this buffer.
	 * 
	 * @return the reader index of this buffer.
	 */
	int readerIndex();
	
	/**
	 * Returns the number of readable bytes.
	 * 
	 * @return the number of readable bytes.
	 */
	int readableBytes();
	
	/**
	 * Sets the reader index of this buffer.
	 * 
	 * @param readerIndex the new reader index of this buffer.
	 * @return this <code>ReadableBuffer</code>
	 */
	ReadableBuffer readerIndex(int readerIndex);
	
	/**
	 * Returns the byte located at the specified position.
	 * 
	 * @param index the byte index
	 * @return the byte located at the specified index
	 */
	byte getByte(int index);
	
	/**
	 * Transfers the bytes from this buffer starting at the specified location into the specified array.
	 * 
	 * @param index the byte index where the transfers should start.
	 * @param array the array into which the bytes must be transfered.
	 * @return this buffer
	 */
	ReadableBuffer getBytes(int index, byte[] array);
	
	/**
	 * Transfers the bytes from this buffer starting at the specified location into the specified array.
	 * 
	 * @param index the byte index where the transfers should start.
	 * @param array the array into which the bytes must be transfered.
	 * @param offset the position where the first byte must be transfered.
	 * @param length the number of bytes to copy.
	 * @return this buffer
	 */
	ReadableBuffer getBytes(int index, byte[] array, int offset, int length);
	
    /**
     * Reads a 16-bit short integer from the specified position.
     * 
	 * @param index the buffer position to read from.
	 * @return the short integer read.
	 */
	short getShort(int index);
	
    /**
     * Reads a 32-bit integer from the specified position.
     * 
	 * @param index the buffer position to read from.
	 * @return the integer read.
	 */
	int getInt(int index);
	
    /**
     * Reads a 64-bit long integer from the specified position.
     * 
	 * @param index the buffer position to read from.
	 * @return the long integer read.
	 */
	long getLong(int index);
	
    /**
     * Reads a 16-bit unsigned short integer from the specified position.
     * 
	 * @param index the buffer position to read from.
	 * @return the unsigned short integer read.
	 */
	int getUnsignedShort(int index);
	
    /**
     * Reads a 32-bit unsigned integer from the specified position.
     * 
	 * @param index the buffer position to read from.
	 * @return the unsigned integer read.
	 */
	long getUnsignedInt(int index);
	
	/**
	 * Creates a copy of this buffer with its own independent indices.
	 * @return a copy of this buffer with its own independent indices.
	 */
	ReadableBuffer duplicate();
}
