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

/**
 * Writeable buffer.
 * 
 * @author Benjamin
 *
 */
public interface WritableBuffer extends ByteWriter {
	
	/**
	 * Returns the writer index of this buffer.
	 * 
	 * @return the writer index of this buffer.
	 */
	int writerIndex();
	
	/**
	 * Sets the writer index of this buffer.
	 * 
	 * @param index the new writer index of this buffer.
	 */
	WritableBuffer writerIndex(int index);
	
	/**
	 * Returns <code>true</code> if this buffer is writable, <code>false</code> otherwise.
	 * @return <code>true</code> if this buffer is writable, <code>false</code> otherwise.
	 */
	boolean isWriteable();
	
	/**
	 * Returns the number of writable bytes.
	 * 
	 * @return the number of writable bytes.
	 */
	int writeableBytes();
	
	/**
	 * Sets the byte located at the specified position.
	 * 
	 * @param index the byte index
	 * @param b the byte to set
	 * @return this buffer
	 */
	WritableBuffer setByte(int index, int b);
	
    /**
     * Writes the specified 16-bit short integer at the specified position.
     * 
     * @param index the position at which the short must be written.
     * @param s the short integer to write.
     * @return this buffer.
     */
	WritableBuffer setShort(int index, short s);
	
    /**
     * Writes the specified 16-bit unsigned short integer at the specified position.
     * 
     * @param index the position at which the short must be written.
     * @param s the short integer to write.
     * @return this buffer.
     */
	WritableBuffer setUnsignedShort(int index, int s);

    /**
     * Writes the specified 32-bit integer at the specified position.
     * 
     * @param index the position at which the integer must be written.
     * @param i the integer to write.
     * @return this buffer.
     */
	WritableBuffer setInt(int index, int i);
	
    /**
     * Writes the specified 64-bit integer at the specified position.
     * 
     * @param index the position at which the long must be written.
     * @param l the long integer to write.
     * @return this buffer.
     */
	WritableBuffer setLong(int index, long l);
	
    /**
     * Writes the specified 32-bit unsigned integer at the specified position.
     * 
     * @param index the position at which the unsigned integer must be written.
     * @param i the integer to write.
     * @return this buffer.
     */
	WritableBuffer setUnsignedInt(int index, long l);
}
