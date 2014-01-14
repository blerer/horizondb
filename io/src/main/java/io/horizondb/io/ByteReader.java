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


import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Interface for reading bytes from a buffer or a stream.
 * 
 * @author Benjamin
 *
 */
public interface ByteReader {
	
	/**
	 * Returns the byte order used by this <code>ByteReader</code> to read the data.
	 * 
	 * @return the byte order used to read the data
	 */
	ByteOrder order();
	
	/**
	 * Specify the byte order that this <code>ByteReader</code> must used to read the data.
	 * 
	 * @param order the byte order that this <code>ByteReader</code> must used to read the data
	 * @return this <code>ByteReader</code>
	 */
	ByteReader order(ByteOrder order);
	
    /**
     * Skip the specified number of bytes.
     * 
     * @param numberOfBytes the number of bytes to skip.
     * @return this <code>ByteReader</code>
     * @throws IOException if an I/O problem occurs while reading the data.
     */
	ByteReader skipBytes(int numberOfBytes) throws IOException;

    /**
     * Read the next byte from the underlying file.
     * 
     * @return the next byte available.
     * @throws IOException if an I/O problem occurs while reading the data.
     */
    byte readByte() throws IOException;

    /**
     * Read the next bytes from the underlying file.
     * 
     * @param bytes the array that must be filled with the next bytes 
     * available.
     * @return this <code>ByteReader</code>
     * @throws IOException if an I/O problem occurs while reading the data.
     */
    ByteReader readBytes(byte[] bytes) throws IOException;
    
    /**
     * Read the specified amount of bytes from the underlying file.
     * 
     * @param bytes the array that must be filled with the next bytes 
     * available.
     * @param offset the position where the bytes must be written in the array.
     * @param length the number of bytes that must be transfered.
     * @return this <code>ByteReader</code>
     * @throws IOException if an I/O problem occurs while reading the data.
     */
    ByteReader readBytes(byte[] bytes, int offset, int length) throws IOException;
    
    /**
     * Reads a 16-bit short integer.
     * 
	 * @return the short integer read.
	 * @throws IOException if a problem occurs while reading.
	 */
    short readShort() throws IOException;
    
    /**
     * Reads a 16-bit unsigned short integer.
     * 
	 * @return the unsigned short integer read.
	 * @throws IOException if a problem occurs while reading.
	 */
	int readUnsignedShort() throws IOException;
    
    /**
     * Reads a 32-bit integer.
     * 
	 * @return the integer read.
	 * @throws IOException if a problem occurs while reading.
	 */
	int readInt() throws IOException;
	
    /**
     * Reads a 32-bit unsigned integer.
     * 
	 * @return the unsigned integer read.
	 * @throws IOException if a problem occurs while reading.
	 */
	long readUnsignedInt() throws IOException;
	
    /**
     * Reads a 64-bit long integer.
     * 
	 * @return the long integer read.
	 * @throws IOException if a problem occurs while reading.
	 */
	long readLong() throws IOException;

    /**
     * Returns a slice from this <code>ByteReader</code> containing only the specified amount of bytes.
     * 
     * <p>WARNING: For performance reasons slices object can be recycled.</p>
     * 
     * @param length the number of bytes that can be read from the <code>ByteReader</code>.
     * @return a <code>ByteReader</code> containing the specified amount of bytes.
     * @throws IOException if an I/O problem occurs while reading the data.
     */
    ReadableBuffer slice(int length) throws IOException;

    /**
     * Returns <code>true</code> if there are more available bytes to read.
     * 
     * @return <code>true</code> if there are more available bytes to read.
     * @throws IOException if an I/O problem occurs while reading the data.
     */
    boolean isReadable() throws IOException;

	/**
	 * Returns <code>true</code> if the value of the next byte is one, <code>false</code> otherwise.
	 * 
	 * @return <code>true</code> if the value of the next byte is one, <code>false</code> otherwise.
	 * @throws IOException if an I/O problem occurs while reading the data.
	 */
    boolean readBoolean() throws IOException;
}
