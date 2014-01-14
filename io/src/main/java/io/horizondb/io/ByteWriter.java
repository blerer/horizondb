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

import io.horizondb.io.serialization.Serializable;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Interface for writing bytes to a buffer or a stream.
 * 
 * @author Benjamin
 *
 */
public interface ByteWriter {

	/**
	 * Returns the byte order used by this <code>ByteWriter</code> to write the data.
	 * 
	 * @return the byte order used to write the data
	 */
	ByteOrder order();
	
	/**
	 * Specify the byte order that this <code>ByteWriter</code> must used to write the data.
	 * 
	 * @param order the byte order that this <code>ByteWriter</code> must used to write the data
	 * @return this <code>ByteWriter</code>
	 */
	ByteWriter order(ByteOrder order);
	
    /**
     * Write the specified byte to this output.
     * 
     * @param b the byte to write.
     * @return this <code>ByteWriter</code>
     * @throws IOException if an I/O error occurs.
     */
	ByteWriter writeByte(int b) throws IOException;
    
    /**
     * Writes the specified bytes to this output.
     * 
     * @param bytes the bytes to write.
     * @return this <code>ByteWriter</code>
     * @throws IOException if an I/O error occurs.
     */
	ByteWriter writeBytes(byte[] bytes) throws IOException;
    
    /**
     * Writes the specified bytes from the specified byte array to this output stream.
     * 
     * @param bytes the data
     * @param offset the start offset in the data.
     * @param length the number of bytes to write.
     * @return this <code>ByteWriter</code>
     * @throws IOException if an I/O error occurs.
     */
	ByteWriter writeBytes(byte[] bytes, int offset, int length) throws IOException;
    
    /**
     * Writes the specified number of zeros.
     *
     * @param length the number of zero that must be written.
     * @return this <code>ByteWriter</code>
     * @throws IOException if an I/O error occurs.
     */
	ByteWriter writeZeroBytes(int length) throws IOException;
	
    /**
     * Writes the specified 16-bit short integer.
     * 
     * @param s the short integer to write.
     * @return this <code>ByteWriter</code>
     * @throws IOException if a problem occurs while writing.
     */
	ByteWriter writeShort(short s) throws IOException;
	
    /**
     * Writes the specified unsigned 16-bit short integer.
     * 
     * @param s the unsigned short integer to write.
     * @return this <code>ByteWriter</code>
     * @throws IOException if a problem occurs while writing.
     */
	ByteWriter writeUnsignedShort(int s) throws IOException;
	
    /**
     * Writes the specified 32-bit integer.
     * 
     * @param i the integer to write.
     * @return this <code>ByteWriter</code>
     * @throws IOException if a problem occurs while writing.
     */
	ByteWriter writeInt(int i) throws IOException;
	
    /**
     * Writes the specified unsigned 32-bit integer.
     * 
     * @param i the unsigned integer to write.
     * @return this <code>ByteWriter</code>
     * @throws IOException if a problem occurs while writing.
     */
	ByteWriter writeUnsignedInt(long l) throws IOException;
	
    /**
     * Writes the specified 64-bit long.
     * 
     * @param l the long.
     * @return this <code>ByteWriter</code>
     * @throws IOException if a problem occurs while writing.
     */
	ByteWriter writeLong(long l) throws IOException;
		
    /**
     * Writes the specified <code>Serializable</code>.
     *
     * @param serializable the <code>Serializable</code> that must be written.
     * @return this <code>ByteWriter</code>
     * @throws IOException if an I/O error occurs.
     */
	ByteWriter writeObject(Serializable serializable) throws IOException;

	/**
	 * Transfers the bytes from the specified byte reader into this writer.
	 * 
	 * @param reader the <code>ByteReader</code>.
     * @return this <code>ByteWriter</code>
     * @throws IOException if an I/O error occurs.
	 */
    ByteWriter transfer(ByteReader reader) throws IOException;

	/**
	 * Writes the specified boolean. 
	 * 
	 * @param b the boolean to write
     * @return this <code>ByteWriter</code>
     * @throws IOException if an I/O error occurs.
	 */
    ByteWriter writeBoolean(boolean b) throws IOException;
}
