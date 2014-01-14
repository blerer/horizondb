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
package io.horizondb.io.checksum;

import io.horizondb.io.AbstractByteReader;
import io.horizondb.io.ByteReader;
import io.horizondb.io.ReadableBuffer;

import java.io.IOException;

import static org.apache.commons.lang.Validate.notNull;


/**
 * Base class for the checksum <code>ByteReader</code>.
 * 
 * @author Benjamin
 *
 */
public final class ChecksumByteReader extends AbstractByteReader {

	/**
	 * The CRC calculator.
	 */
	private final Crc32 checksum = new Crc32();
	
	/**
	 * The decorated <code>ByteReader</code>.
	 */
	private final ByteReader reader;
	
	/**
	 * Creates a new <code>ChecksumByteReader</code> instance that wraps the specified reader.
	 * 
	 * @param reader the wrapped reader.
	 */
	public static ChecksumByteReader wrap(ByteReader reader) {
		
		return new ChecksumByteReader(reader);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public final ByteReader skipBytes(int numberOfBytes) throws IOException {
		this.reader.skipBytes(numberOfBytes);
		
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final byte readByte() throws IOException {

		byte b = this.reader.readByte();
		updateChecksum(b);
		
		return b;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final ByteReader readBytes(byte[] bytes) throws IOException {
		
		return readBytes(bytes, 0, bytes.length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final ByteReader readBytes(byte[] bytes, int offset, int length) throws IOException {
		
		this.reader.readBytes(bytes, offset, length);
		updateChecksum(bytes, offset, length);
		
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final ReadableBuffer slice(int length) throws IOException {
		
		ReadableBuffer slice = this.reader.slice(length);
		
		this.checksum.update(slice);
		
		return slice;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final boolean isReadable() throws IOException {
		return this.reader.isReadable();
	}

	/**
	 * Reads the checksum from the underlying reader and compare it to the computed one.
	 * 
	 * @return <code>true</code> if the computed and read checksums match, <code>false</code> otherwise.
	 * @throws IOException if a problem occurs while reading.
	 */
	public final boolean readChecksum() throws IOException {
		
		long l = this.reader.readLong();
		
		return this.checksum.getValue() == l;
	}
	
	/**
	 * Reset the CRC value.
	 */
	public final void resetChecksum() {
		
		this.checksum.reset();
	}

	/**
	 * Updates the checksum with the specified byte.
	 * 
	 * @param b the byte
	 */
    private final void updateChecksum(byte b) {
	    this.checksum.update(b);
    }
    
	/**
	 * Updates the checksum with the specified bytes.
	 * 
	 * @param bytes the byte array 
	 * @param offset the start within the array of the bytes that should be use to update the checksum
	 * @param length the number of bytes
	 */
    private final void updateChecksum(byte[] bytes, int offset, int length) {
	    this.checksum.update(bytes, offset, length);
    }
	
	/**
	 * Creates a new <code>ChecksumByteReader</code> instance.
	 */
    private ChecksumByteReader(ByteReader reader) {
		
		notNull(reader, "the reader parameter must not be null.");
		
		this.reader = reader;
		order(this.reader.order());
	}
}
