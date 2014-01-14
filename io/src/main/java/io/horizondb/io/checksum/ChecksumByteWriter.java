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

import io.horizondb.io.AbstractByteWriter;
import io.horizondb.io.ByteWriter;

import java.io.IOException;

import static org.apache.commons.lang.Validate.notNull;


/**
 * <code>ByteWriter</code> decorator that computes a CRC checksum on the data being written.
 * 
 * @author Benjamin
 *
 */
public final class ChecksumByteWriter extends AbstractByteWriter {

	/**
	 * The CRC calculator.
	 */
	private final Crc32 checksum = new Crc32();
	
	/**
	 * The decorated <code>ByteWriter</code>.
	 */
	private final ByteWriter writer;
	  
	/**
	 * Decorates the specified <code>ByteWriter</code> with an new <code>ChecksumByteWriter</code>.
	 * 
	 * @param writer the writer to decorate.
	 * @return a new <code>ChecksumByteWriter</code>.
	 */
	public static ChecksumByteWriter wrap(ByteWriter writer) {
		
		notNull(writer, "the writer parameter must not be null.");
		
		return new ChecksumByteWriter(writer);
	}
	
	/**
	 * Creates a new <code>ChecksumByteWriter</code> instance.
	 * 
	 * @param writer the decorated <code>ByteWriter</code>.
	 */
    private ChecksumByteWriter(ByteWriter writer) {
	    this.writer = writer;
	    order(writer.order());
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChecksumByteWriter writeByte(int b) throws IOException {
		
		this.checksum.update(b);
		this.writer.writeByte(b);
		
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChecksumByteWriter writeBytes(byte[] bytes, int offset, int length) throws IOException {
		
		this.checksum.update(bytes, offset, length);
		this.writer.writeBytes(bytes, offset, length);
		
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChecksumByteWriter writeZeroBytes(int length) throws IOException {
		
		for (int i = 0; i < length; i++) {
			
			writeByte((byte) 0); 
		}
		
		return this;
	}
	
	/**
	 * Writes the checksum of the data written since the last reset to the decorated writer.
	 * 
	 * @throws IOException if a problem occurs while writing.
	 */
	public void writeChecksum() throws IOException {
		
		this.writer.writeLong(this.checksum.getValue());
	}
	
	/**
	 * Reset the CRC value.
	 * 
	 * @return this <code>ChecksumByteWriter</code>.
	 */
	public ChecksumByteWriter reset() {
		
		this.checksum.reset();
		return this;
	}
}
