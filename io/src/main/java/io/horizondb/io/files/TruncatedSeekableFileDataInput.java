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
package io.horizondb.io.files;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ReadableBuffer;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteOrder;

import static org.apache.commons.lang.Validate.isTrue;
import static org.apache.commons.lang.Validate.notNull;

/**
 * <code>SeekableFileDataInput</code> decorator that truncate the decorated input to a specified length.
 * 
 * @author Benjamin
 *
 */
public final class TruncatedSeekableFileDataInput implements SeekableFileDataInput {

	/**
	 * The decorated input.
	 */
	private final SeekableFileDataInput input;

	/**
	 * The offset.
	 */
	private final long offset;
	
	/**
	 * The length.
	 */
	private final long length;
		
	/**
	 * Creates a <code>TruncatedSeekableFileDataInput</code> that truncate the specified input to the 
	 * specified length.
	 * 
	 * @param input the input to truncate
	 * @param length the length of this truncated input
	 * @throws IOException if an I/O problem occurs.
	 */
    public TruncatedSeekableFileDataInput(SeekableFileDataInput input, long length) throws IOException {
	    
    	this(input, 0, length); 
    }
	
	/**
	 * Creates a <code>TruncatedSeekableFileDataInput</code> that truncate the specified input to the 
	 * part specified by the given offset and length.
	 *  
	 * @param input the input to truncate
	 * @param offset the offset at which will start the input
	 * @param length the length of this truncated input
	 * @throws IOException if an I/O problem occurs.
	 */
    public TruncatedSeekableFileDataInput(SeekableFileDataInput input, long offset, long length) throws IOException {
	    
    	notNull(input, "the input parameter must not be null.");
    	
    	long realSize = input.size();
    	
    	isTrue(offset >= 0, "the offset must be greater or equals to zero");
    	isTrue(length >= 0, "the length must be greater or equals to zero");
    	isTrue(offset < realSize, "the offset is greater than the input length");
    	isTrue(offset + length <= realSize, "the offset + length is greater than the input length");
    	
    	this.input = input;
    	this.offset = offset;
    	this.length = length;
    	
    	this.input.seek(offset);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void seek(long position) throws IOException {
		
		if (position >= this.length) {
			throw new EOFException("seeking position: " + position + " length of the input: " + this.length);
		}
		
	    this.input.seek(position + this.offset);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ByteOrder order() {
	    return this.input.order();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getPosition() throws IOException {
	    return this.input.getPosition() - this.offset;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ByteReader order(ByteOrder order) {
	    return this.input.order(order);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
	    this.input.close();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long size() throws IOException {
	    return this.length;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ByteReader skipBytes(int numberOfBytes) throws IOException {
		checkReadable(numberOfBytes);
		return this.input.skipBytes(numberOfBytes);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public byte readByte() throws IOException {
		checkReadable(1);
		return this.input.readByte();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ByteReader readBytes(byte[] bytes) throws IOException {
		checkReadable(bytes.length);
	    return this.input.readBytes(bytes);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ByteReader readBytes(byte[] bytes, int offset, int length) throws IOException {
		checkReadable(length);
	    return this.input.readBytes(bytes, offset, length);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public short readShort() throws IOException {
		checkReadable(2);
	    return this.input.readShort();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int readUnsignedShort() throws IOException {
		checkReadable(2);
		return this.input.readUnsignedShort();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int readInt() throws IOException {
		checkReadable(4);
	    return this.input.readInt();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long readUnsignedInt() throws IOException {
		checkReadable(4);
		return this.input.readUnsignedInt();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long readLong() throws IOException {
		checkReadable(8);
	    return this.input.readLong();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ReadableBuffer slice(int length) throws IOException {
		checkReadable(length);
		return this.input.slice(length);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isReadable() throws IOException {
	    	
		return getPosition() < this.length;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean readBoolean() throws IOException {
		checkReadable(1);
		return this.input.readBoolean();
    }
	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public long readableBytes() throws IOException {
	    return size() - getPosition();
    }
	
	/**
	 * Checks that the specified amount of bytes is readable.
	 * 
	 * @param numberOfBytesToRead the number of bytes to read
	 * @throws IOException if an I/O problem occurs or if the bytes are not readable.
	 */
    private void checkReadable(int numberOfBytesToRead) throws IOException {
	    
    	long readableBytes = readableBytes();
    	
    	if (numberOfBytesToRead > readableBytes) {

			throw new EOFException("Expected to be able to read "
                    + numberOfBytesToRead + " bytes, but only " + readableBytes
                    + " bytes are readable.");
    	}
	    
    }
}
