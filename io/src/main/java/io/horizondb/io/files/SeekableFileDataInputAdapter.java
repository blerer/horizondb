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

import io.horizondb.io.ReadableBuffer;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * @author Benjamin
 *
 */
final class SeekableFileDataInputAdapter extends AbstractFileDataInput implements SeekableFileDataInput {

	private final ReadableBuffer buffer;

	private final long size;
	
	public SeekableFileDataInputAdapter(ReadableBuffer buffer) {
		
		this.buffer = buffer.duplicate();
		this.size = buffer.readableBytes();
	}
	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public long getPosition() throws IOException {
	    return this.buffer.readerIndex();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public long size() throws IOException {
	    return this.size;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void close() throws IOException {

    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public SeekableFileDataInput order(ByteOrder order) {
    	
    	this.buffer.order(order);
    	return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public SeekableFileDataInput skipBytes(int numberOfBytes) throws IOException {
    	this.buffer.skipBytes(numberOfBytes);
    	return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public byte readByte() throws IOException {
	    return this.buffer.readByte();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public SeekableFileDataInput readBytes(byte[] bytes) throws IOException {
    	
    	this.buffer.readBytes(bytes);
    	return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public SeekableFileDataInput readBytes(byte[] bytes, int offset, int length) throws IOException {
    	this.buffer.readBytes(bytes, offset, length);
    	return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public ReadableBuffer slice(int length) throws IOException {
	    return this.buffer.slice(length);
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public boolean isReadable() throws IOException {
	    return this.buffer.isReadable();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void seek(long position) throws IOException {
    	this.buffer.readerIndex((int) position);
    }
}
