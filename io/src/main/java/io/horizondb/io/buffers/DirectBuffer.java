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
package io.horizondb.io.buffers;

import java.nio.ByteBuffer;

import static org.apache.commons.lang.Validate.isTrue;
import static org.apache.commons.lang.Validate.notNull;

/**
 * <code>SliceableBuffer</code> backed by a direct buffer.
 * 
 * @author Benjamin
 *
 */
final class DirectBuffer extends AbstractBuffer {

	/**
	 * The direct NIO buffer.
	 */
	private final ByteBuffer buffer;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasArray() {
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public byte[] array() {
		throw new UnsupportedOperationException("this buffer is not backed by an array");
	}
	
	/**	
	 * {@inheritDoc}
	 */
	@Override
    public boolean isDirect() {
	    return true;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DirectBuffer duplicate() {
		
		DirectBuffer duplicate = new DirectBuffer(this.buffer);
		duplicate.subRegion(getOffset(), capacity());
		duplicate.writerIndex(writerIndex());
		duplicate.readerIndex(readerIndex());
	    
		return duplicate;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected byte doGetByte(int index) {
		return this.buffer.get(index);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void doGetBytes(int index, byte[] bytes, int off, int len) {

		this.buffer.position(index);
		this.buffer.get(bytes, off, len);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void doSetByte(int index, int b) {
		this.buffer.put(index, (byte) b);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void doSetBytes(int index, byte[] bytes, int off, int len) {
		this.buffer.position(index);
		this.buffer.put(bytes, off, len);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void checkSubRegion(int offset, int length) {
    	
		if ((offset + length) > this.buffer.capacity()) { 
            throw new IndexOutOfBoundsException("Index: " + (offset + length) +
            		" Capacity: " + this.buffer.capacity());
        }  
	}

    /**
     * Creates a new <code>DirectBuffer</code> that wrap the specified <code>ByteBuffer</code>.
     * 
     * @param buffer the <code>ByteBuffer</code>.
     */
	DirectBuffer(ByteBuffer buffer) {
		
		notNull(buffer, "the buffer parameter must not be null");
		isTrue(buffer.isDirect(), "the buffer must be direct");
		
		this.buffer = buffer.duplicate();
		
		subRegion(0, buffer.capacity());
		writerIndex(buffer.capacity());
	}
	
    /**
     * Creates a new <code>DirectBuffer</code> with the specified capacity.
     * 
     * @param capacity the buffer capacity.
     */
	DirectBuffer(int capacity) {
		
		isTrue(capacity >= 0, "the capacity must be positive");
		
		this.buffer = ByteBuffer.allocateDirect(capacity);
		subRegion(0, capacity);
		writerIndex(0);
	}
}
