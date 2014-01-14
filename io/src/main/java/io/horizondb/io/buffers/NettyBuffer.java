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

import io.horizondb.io.Buffer;
import io.netty.buffer.ByteBuf;

import static org.apache.commons.lang.Validate.notNull;

/**
 * Adapts the <code>ByteBuff</code> to the <code>Buffer</code> interface.
 * 
 * @author Benjamin
 *
 */
final class NettyBuffer extends AbstractBuffer {

	/**
	 * The adapted buffer.
	 */
	private final ByteBuf buffer;

	/**
	 * {@inheritDoc}
	 */
    @Override
    public boolean hasArray() {
	    return this.buffer.hasArray();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public byte[] array() {
	    return this.buffer.array();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public boolean isDirect() {
	    return this.buffer.isDirect();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public Buffer duplicate() {

    	NettyBuffer duplicate = new NettyBuffer(this.buffer);
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
	    return this.buffer.getByte(index);
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    protected void doGetBytes(int index, byte[] bytes, int off, int len) {
    	this.buffer.getBytes(index, bytes, off, len);
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    protected void doSetByte(int index, int b) {
    	this.buffer.setByte(index, b);
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    protected void doSetBytes(int index, byte[] bytes, int off, int len) {
    	this.buffer.setBytes(index, bytes, off, len);
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
	 * Creates a new <code>NettyBuffer</code> that wraps the specified <code>ByteBuf</code>.
	 * @param buffer the <code>ByteBuf</code>
	 */
    NettyBuffer(ByteBuf buffer) {

    	notNull(buffer, "the buffer parameter must not be null.");
    	
	    this.buffer = buffer;
		
		subRegion(0, buffer.capacity());
		writerIndex(buffer.writerIndex());
    }
}
