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


import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import static org.apache.commons.lang.Validate.isTrue;
import static org.apache.commons.lang.Validate.notNull;

/**
 * <code>SliceableBuffer</code> backed by an array.
 * 
 * @author Benjamin
 *
 */
public final class HeapBuffer extends AbstractBuffer {

	/**
	 * The byte array.
	 */
	private final byte[] array;

	/**	
	 * {@inheritDoc}
	 */
    @Override
    public HeapBuffer duplicate() {
	   
	    HeapBuffer duplicate = new HeapBuffer(this.array);
		duplicate.subRegion(getOffset(), capacity());
		duplicate.writerIndex(writerIndex());
		duplicate.readerIndex(readerIndex());
	    
		return duplicate;
    }

	/**    
     * {@inheritDoc}
     */
	@Override
    public boolean hasArray() {
	    return true;
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public byte[] array() {
	    return this.array;
    }

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public boolean isDirect() {
	    return false;
    }
	
    /**
     * 
     * {@inheritDoc}
     */
    @Override
	public String toString() {
	    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).appendSuper(super.toString())
	    		                                                          .toString();
	}
    
	/**
	 * {@inheritDoc}
	 */
    @Override
    protected byte doGetByte(int index) {
	    return this.array[index];
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    protected void doGetBytes(int index, byte[] bytes, int off, int len) {
    	System.arraycopy(this.array, index, bytes, off, len);
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    protected void doSetByte(int index, int b) {
    	this.array[index] = (byte) b;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    protected void doSetBytes(int index, byte[] bytes, int off, int len) {
    	
    	System.arraycopy(bytes, off, this.array, index, len);
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    protected void checkSubRegion(int offset, int length) {
        
    	if ((offset + length) > this.array.length) { 
            throw new IndexOutOfBoundsException("Index: " + (offset + length) +
            		" Capacity: " + this.array.length);
        }    
    }
    
    /**
     * Creates a new <code>HeapBuffer</code> that wrap the specified array.
     * 
     * @param array the array.
     */
    HeapBuffer(byte[] array) {
    	
    	notNull(array, "the array parameter must not be null.");
    	
    	this.array = array;
		subRegion(0, array.length);
		writerIndex(array.length);
    }
    
    /**
     * Creates a new <code>HeapBuffer</code> with the specified capacity.
     * 
     * @param capacity the buffer capacity.
     */
    HeapBuffer(int capacity) {

		isTrue(capacity >= 0, "the capacity must be positive");
		
		this.array = new byte[capacity];
		subRegion(0, capacity);
		writerIndex(0);
    }
}    
