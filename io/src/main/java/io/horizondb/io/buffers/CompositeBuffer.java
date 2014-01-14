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


import io.horizondb.io.ReadableBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * A composite <code>ReadableBuffer</code>.
 * 
 * @author Benjamin
 *
 */
public final class CompositeBuffer extends AbstractReadableBuffer {

	/**
	 * The buffers composing this composite.
	 */
	private final List<ReadableBuffer> buffers;
	
	/**
	 * The offset of the region being visible.
	 */
	private int offset;

	/**
	 * The buffer capacity.
	 */
	private int capacity;
	
	/**
	 * The buffer being currently read.	
	 */
	private ReadableBuffer current;
	
	/**
	 * The offset in bytes of the current buffer.
	 */
	private int bufferOffset;
	
	/**
	 * The index of the buffer.
	 */
	private int bufferIndex;
	
	private CompositeBuffer slice;
	
	/**
	 * Creates a new <code>CompositeBuffer</code> instance.
	 */
	public CompositeBuffer() {
		
		this(new ArrayList<ReadableBuffer>());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int readerIndex() {
		return this.readerIndex - this.offset;
	}

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public CompositeBuffer duplicate() {
	    
	    CompositeBuffer duplicate = new CompositeBuffer(new ArrayList<>(this.buffers));
	    duplicate.offset = this.offset;
	    duplicate.bufferIndex = this.bufferIndex;
	    duplicate.bufferOffset = this.bufferOffset;
	    duplicate.current = this.current;
	    duplicate.readerIndex = this.readerIndex;
	    duplicate.capacity = this.capacity;
	    
		return duplicate;
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public CompositeBuffer readerIndex(int readerIndex) {
		
    	if (readerIndex < 0 || readerIndex > this.capacity) { 
    		
    		@SuppressWarnings("boxing")
            String msg = format("readerIndex: %d Expected: 0 <= readerIndex < capacity(%d)",
                                readerIndex, 
                                this.capacity);
    		
			throw new IndexOutOfBoundsException(msg);
        } 
		
    	this.current = this.buffers.get(0);
    	this.bufferIndex = 0;
    	this.bufferOffset = 0;
    	
		this.readerIndex = readerIndex + this.offset;
		
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public byte getByte(int index) {
		
		int idx = index + this.offset;
		
    	if (index < 0 || index > this.capacity) { 
    		
    		@SuppressWarnings("boxing")
            String msg = format("Index: %d Expected: 0 <= index < capacity(%d)",
                                index, 
                                this.capacity);
    		
			throw new IndexOutOfBoundsException(msg);
        } 
		
		int off = 0;
		
		for (int i = 0, m = this.buffers.size(); i < m; i++) {
			
			ReadableBuffer buffer = this.buffers.get(i);
			
			if (idx < off + buffer.readableBytes()) {
				
				return buffer.getByte(idx - off);
			}
			
			off += buffer.readableBytes();
		}
		
		throw new IllegalStateException();
	}

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public CompositeBuffer getBytes(int index, byte[] array) {
		
		return getBytes(index, array, 0, array.length);		
	}
	
	/**	
	 * {@inheritDoc}
	 */
	@Override
    public CompositeBuffer getBytes(int index, byte[] array, int offset, int length) {

		if (index < 0 || (index + length) > this.capacity) { 
    		
    		@SuppressWarnings("boxing")
            String msg = format("Index: %d Length: %d Expected: 0 <= index and index + length < capacity(%d)",
                                index,
                                length,
                                this.capacity);
    		
			throw new IndexOutOfBoundsException(msg);
        } 
		
		int position = index + this.offset;
		int bufferOffset = 0;
		int off = offset;
		int remaining = length;
		
		for (int i = 0, m = this.buffers.size(); i < m; i++) {
			
			ReadableBuffer buffer = this.buffers.get(i);
			
			if (position < bufferOffset + buffer.readableBytes()) {

				int len = Math.min(buffer.readableBytes() - (position - bufferOffset), remaining);
				
				buffer.getBytes(position - bufferOffset, array, off, len);
				
				remaining -= len;
			
				if (remaining == 0) {
					break;
				}
				
				position += len;
				off += len;
			}
			
			bufferOffset += buffer.readableBytes();
		}
		
		return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CompositeBuffer skipBytes(int numberOfBytes) {
		
		this.readerIndex += numberOfBytes;
		
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public byte readByte() {
		
		checkReadable(1);
		
		while (currentReadableBytes() <= 0) {
			
			nextBuffer();
		}
		
		byte b = this.current.getByte(currentIndex());
		this.readerIndex++;
		
		return b;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CompositeBuffer readBytes(byte[] bytes) {

		return readBytes(bytes, 0, bytes.length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CompositeBuffer readBytes(byte[] bytes, int offset, int length) {
		
		checkReadable(length);
		
		int off = offset;
		int remaining = length;	
		
		while (remaining > 0) {
						
			while (currentReadableBytes() <= 0) {
				nextBuffer();
			}
			
			int numberOfBytesToCopy = Math.min(remaining, currentReadableBytes());
		
			this.current.getBytes(currentIndex(), bytes, off, numberOfBytesToCopy);
			this.readerIndex += numberOfBytesToCopy;
			off += numberOfBytesToCopy;
			remaining -= numberOfBytesToCopy;
		}	
		
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CompositeBuffer slice(int len) {
		
		if (this.slice == null) {
			this.slice = new CompositeBuffer(this.buffers);
		}
		
		this.slice.readerIndex = this.readerIndex;
		this.slice.offset = this.readerIndex;
		this.slice.capacity = len;
		this.slice.bufferIndex = this.bufferIndex;
		this.slice.bufferOffset = this.bufferOffset;
		this.slice.current = this.current;
		
		this.readerIndex += len;
		
		return this.slice;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isReadable() {
		return readableBytes() > 0;
	}

	/**
	 * Adds the specified buffer to this composite.
	 * 
	 * @param buffer the buffer to add.
	 * @throws IOException if an I/O problem occurs.
	 */
    public void add(ReadableBuffer buffer) throws IOException {
	    
    	ReadableBuffer duplicate = buffer.slice(buffer.readableBytes()).duplicate();
    	
    	if (this.buffers.isEmpty()) {
    		
    		this.current = duplicate;
    	}
    	
    	this.buffers.add(duplicate);	
    	this.capacity += duplicate.readableBytes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("endianness", this.endianness)
		                                                                  .append("offset", this.offset)
		                                                                  .append("readerIndex", this.readerIndex)
		                                                                  .append("buffers", this.buffers)
		                                                                  .toString();
	}
    
	/**
	 * {@inheritDoc}
	 */
    @Override
    public int readableBytes() {
    	
	    return this.capacity - (this.readerIndex - this.offset);  
    }
    
	/**
	 * Returns the number of readable bytes within the current buffer.
	 * 
	 * @return the number of readable bytes within the current buffer.
	 */
    private int currentReadableBytes() {
	    return this.current.readableBytes() - currentIndex();
    }

	/**
	 * Return the reader index for the current buffer.
	 * 
	 * @return the reader index for the current buffer.
	 */
    private int currentIndex() {
	    return this.readerIndex - this.bufferOffset;
    }
    
	/**
	 * Creates a new <code>CompositeBuffer</code> instance which contains the specified buffers.
	 */
	private CompositeBuffer(List<ReadableBuffer> buffers) {
		this.buffers = buffers;
				
		if (!this.buffers.isEmpty()) {
			
			for (int i = 0, m = buffers.size(); i < m; i++) {
				this.capacity += buffers.get(i).readableBytes();
			}
		}
		
	}
    
	/**
	 * Moves to the next buffer.
	 */
    private void nextBuffer() {
	    this.bufferOffset += this.current.readableBytes();
	    this.bufferIndex++;
	    this.current = this.buffers.get(this.bufferIndex);
    }
    
	/**
	 * Checks that the specified amount of bytes can be read.
	 * 
	 * @param numberOfBytes the number of bytes to read.
	 * @throws IndexOutOfBoundsException if the specified amount of bytes cannot be read.
	 */
    private void checkReadable(int numberOfBytes) {
    	
    	if (readableBytes() < numberOfBytes) {
    		
    		@SuppressWarnings("boxing")
            String msg = format("bytes to read: %d readable bytes: %d",
                                numberOfBytes,
                                readableBytes());
    		
			throw new IndexOutOfBoundsException(msg);
    	}
    }
}
