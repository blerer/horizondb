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
import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.encoding.Endianness;
import io.horizondb.io.serialization.Serializable;

import java.io.IOException;
import java.nio.ByteOrder;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import static java.lang.String.format;

/**
 * Base class for <code>Buffer</code> classes.
 * 
 * @author Benjamin
 *
 */
abstract class AbstractBuffer extends AbstractReadableBuffer implements Buffer {
		
	/**
	 * The buffer slice.
	 */
	private Buffer slice;
	
	/**
	 * The region offset.
	 */
	private int offset;
	
	/**
	 * The length of the region. 
	 */
	private int length;
	
	/**
	 * The writer index.	
	 */
	private int writerIndex;

	/**
	 * {@inheritDoc}
	 */
	@Override
    public Buffer order(ByteOrder order) {
		
		super.order(order);
	    return this;
    }
	
	/**
	 * {@inheritDoc}
	 */	
	@Override
    public ByteWriter writeShort(short s) throws IOException {
		
		checkWriteable(Endianness.SHORT_LENGTH);
		setShort(this.writerIndex, s);	
		this.writerIndex += Endianness.SHORT_LENGTH;
		
	    return this;
    }

	/**	
	 * {@inheritDoc}
	 */	
	@Override
    public ByteWriter writeBoolean(boolean b) {

		if (b) {
			writeByte(1);
		} else {
			writeByte(0);
		}
		
		return this;
    }
	
	/**	
	 * {@inheritDoc}
	 */
	@Override
    public Buffer setShort(int index, short s) {
		
		this.endianness.setShort(this, index, s);	
	    return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public ByteWriter writeUnsignedShort(int s) throws IOException {
		
		checkWriteable(Endianness.SHORT_LENGTH);
		setUnsignedShort(this.writerIndex, s);	
		this.writerIndex += Endianness.SHORT_LENGTH;
		
	    return this;
    }

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public Buffer setUnsignedShort(int index, int s) {
		this.endianness.setUnsignedShort(this, index, s);
	    return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public ByteWriter writeInt(int i) throws IOException {
		
		checkWriteable(Endianness.INT_LENGTH);
		setInt(this.writerIndex, i);	
		this.writerIndex += Endianness.INT_LENGTH;
		
	    return this;
    }

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public Buffer setInt(int index, int i) {

		this.endianness.setInt(this, index, i);	
		return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public ByteWriter writeUnsignedInt(long l) throws IOException {
		
		checkWriteable(Endianness.INT_LENGTH);
		this.endianness.setUnsignedInt(this, this.writerIndex, l);	
		this.writerIndex += Endianness.INT_LENGTH;
		
	    return this;
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public Buffer setUnsignedInt(int index, long l) {
		this.endianness.setUnsignedInt(this, index, l);	
	    return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public ByteWriter writeLong(long l) throws IOException {
		
		checkWriteable(Endianness.LONG_LENGTH);
		setLong(this.writerIndex, l);	
		this.writerIndex += Endianness.LONG_LENGTH;
		
	    return this;
    }

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public Buffer setLong(int index, long l) {
		this.endianness.setLong(this, index, l);	
	    return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer subRegion(int offset, int length) {
	    
		checkSubRegion(offset, length);
		
		this.offset = offset;
		this.length = length;
		this.readerIndex = 0;
		this.writerIndex = length;
		
		return this;
	}

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public Buffer transfer(ByteReader reader) throws IOException {
	    
		while(reader.isReadable()) {
		
			this.writeByte(reader.readByte());
		}
	    		
		return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public final Buffer writeObject(Serializable serializable) throws IOException {

		serializable.writeTo(this);
		
	    return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public final Buffer writeBytes(byte[] bytes) {

		writeBytes(bytes, 0, bytes.length);
		
	    return this;
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Buffer skipBytes(int numberOfBytes) {
		
		checkReadable(numberOfBytes);
		
		this.readerIndex += numberOfBytes; 	
		
		return this;
	}

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public int arrayOffset() {
	    
		if (hasArray()) {
			
			return this.offset;
		}
		
	    throw new UnsupportedOperationException("This buffer has no backing array.");
    }

	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public Buffer slice(int len) {
	    
    	checkReadable(len);
    	
    	if (this.slice == null) {
    		
    		this.slice = duplicate();
    	}

    	this.slice.subRegion(this.offset + this.readerIndex, len);

    	this.readerIndex += len;    	    	
    	
	    return this.slice;
    }
	
    /**
     * Creates a new buffer that shares this buffer's content.
     *
     * <p> The content of the new buffer will be that of this buffer.  Changes
     * to this buffer's content will be visible in the new buffer, and vice
     * versa; the buffers' readerIndex and writerIndex will be independent.
     *
     * @return  The new buffer
     */
    @Override
    public abstract Buffer duplicate();

    /**    
     * {@inheritDoc}
     */
	@Override
    public byte getByte(int index) {
		
		if (index < 0 || index >= capacity()) {
			
			@SuppressWarnings("boxing")
            String msg = String.format("Index: %d Capacity: %d", index, capacity());
			
			throw new IndexOutOfBoundsException(msg);
		}
		
	    return doGetByte(this.offset + index);
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public ReadableBuffer getBytes(int index, byte[] array, int off, int len) {

		if (index < 0 || len < 0 || (index + len) > capacity()) {
			
			@SuppressWarnings("boxing")
            String msg = String.format("Index: %d Length: %d  Capacity: %d", index, len, capacity());
			
			throw new IndexOutOfBoundsException(msg);
		}
		
		doGetBytes(this.offset + index, array, off, len);		
	    return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final byte readByte() {
		
		checkReadable(1);
		
		return doGetByte(this.offset + this.readerIndex++);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Buffer readBytes(byte[] bytes) {
		return readBytes(bytes, 0, bytes.length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Buffer readBytes(byte[] bytes, int off, int len) {
		
		checkReadable(len);
		doGetBytes(this.offset + this.readerIndex, bytes, off, len);	
		this.readerIndex += len;
		
		return this;
	}

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public ReadableBuffer getBytes(int index, byte[] array) {
	    
	    return getBytes(index, array, 0, array.length);
    }

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public int readerIndex() {
	    return this.readerIndex;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public int writerIndex() {
        return this.writerIndex;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWriteable() {
    	
    	return this.writerIndex < capacity();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Buffer writerIndex(int writerIndex) {
        
    	if (writerIndex < this.readerIndex || writerIndex > capacity()) {
            
    		@SuppressWarnings("boxing")
            String msg = String.format("writerIndex: %d " +
    				"(expected: readerIndex(%d) <= writerIndex <= capacity(%d))", 
    				writerIndex, this.readerIndex, capacity());
    		
        	throw new IndexOutOfBoundsException(msg);
        }
    	
        this.writerIndex = writerIndex;
        
        return this;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Buffer readerIndex(int readerIndex) {
        
    	if (this.writerIndex < readerIndex || readerIndex < 0) {
            
    		@SuppressWarnings("boxing")
            String msg = String.format("readerIndex: %d " +
    				"(expected: 0 <= readerIndex <= writerIndex(%d))", readerIndex, this.writerIndex);
    		
        	throw new IndexOutOfBoundsException(msg);
        }
    	
        this.readerIndex = readerIndex;
        
        return this;
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public final boolean isReadable() {
		return this.writerIndex > this.readerIndex;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Buffer writeByte(int b) {
		
		checkWriteable(1);
		doSetByte(this.offset + this.writerIndex++, b);
		
		return this;
	}

	/**
	 * 	
	 * {@inheritDoc}
	 */
	@Override
    public final Buffer setByte(int index, int b) {
	    
		doSetByte(index, b);
		return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Buffer writeBytes(byte[] bytes, int off, int len) {

		checkWriteable(len);
		doSetBytes(this.offset + this.writerIndex, bytes, off, len);
		this.writerIndex += len;
		
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Buffer writeZeroBytes(int numberOfZero) {

		checkWriteable(numberOfZero);
		
		for (int i = 0; i < numberOfZero; i++) {
			doSetByte(this.offset + this.writerIndex++, 0);
		}
		
		return this;
	}

	/**
	 * {@inheritDoc}
	 */ 
	@Override
	public final int capacity() {
		
		return this.length;
	}
	    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int readableBytes() {
		
		return this.writerIndex - this.readerIndex;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int writeableBytes() {
		return this.length - this.writerIndex;
	}
	
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
		
		byte[] readableBytes = new byte[readableBytes()];
		doGetBytes(this.readerIndex + this.offset, readableBytes, 0, readableBytes.length);
		
	    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("offset", this.offset)
	    															      .append("length", this.length)
	    															      .append("readerIndex", this.readerIndex)
	    															      .append("writerIndex", this.writerIndex)
	                                                                      .append("bytes", readableBytes)
	                                                                      .toString();
	}

    
    
	/**
	 * Returns the byte at the specified index position.
	 * 
	 * @param index the byte index.
	 * @return the byte at the specified index position.
	 */
    protected abstract byte doGetByte(int index);
	
	/**
	 * @param index the bytes position 
	 * @param bytes the array into which the bytes must be read.
     * @param off the start offset in array at which the bytes are written.
     * @param len the number of bytes to read.
	 */
    protected abstract void doGetBytes(int index, byte[] bytes, int off, int len);
    
	/**
	 * Puts the specified byte at the specified position.
	 * 
	 * @param index the position at which the byte must be inserted.
	 * @param b the byte to store.
	 */
    protected abstract void doSetByte(int index, int b);
    
	/**
	 * Puts the specified bytes at the specified position.
	 * 
	 * @param index the position at which the byte must be inserted.
	 * @param bytes the bytes array 
     * @param off   the start offset in the bytes.
     * @param len   the number of bytes to write.
	 */
    protected abstract void doSetBytes(int index, byte[] bytes, int off, int len);
	
	/**
	 * Checks that the specified sub-region is valid.
	 * 
	 * @param offset the start of the sub-region
	 * @param length the sub-region length
	 */
    protected abstract void checkSubRegion(int offset, int length);
	
    /**
     * Returns the offset.
     * @return the offset.
     */
    protected final int getOffset() {
    	return this.offset;
    }
    
	/**
	 * Checks that the specified amount of bytes can be read.
	 * 
	 * @param numberOfBytes the number of bytes to read.
	 * @throws IllegalArgumentException if the specified number of bytes cannot be read.
	 */
    private void checkReadable(int numberOfBytes) {
	    
    	int readableBytes = readableBytes();

    	if (numberOfBytes > readableBytes) {
    		
    		@SuppressWarnings("boxing")
            String msg = format("the number of bytes to read (%d) exceed the number of readable bytes (%d)",
                                numberOfBytes, 
                                readableBytes);
    		
			throw new IndexOutOfBoundsException(msg);
    	}
    }
    
	/**
	 * Checks that the specified amount of bytes can be written.
	 * 
	 * @param numberOfBytes the number of bytes to write.
	 * @throws IllegalArgumentException if the specified number of bytes cannot be written.
	 */
    private void checkWriteable(int numberOfBytes) {
	    
    	int writeableBytes = writeableBytes();
    	
    	if (numberOfBytes > writeableBytes) {
    		
    		@SuppressWarnings("boxing")
            String msg = format("the number of bytes to write (%d) exceed the number of writeable bytes (%d)",
                                numberOfBytes, 
                                writeableBytes);
    		
			throw new IndexOutOfBoundsException(msg);
    	}	
    }

    /**
     * {@inheritDoc}
     */
    @Override
	public boolean equals(Object object) {
	    
		if (object == this) {
		    return true;
	    }
	    if (!(object instanceof Buffer)) {
		    return false;
	    }
	    Buffer rhs = (Buffer) object;

	    if (rhs.readableBytes() != this.readableBytes()) {
	    	return false;
	    }
	    
	    int i = readerIndex();
	    int j = rhs.readerIndex();
	    int m = readableBytes();
	    
	    while (i < m) {
	    	
	    	if (getByte(i++) != rhs.getByte(j++)) {
	    		return false;
	    	}
	    }
	    
	    return true;
	}

    /**
     * {@inheritDoc}
     */
    @Override
	public int hashCode() {
		
		HashCodeBuilder builder = new HashCodeBuilder(-1263385815, 438112389);
		
		for (int i = readerIndex(), m = readableBytes(); i < m; i++) {
			
			builder.append(getByte(i));
		}
		
		return builder.toHashCode();
	}

    /**
     * {@inheritDoc}
     */
	@Override
    public final void clear() {
		
		readerIndex(0);
		writerIndex(0);
    }
}
