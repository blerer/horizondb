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
 * A data buffer.
 * 
 * @author Benjamin
 *
 */
public interface Buffer extends ReadableBuffer, WritableBuffer, SliceableBuffer {
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer order(ByteOrder order);
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer skipBytes(int numberOfBytes);

	/**
	 * {@inheritDoc}
	 */
	@Override
    byte readByte();

	/**
	 * {@inheritDoc}
	 */
	@Override
    Buffer readBytes(byte[] bytes);
    
	/**
	 * {@inheritDoc}
	 */
	@Override
    Buffer readBytes(byte[] bytes, int offset, int length);

	/**
	 * {@inheritDoc}
	 */
	@Override
    short readShort();
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	int readUnsignedShort();
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	int readInt();
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	long readUnsignedInt();
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	long readLong();
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    boolean isReadable();
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer writeByte(int b);
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    ByteWriter writeBoolean(boolean b);
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer writeBytes(byte[] bytes);
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer writeBytes(byte[] bytes, int offset, int length);
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer writeZeroBytes(int length);
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer writeObject(Serializable serializable) throws IOException;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer transfer(ByteReader reader) throws IOException;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer writerIndex(int index);
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer setByte(int index, int b);
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer setShort(int index, short s);
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer setUnsignedShort(int index, int s);

	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer setInt(int index, int i);
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer setLong(int index, long l);
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	Buffer setUnsignedInt(int index, long l);
	
    /**
     * Returns <code>true</code> if  this buffer is backed by a byte array.
     * 
     * <p> If this method returns <tt>true</tt> then the {@link #array() array}
     * and {@link #arrayOffset() arrayOffset} methods may safely be invoked.</p>
     *
     * @return <tt>true</tt> if, and only if, this buffer is backed by an array
     */
	boolean hasArray();
	
    /**
     * Returns the byte array that backs this buffer <i>(optional operation)</i>.
     *
     * <p> Modifications to this buffer's content will cause the returned
     * array's content to be modified, and vice versa.</p>
     *
     * <p> The {@link #hasArray hasArray} method must be invoked before this
     * method in order to ensure that this buffer has a backing array. </p>
     *
     * @return The array that backs this buffer
     * @throws UnsupportedOperationException if this buffer is not backed by an array
     */
    byte[] array();
    
    /**
     * Returns the offset of the first byte within the backing byte array of
     * this buffer.
     *
     * @throws UnsupportedOperationException if this buffer is not backed by an array
     */
    int arrayOffset();
    
    /**
     * Checks if this buffer is direct.
     *
     * @return <code>true<code> if this buffer is direct, <code>false</code> otherwise.
     */
    boolean isDirect();
    
	/**
	 * Returns the capacity of this buffer.
	 * 
	 * @return the capacity of this buffer.
	 */
	int capacity();
	
	/**
	 * Sets the reader and writer index to zero.
	 */
	void clear();
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    Buffer slice(int length);
}
