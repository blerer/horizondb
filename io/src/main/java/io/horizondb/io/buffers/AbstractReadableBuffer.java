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
import io.horizondb.io.encoding.Endianness;

import java.io.IOException;
import java.nio.ByteOrder;

import static io.horizondb.io.encoding.Endianness.INT_LENGTH;
import static io.horizondb.io.encoding.Endianness.LONG_LENGTH;
import static io.horizondb.io.encoding.Endianness.SHORT_LENGTH;
import static java.lang.String.format;

/**
 * @author Benjamin
 *
 */
abstract class AbstractReadableBuffer implements ReadableBuffer {

	/**
	 * The Endianness used to read the bytes.
	 */
	protected Endianness endianness = Endianness.getEndianness(ByteOrder.nativeOrder());
	
	/**
	 * The reader index.
	 */
	protected int readerIndex;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public final ByteOrder order() {
	    return this.endianness.order();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public ReadableBuffer order(ByteOrder order) {
		
		if (order != order()) {
			this.endianness = Endianness.getEndianness(order);
		}

	    return this;
    }

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public final boolean readBoolean() throws IOException {
	    return readByte() == 1;
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public final short readShort() {
		
		checkReadable(SHORT_LENGTH);
		
		short s = getShort(this.readerIndex);
		this.readerIndex += SHORT_LENGTH;	
		
		return s;
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public final short getShort(int index) {

		return this.endianness.getShort(this, index);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public final int readUnsignedShort() {
		
		checkReadable(SHORT_LENGTH);
		
		int s = getUnsignedShort(this.readerIndex);
		this.readerIndex += SHORT_LENGTH;	
		
		return s;
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public final int getUnsignedShort(int index) {

		return this.endianness.getUnsignedShort(this, index);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public final int readInt() {
		
		checkReadable(INT_LENGTH);
		
		int i = getInt(this.readerIndex);
		this.readerIndex += INT_LENGTH;	
		
		return i;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public final int getInt(int index) {

		return this.endianness.getInt(this, index);
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public final long readUnsignedInt() {
		checkReadable(INT_LENGTH);
		
		long i = getUnsignedInt(this.readerIndex);
		this.readerIndex += INT_LENGTH;	
		
		return i;
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public final long getUnsignedInt(int index) {

		return this.endianness.getUnsignedInt(this, index);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public final long readLong() {
		checkReadable(LONG_LENGTH);
		
		long l = this.endianness.getLong(this, this.readerIndex);
		this.readerIndex += LONG_LENGTH;	
		
		return l;
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public final long getLong(int index) {

		return this.endianness.getLong(this, index);
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
}
