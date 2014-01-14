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

import io.horizondb.io.encoding.Endianness;
import io.horizondb.io.serialization.Serializable;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Base class for the <code>ByteWriter</code>s classes.
 * 
 * @author Benjamin
 *
 */
public abstract class AbstractByteWriter implements ByteWriter {

	/**
	 * The Endianness used to read the bytes.
	 */
	private Endianness endianness = Endianness.getEndianness(ByteOrder.nativeOrder());

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
    public final ByteWriter order(ByteOrder order) {
		
		if (order != order()) {
			this.endianness = Endianness.getEndianness(order);
		}

	    return this;
    }
	
	/**	
	 * {@inheritDoc}
	 */	
	@Override
    public ByteWriter writeBoolean(boolean b) throws IOException {

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
    public ByteWriter writeShort(short s) throws IOException {

		this.endianness.writeShort(this, s);
	    return this;
    }

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public ByteWriter writeUnsignedShort(int s) throws IOException {
		
		this.endianness.writeUnsignedShort(this, s);
	    return this;
    }

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public ByteWriter writeInt(int i) throws IOException {
		
		this.endianness.writeInt(this, i);
	    return this;
    }

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public ByteWriter writeUnsignedInt(long l) throws IOException {
		
		this.endianness.writeUnsignedInt(this, l);
	    return this;
    }

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public ByteWriter writeLong(long l) throws IOException {
		
		this.endianness.writeLong(this, l);
	    return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public final ByteWriter writeObject(Serializable serializable) throws IOException {

		serializable.writeTo(this);
		
	    return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public final ByteWriter writeBytes(byte[] bytes) throws IOException {

		writeBytes(bytes, 0, bytes.length);
		
	    return this;
    }

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public ByteWriter transfer(ByteReader reader) throws IOException {
	    
		while (reader.isReadable()) {
			
			writeByte(reader.readByte());
		}
		
	    return this;
    }
}
