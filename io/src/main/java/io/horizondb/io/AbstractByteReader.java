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

import java.io.IOException;
import java.nio.ByteOrder;

import io.horizondb.io.encoding.Endianness;

/**
 * Base class for the <code>ByteReader</code>s.
 * 
 * @author Benjamin
 *
 */
public abstract class AbstractByteReader implements ByteReader {

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
    public ByteReader order(ByteOrder order) {
		
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
    public final short readShort() throws IOException {
		return this.endianness.readShort(this);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public final int readUnsignedShort() throws IOException {
		return this.endianness.readUnsignedShort(this);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public final int readInt() throws IOException {
		return this.endianness.readInt(this);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public final long readUnsignedInt() throws IOException {
		return this.endianness.readUnsignedInt(this);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public final long readLong() throws IOException {
	    return this.endianness.readLong(this);
    }

	protected Endianness getEndianness() {
		return this.endianness;
	}
}
