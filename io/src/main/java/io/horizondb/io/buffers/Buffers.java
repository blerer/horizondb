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

import java.nio.ByteBuffer;


/**
 * <code>Buffer</code> factory methods.
 * 
 * @author Benjamin
 *
 */
public final class Buffers {

    /**
     * A buffer whose capacity is {@code 0}.
     */
    public static final Buffer EMPTY_BUFFER = new HeapBuffer(0);
	
	/**
	 * Allocates a new buffer with the specified capacity. 
	 * 
	 * @param capacity the buffer capacity.
	 * @return a new buffer with the specified capacity. 
	 */
	public static Buffer allocate(int capacity) {
		
		if (capacity == 0) {
			return EMPTY_BUFFER;
		}
		
		return new HeapBuffer(capacity);
	}
	
	/**
	 * Wraps the specified direct <code>ByteBuffer</code> into a buffer. 
	 * 
	 * @param directBuffer the direct buffer to wrap.
	 * @return the direct buffer wrapped into a buffer. 
	 */
	public static Buffer wrap(ByteBuffer directBuffer) {

		return new DirectBuffer(directBuffer);
	}
	
	/**
	 * Wraps the specified <code>ByteBuf</code> into a buffer. 
	 * 
	 * @param buffer the <code>ByteBuf</code> to wrap.
	 * @return the <code>ByteBuf</code> wrapped into a buffer. 
	 */
	public static Buffer wrap(ByteBuf buffer) {

		return new NettyBuffer(buffer);
	}
	
	/**
	 * Allocates a new direct buffer. 
	 * 
	 * @param capacity the buffer capacity.
	 * @return a new buffer with the specified capacity. 
	 */
	public static Buffer allocateDirect(int capacity) {
		
		if (capacity == 0) {
			return EMPTY_BUFFER;
		}
		
		return new DirectBuffer(capacity);
	}
	
	/**
	 * Wraps the specified byte array into a buffer. 
	 * 
	 * @param array the array to wrap
	 * @return the byte array wrapped into a buffer
	 */
	public static Buffer wrap(byte[] array) {

		return new HeapBuffer(array);
	}
    
	/**
	 * Wraps the specified portion of the specified byte array into a buffer. 
	 * 
	 * @param array the array to wrap
	 * @param offset the offset within the byte array
	 * @param length the number of bytes from the byte array
	 * @return the byte array wrapped into a buffer 
	 */
	public static Buffer wrap(byte[] array, int offset, int length) {

		return new HeapBuffer(array).subRegion(offset, length);
	}
	
	/**
	 * Must not be instantiated.
	 */
	private Buffers() {
	}
}
