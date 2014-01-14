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
package io.horizondb.io.compression;

import java.io.IOException;

import io.horizondb.io.ByteWriter;
import io.horizondb.io.serialization.Serializable;
import io.netty.buffer.ByteBuf;

/**
 * @author Benjamin
 *
 */
public enum CompressionType implements Serializable {
	
	NO_COMPRESSION(0);
		
	/**
	 * The field type binary representation.
	 */
	private final int b; 
	
	/**
	 * Creates a new <code>FieldType</code> with the specified binary representation.
	 * 
	 * @param b the byte representing the <code>FieldType</code>.
	 */
	private CompressionType(int b) {
		
		this.b = b;
	}
	
	/**
	 * {@inheritDoc}
	 */	
	@Override
    public int computeSerializedSize() {

	    return 1;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeTo(ByteWriter writer) throws IOException {
		
		writer.writeByte(this.b);
	}

	/**
	 * Returns the type of field represented by the next readable byte in the specified buffer. 
	 * 
	 * @param buffer the buffer to read from.
	 * @return the type of field represented by the next readable byte in the specified buffer.
	 */
	public static CompressionType readCompressionType(ByteBuf buffer) {
		
		return toFieldType(buffer.readByte());
	}
	
	/**
	 * Returns the compression type represented by the next readable byte in the specified buffer. 
	 * 
	 * @param buffer the buffer to read from.
	 * @return the compression type represented by the next readable byte in the specified buffer.
	 */
    private static CompressionType toFieldType(int code) {
	    
    	CompressionType[] values = CompressionType.values();
    	
    	for (int i = 0; i < values.length; i++) {
    		
    		CompressionType compressionType = values[i];
    		
			if (compressionType.b == code) {
    			
				return compressionType;
    		}
    	}
    	
	    throw new IllegalStateException("The byte " + code + " does not match any compression type");
    }
}
