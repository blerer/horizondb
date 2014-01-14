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
package io.horizondb.io.serialization;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;

import java.io.IOException;

/**
 * Utility to make a <code>String</code> Serializable.
 * 
 * @author Benjamin
 *
 */
public class SerializableString implements Serializable {

	/**
	 * The parser instance.
	 */
	private static final Parser<SerializableString> PARSER = new Parser<SerializableString>() {
	
		/**
		 * {@inheritDoc}
		 */
		@Override
		public SerializableString parseFrom(ByteReader reader) throws IOException {

			return new SerializableString(VarInts.readString(reader));
		}
	};	
	
	/**
	 * The <code>String</code>.
	 */
	private final String s;
		
	/**
	 * Creates a <code>SerializableString</code>.
	 * 
	 * @param s the string.
	 */
    public SerializableString(String s) {
	    this.s = s;
    }

    /**    
     * {@inheritDoc}
     */
	@Override
    public String toString() {
	    return this.s;
    }

	/**
	 * Creates a new <code>SerializableString</code> by reading the data from the specified reader.
	 * 
	 * @param reader the reader to read from.
	 * @throws IOException if an I/O problem occurs
	 */
	public static SerializableString parseFrom(ByteReader reader) throws IOException {
		
		return getParser().parseFrom(reader);
	}
	
	/**
	 * Returns the parser that can be used to deserialize <code>SerializableString</code> instances.
	 * @return the parser that can be used to deserialize <code>SerializableString</code> instances.
	 */
    public static Parser<SerializableString> getParser() {

	    return PARSER;
    }
    
	/**
	 * {@inheritDoc}
	 */
    @Override
    public int computeSerializedSize() {
	    return VarInts.computeStringSize(this.s);
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
    	VarInts.writeString(writer, this.s);
    }
}
