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
package io.horizondb.model;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import static org.apache.commons.lang.Validate.notEmpty;

/**
 * The content of the error message.
 * 
 * @author Benjamin
 *
 */
@Immutable
public final class Error implements Serializable {

	/**
	 * The parser instance.
	 */
	private static final Parser<Error> PARSER = new Parser<Error>() {
	
		/**
		 * {@inheritDoc}
		 */
		@Override
		public Error parseFrom(ByteReader reader) throws IOException {
	    	
			int code = VarInts.readInt(reader);
			String message = VarInts.readString(reader);
			
			return new Error(code, message);
		}
	};	
	
	/**
	 * The error message.
	 */
	private final String message;
	
	/**
	 * The error code.
	 */
	private final int code;

	/**
	 * Creates a new <code>FieldDefinition</code> instance with the specified name and type.
	 * 
	 * @param name the field name
	 * @param type the field type
	 * @return a new <code>FieldDefinition</code> instance.
	 */
	public Error(int code, String message) {
		
		notEmpty(message, "the message parameter must not be empty.");
		
		this.code = code;
		this.message = message;
	}
	
	/**
	 * Returns the error message.
	 * 
	 * @return the error message.
	 */	
	public String getMessage() {
		return this.message;
	}

	/**
	 * Returns the error code.
	 * 
	 * @return the error code.
	 */
	public int getCode() {
		return this.code;
	}

	/**
	 * Creates a new <code>Error</code> by reading the data from the specified reader.
	 * 
	 * @param reader the reader to read from.
	 * @throws IOException if an I/O problem occurs
	 */
	public static Error parseFrom(ByteReader reader) throws IOException {
		
		return getParser().parseFrom(reader);
	}
	
	/**
	 * Returns the parser that can be used to deserialize <code>Error</code> instances.
	 * @return the parser that can be used to deserialize <code>Error</code> instances.
	 */
    public static Parser<Error> getParser() {

	    return PARSER;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object object) {
	    if (object == this) {
		    return true;
	    }
	    if (!(object instanceof Error)) {
		    return false;
	    }
	    Error rhs = (Error) object;
	    
	    return new EqualsBuilder().append(this.code, rhs.code)
	                              .append(this.message, rhs.message)
	                              .isEquals();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
	    return new HashCodeBuilder(-992334025, -1668721805).append(this.message)
	                                                       .append(this.code)
	                                                       .toHashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
	    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("code", this.code)
	    		                                                          .append("message", this.message)
	    		                                                          .toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int computeSerializedSize() {
		
		return VarInts.computeIntSize(this.code) + VarInts.computeStringSize(this.message);
	}
	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
	    
    	VarInts.writeInt(writer, this.code);
    	VarInts.writeString(writer, this.message);
    }
}
