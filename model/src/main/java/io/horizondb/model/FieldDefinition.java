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
import static org.apache.commons.lang.Validate.notNull;

/**
 * The definition of a field in a record.
 * 
 * @author Benjamin
 */
@Immutable
public final class FieldDefinition implements Serializable {

	/**
	 * The parser instance.
	 */
	private static final Parser<FieldDefinition> PARSER = new Parser<FieldDefinition>() {
	
		/**
		 * {@inheritDoc}
		 */
		@Override
		public FieldDefinition parseFrom(ByteReader reader) throws IOException {
	    	
			return new FieldDefinition(VarInts.readString(reader), FieldType.parseFrom(reader));
		}
	};	
	
	/**
	 * The name of the field.
	 */
	private final String name;
	
	/**
	 * The type of the field.
	 */
	private final FieldType type;

	/**
	 * Creates a new <code>FieldDefinition</code> instance with the specified name and type.
	 * 
	 * @param name the field name
	 * @param type the field type
	 * @return a new <code>FieldDefinition</code> instance.
	 */
	public static FieldDefinition newInstance(String name, FieldType type) {
		
		return new FieldDefinition(name, type);
	}
	
	/**
	 * Creates a new <code>FieldDefinition</code> by reading the data from the specified reader.
	 * 
	 * @param reader the reader to read from.
	 * @throws IOException if an I/O problem occurs
	 */
	public static FieldDefinition parseFrom(ByteReader reader) throws IOException {
		
		return getParser().parseFrom(reader);
	}
	
	/**
	 * Returns the parser that can be used to deserialize <code>FieldDefinition</code> instances.
	 * @return the parser that can be used to deserialize <code>FieldDefinition</code> instances.
	 */
    public static Parser<FieldDefinition> getParser() {

	    return PARSER;
    }

	/**
     * Returns the type of this field.
     *     
     * @return the type of this field.
     */
	public FieldType getType() {
		return this.type;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object object) {
	    if (object == this) {
		    return true;
	    }
	    if (!(object instanceof FieldDefinition)) {
		    return false;
	    }
	    FieldDefinition rhs = (FieldDefinition) object;
	    
	    return new EqualsBuilder().append(this.name, rhs.name)
	                              .append(this.type, rhs.type)
	                              .isEquals();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		
	    return new HashCodeBuilder(-424505767, -612153919).append(this.name)
	                                                      .append(this.type)
	                                                      .toHashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
	    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("name", this.name)
	    		                                                          .append("type", this.type)
	    		                                                          .toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int computeSerializedSize() {
		
		return VarInts.computeStringSize(this.name) + this.type.computeSerializedSize();
	}
	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
	    
    	VarInts.writeString(writer, this.name);
    	this.type.writeTo(writer);
    }

	/**
	 * Creates a new <code>FieldDefinition</code>.
	 * 
	 * @param name the field name.
	 * @param type the field type.
	 */
    private FieldDefinition(String name, FieldType type) {

    	notEmpty(name, "the name parameter must not be empty.");
    	notNull(type, "the type parameter must not be null.");
    	
	    this.name = name;
	    this.type = type;
    }
}
