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
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;

import java.io.IOException;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * @author Benjamin
 *
 */
public final class DataChunk implements Serializable {

	/**
     * The parser instance.
     */
    private static final Parser<DataChunk> PARSER = new Parser<DataChunk>() {

	    /**
	     * {@inheritDoc}
	     */
	    @Override
	    public DataChunk parseFrom(ByteReader reader) throws IOException {
	    	
	    	ReadableBuffer buffer = (ReadableBuffer) reader;
	    	
		    return new DataChunk(buffer);
	    }
    };
	
	/**
	 * The buffer containing the data.
	 */
	private final ReadableBuffer buffer;

	/**
	 * Creates a new data chunk containing the specified bytes.
	 * 
	 * @param buffer the buffer
	 */
    public DataChunk(ReadableBuffer buffer) {
	    this.buffer = buffer;
    }

    /**
     * Creates a new <code>DataChunk</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static DataChunk parseFrom(ByteReader reader) throws IOException {

	    return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>DataChunk</code> instances.
     * @return the parser that can be used to deserialize <code>DataChunk</code> instances.
     */
    public static Parser<DataChunk> getParser() {

	    return PARSER;
    }
    
	
    public ReadableBuffer getBuffer() {
		return this.buffer;
	}
    
	/**
	 * {@inheritDoc}
	 */
    @Override
    public int computeSerializedSize() {
	    return this.buffer.readableBytes(); 
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
    	writer.transfer(this.buffer);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object object) {
		
	    if (object == this) {
		    return true;
	    }
	    if (!(object instanceof DataChunk)) {
		    return false;
	    }
	    DataChunk rhs = (DataChunk) object;
	    return new EqualsBuilder().append(this.buffer, rhs.buffer).isEquals();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
	    return new HashCodeBuilder(952880905, 769187925).append(this.buffer).toHashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("buffer", this.buffer).toString();
	}

	
}
