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

/**
 * A database definition.
 * 
 * @author Benjamin
 *
 */
@Immutable
public class DatabaseDefinition implements Serializable {

	/**
	 * The parser instance.
	 */
	private static final Parser<DatabaseDefinition> PARSER = new Parser<DatabaseDefinition>() {

		/**
		 * {@inheritDoc}
		 */
		@Override
        public DatabaseDefinition parseFrom(ByteReader reader) throws IOException {
	        
			String name = VarInts.readString(reader);
			
			return new DatabaseDefinition(name);
        }
	};

	/**
	 * The database name.
	 */
	private String name;
	
	/**
	 * Creates a new meta data with the specified name.
	 * @param name the database name.
	 */
	public DatabaseDefinition(String name) {
		
		this.name = name;
	}

	/**
	 * Creates a new <code>DatabaseDefinition</code> by reading the data from the specified reader.
	 * 
	 * @param reader the reader to read from.
	 * @throws IOException if an I/O problem occurs
	 */
	public static DatabaseDefinition parseFrom(ByteReader reader) throws IOException {
		
		return getParser().parseFrom(reader);
	}
	
	/**
	 * Returns the parser that can be used to deserialize <code>DatabaseDefinition</code> instances.
	 * 
	 * @return the parser that can be used to deserialize <code>DatabaseDefinition</code> instances.
	 */
	public static Parser<DatabaseDefinition> getParser() {
	    return PARSER;
    }

	/**
	 * Returns the database name.
	 * 	
	 * @return the database name.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Returns a new time series builder.
	 * 
	 * @param seriesName the name of the new time series
	 * @return a new time series builder.
	 */
	public TimeSeriesDefinition.Builder newTimeSeriesDefinitionBuilder(String seriesName) {
		
		return TimeSeriesDefinition.newBuilder(this.getName(), seriesName);
	}
	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public int computeSerializedSize() {
	    return VarInts.computeStringSize(this.name);
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
    	VarInts.writeString(writer, this.name);
    }
    	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object object) {
	    if (object == this) {
		    return true;
	    }
	    if (!(object instanceof DatabaseDefinition)) {
		    return false;
	    }
	    DatabaseDefinition rhs = (DatabaseDefinition) object;
	    return new EqualsBuilder().append(this.name, rhs.name)
	                              .isEquals();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
	    return new HashCodeBuilder(235543691, -72117241).append(this.name)
	                                                    .toHashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
	    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("name", this.name).toString();
	}
	
	
}
