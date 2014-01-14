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
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * A composite <code>Serializable</code>.
 * 
 * @author Benjamin
 *
 */
@Immutable
public final class Serializables<T extends Serializable> implements Serializable {
	
	/**
	 * The composing serializables.
	 */
	private final List<T> serializables;
	
	/**
	 * Creates a new composite composed of the specified serializables
	 * 
	 * @param serializables the serializables.
	 */
    public Serializables(List<T> serializables) {

	    this.serializables = new ArrayList<>(serializables);
    }
	
	/**
	 * Returns the serializable at the specified index.
	 * 
	 * @param index the serializable index.
	 * @return the serializable at the specified index.
	 */
	public T get(int index) {
		
		return this.serializables.get(index);
	}
	
	/**
	 * Returns the number of serializables composing this <code>Serializables</code>.
	 * @return the number of serializables composing this <code>Serializables</code>.
	 */
	public int size() {
		
		return this.serializables.size();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int computeSerializedSize() {

		int size = VarInts.computeUnsignedIntSize(this.serializables.size());
		
		for (int i = 0, m = this.serializables.size(); i < m; i++) {
			
			size += this.serializables.get(i).computeSerializedSize();
		}

		return size;
	}

	public static <S extends Serializable> Serializables<S> parseFrom(Parser<S> parser, ByteReader reader) 
			throws IOException {
		
		return new SerializablesParser<S>(parser).parseFrom(reader);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeTo(ByteWriter writer) throws IOException {
		
		VarInts.writeUnsignedInt(writer, this.serializables.size());
		
		for (int i = 0, m = this.serializables.size(); i < m; i++) {
			
			this.serializables.get(i).writeTo(writer);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object object) {
	    if (object == this) {
		    return true;
	    }
	    if (!(object instanceof Serializables)) {
		    return false;
	    }
	    @SuppressWarnings("unchecked")
        Serializables<T> rhs = (Serializables<T>) object;
	    return new EqualsBuilder().append(this.serializables, rhs.serializables)
	                              .isEquals();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
	    return new HashCodeBuilder(252469533, 1404885147).append(this.serializables)
	                                                     .toHashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("serializables", this.serializables)
		                                                                  .toString();
	}
	
	/**
	 * The parser for the <code>Serializables</code>.
	 *
	 * @param <T> the type of the composing <code>Serializable</code>
	 */
	private static final class SerializablesParser<T extends Serializable> implements Parser<Serializables<T>> {

		/**
		 * The parser for the serializable instances.
		 */
		private final Parser<T> parser;
		
        public SerializablesParser(Parser<T> parser) {
	        this.parser = parser;
        }

		/**
		 * {@inheritDoc}
		 */
        @Override
        public Serializables<T> parseFrom(ByteReader reader) throws IOException {

        	int size = VarInts.readUnsignedInt(reader);
        	
        	List<T> serializables = new ArrayList<>(size);
        	
        	for (int i = 0; i < size; i++) {
        		
        		serializables.add(this.parser.parseFrom(reader));        		
        	}
        	
	        return new Serializables<>(serializables);
        }
	}
}
