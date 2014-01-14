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
package io.horizondb.protocol;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.Error;

import java.io.IOException;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import static org.apache.commons.lang.Validate.notNull;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Benjamin
 *
 */
public final class Msg<T extends Serializable> implements Serializable {
	
	/**
	 * The marker for the end of stream.
	 */
	public static final byte END_OF_STREAM_MARKER = (byte) 0x80;
	
	/**
	 * The parser instance.
	 */
	private static final Parser<Msg<?>> PARSER = new Parser<Msg<?>>() {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public Msg<?> parseFrom(ByteReader reader) throws IOException {

			MsgHeader header = MsgHeader.parseFrom(reader);
			
			if (header.isResponseHeader() && !header.isSuccess()) {
				
				Error error = Error.parseFrom(reader);
				return newMsg(header, error);	
			}
			
			OpCode opCode = header.getOpCode();
			
			Parser<?> payloadParser = opCode.getPayloadParser(header.isRequestHeader());
		
			return newMsg(header, payloadParser.parseFrom(reader));			
		}
	};
	
	/**
	 * The message header.
	 */
	private final MsgHeader header;
	
	/**
	 * The message data.
	 */
	private final T payload;

	
	public static <S extends Serializable> Msg<S> newRequestMsg(OpCode opCode, S payload) {
		
		return new Msg<S>(MsgHeader.newRequestHeader(opCode, payload.computeSerializedSize()), payload);
	}
	
	public static <S extends Serializable> Msg<S> newResponseMsg(MsgHeader header, S payload) {
		
		return new Msg<S>(MsgHeader.newResponseHeader(header, 0, payload.computeSerializedSize()), payload);
	}
	
	public static Msg<Error> newErrorMsg(MsgHeader header, Error error) {
		
		return new Msg<Error>(MsgHeader.newResponseHeader(header, error.getCode(), error.computeSerializedSize()),
		                      error);
	}
	
	public static Msg<Error> newErrorMsg(Error error) {
		
		return new Msg<Error>(MsgHeader.newResponseHeader(error.getCode(), error.computeSerializedSize()),
		                      error);
	}
	
	public static <S extends Serializable> Msg<S> newMsg(MsgHeader header, S payload) {
		
		return new Msg<S>(header, payload);
	}
	
	/**
	 * Creates a new message without payload. 
	 * 
	 * @param header the message header.
	 * @return a new message with an empty payload.
	 */
	public static Msg<?> emptyMsg(MsgHeader header) {
		
		return new Msg<Serializable>(header, null);
	}

	/**
	 * Returns the message header.
	 * 
	 * @return the message header.
	 */
	public MsgHeader getHeader() {
		return this.header;
	}

    /**
     * Returns <code>true</code> if this OpCode correspond to a mutation.
     * 
     * @return <code>true</code> if this OpCode correspond to a mutation.
     */
    public boolean isMutation() {
    	return getHeader().isMutation();
    }
	
	/**
	 * Returns <code>true</code> if the message contains a payload, <code>false</code> otherwise.
	 *  
	 * @return <code>true</code> if the message contains a payload, <code>false</code> otherwise.
	 */
	public boolean hasPayload() {
		
		return this.payload != null;
	}
	
	/**
	 * Returns the message payload.
	 * 
	 * @return the message payload.
	 */
	public T getPayload() {
		return this.payload;
	}
	
	/**
	 * Creates a new <code>Msg</code> by reading the data from the specified reader.
	 * 
	 * @param reader the reader to read from.
	 * @throws IOException if an I/O problem occurs
	 */
	public static Msg<?> parseFrom(ByteReader reader) throws IOException {
		
		return getParser().parseFrom(reader);
	}
	
	/**
	 * Returns the parser that can be used to deserialize <code>Msg</code> instances.
	 * @return the parser that can be used to deserialize <code>Msg</code> instances.
	 */
    public static Parser<Msg<?>> getParser() {

	    return PARSER;
    }	
	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public final int computeSerializedSize() {

	    return this.header.computeSerializedSize() + this.header.getPayloadLength();
    }

    /**
     * {@inheritDoc}
     */
    @Override
	public String toString() {
	    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("header", this.header)
	    		                                                          .append("payload", this.payload)
	    		                                                          .toString();
	}

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
    	
    	getHeader().writeTo(writer);
    	
    	if (hasPayload()) {
    	
    		this.payload.writeTo(writer);
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
	    if (!(object instanceof Msg)) {
		    return false;
	    }
	    Msg<?> rhs = (Msg<?>) object;
	    return new EqualsBuilder().append(this.payload, rhs.payload)
	                              .append(this.header, rhs.header)
	                              .isEquals();
	}

	/**
	 * {@inheritDoc}
	 */
    @Override
	public int hashCode() {
	    return new HashCodeBuilder(2034116953, 1305262815).append(this.payload)
	                                                      .append(this.header)
	                                                      .toHashCode();
	}
    
	/**
	 * 
	 */
	private Msg(MsgHeader header, T payload) {
		
		notNull(header, "the header parameter must not be null.");
		
		this.header = header;
		this.payload = payload;
	}
}
