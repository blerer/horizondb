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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Header of the message sent between the client and the server.
 * 
 * @author Benjamin
 *
 */
public final class MsgHeader implements Serializable {

	/**
	 * The parser instance.
	 */
	private static final Parser<MsgHeader> PARSER = new Parser<MsgHeader>() {
	
		/**
		 * {@inheritDoc}
		 */
		@Override
		public MsgHeader parseFrom(ByteReader reader) throws IOException {
	    	
	    	byte magicNumber = reader.readByte();
			OpCode opCode = OpCode.parseFrom(reader);
			
			int status = 0;
			
			if (isRequestHeader(magicNumber)) {
				
				reader.skipBytes(2);
				
			} else {
				
				status = reader.readUnsignedShort();
			} 
			
			int payloadLength = reader.readInt();
			long opaque = reader.readUnsignedInt();
			
			return new MsgHeader(magicNumber, opCode, status, payloadLength, opaque);	
		}
	};	
	
	/**
	 * The value of the request magic number for the current protocol version.
	 */
	public static final byte REQUEST_MAGIC_NUMBER = (byte) 0x80;
	
	/**
	 * The value of the response magic number for the current protocol version.
	 */
	public static final byte RESPONSE_MAGIC_NUMBER = (byte) 0x81;
	
	/**
	 * The message header size in bytes.
	 */
	public static final int HEADER_SIZE = 12;
	
	/**
	 * The length offset in bytes.
	 */
	public static final int LENGTH_FIELD_OFFSET = 4;
	
	/**
	 * The length of the payload length in bytes.
	 */
	public static final int LENGTH_FIELD_LENGTH = 4;
	
	/**
	 * The magic number used to identify the protocol version (1 byte).
	 */
    private byte magicNumber;
	/**
	 * The operation code (1 byte).
	 */
    private OpCode opCode;
	/**
	 * The response status (2 bytes) if the message is a response.
	 */
    private int status;
	/**
	 * The length in bytes of the payload (4 bytes). Zero if the length is unknown.
	 */
    private int payloadLength;
    
	/**
	 * The same opaque value in the request is copied back in the response (4 bytes).
	 */
    private long opaque;
	
	/**
	 * The counter used to create the opaque value.
	 */
	private static final AtomicInteger COUNTER = new AtomicInteger();
	
	/**
	 * Creates a new <code>MsgHeaderBean</code> for a request message.
	 * 
	 * @param opCode the operation code
	 * @param payloadLength the length of the payload
	 * @return a new <code>MsgHeaderBean</code> for a request message.
	 */
	public static MsgHeader newRequestHeader(OpCode opCode, int payloadLength) {

		return new MsgHeader(REQUEST_MAGIC_NUMBER, opCode, 0, payloadLength, COUNTER.incrementAndGet());
	}

	/**
	 * Creates a new <code>MsgHeaderBean</code> for a response message to the specified request header.
	 * 
	 * @param requestHeader the request header
	 * @param status the response status
	 * @param payloadLength the length of the payload.
	 * @return a new <code>MsgHeaderBean</code> for a response message
	 */
	public static MsgHeader newResponseHeader(MsgHeader requestHeader, int status, int payloadLength) {

		return new MsgHeader(RESPONSE_MAGIC_NUMBER,
		                         requestHeader.opCode,
		                         status,
		                         payloadLength,
		                         requestHeader.opaque);
	}			

	/**
	 * Creates a new <code>MsgHeaderBean</code> for a response message from the server when the request
	 * could not be deserialized.
	 * 
	 * @param status the response status
	 * @param payloadLength the length of the payload.
	 * @return a new <code>MsgHeaderBean</code> for a response message
	 */
	public static MsgHeader newResponseHeader(int status, int payloadLength) {

		return new MsgHeader(RESPONSE_MAGIC_NUMBER,
		                     OpCode.UNKNOWN_OPERATION,
		                     status,
		                     payloadLength,
		                     0); // Cannot do better.
	}	
	
	public static MsgHeader parseFrom(ByteReader reader) throws IOException {
		
		return getParser().parseFrom(reader);
	}

	/**
	 * Returns the parser that can be used to deserialize <code>MsgHeader</code> instances.
	 * @return the parser that can be used to deserialize <code>MsgHeader</code> instances.
	 */
    public static Parser<MsgHeader> getParser() {
	    return PARSER;
    }
	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

    	writer.writeByte(this.magicNumber)
    	      .writeObject(this.opCode);
    	
    	if (isRequestHeader(this.magicNumber)) {
    		
    		writer.writeZeroBytes(2);
    		
    	} else {
    		
    		writer.writeUnsignedShort(this.status);
    	} 
    	
    	writer.writeInt(this.payloadLength);
    	writer.writeUnsignedInt(this.opaque);
    }
	
	/**
	 * Returns the magic number used to identify the protocol version.
	 *  
	 * @return the the magic number used to identify the protocol version.
	 */
    public byte getMagicNumber() {
    	return this.magicNumber;
    }

	/**
	 * Returns the operation code.
	 * 
	 * @return the operation code.
	 */
    public OpCode getOpCode() {
    	return this.opCode;
    }

    /**
     * Returns <code>true</code> if this header OpCode correspond to a mutation.
     * 
     * @return <code>true</code> if this header OpCode correspond to a mutation.
     */
    public boolean isMutation() {
    	return getOpCode().isMutation();
    }
    
    /**
     * Returns <code>true</code> if the status of this response is success, <code>false</code> otherwise.
     * 
     * @return <code>true</code> if the status of this response is success, <code>false</code> otherwise.
     */
    public boolean isSuccess() {
    	return getStatus() == 0;
    }
    
	/**
	 * Returns the status of the response.
	 * 
	 * @return the status of the response.
	 * @throws UnsupportedOperationException if the message is not a response.
	 */
    public int getStatus() {
    	
    	if (isRequestHeader(this.magicNumber)) {
    		throw new UnsupportedOperationException("The status field exist only for response message.");
    	}
    	
    	return this.status;
    }

	/**
	 * Returns the length of the payload or zero if the length of the payload is unknown.
	 * 
	 * @return the length of the payload or zero if the length of the payload is unknown.
	 */
    public int getPayloadLength() {
    	return this.payloadLength;
    }

	/**
	 * Returns the value of the opaque field that must be returned with the response.
	 * 
	 * @return the value of the opaque field that must be returned with the response.
	 */
    public long getOpaque() {
    	return this.opaque;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public int computeSerializedSize() {
        return HEADER_SIZE;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("magicNumber", this.magicNumber)
		                                                                  .append("opCode", this.opCode)
		                                                                  .append("status", this.status)
		                                                                  .append("payloadLength", this.payloadLength)
		                                                                  .append("opaque", this.opaque)
		                                                                  .toString();
	}
    
	/**
	 * Returns <code>true</code> if this header is the one of a request header, 
	 * <code>false</code> otherwise.
	 * @return <code>true</code> if this header is the one of a request header, 
	 * <code>false</code> otherwise.
	 */
	public boolean isRequestHeader() {
		
		return isRequestHeader(this.magicNumber);
	}
	
	/**
	 * Returns <code>true</code> if this header is the one of a response header, 
	 * <code>false</code> otherwise.
	 * @return <code>true</code> if this header is the one of a response header, 
	 * <code>false</code> otherwise.
	 */
	public boolean isResponseHeader() {
		
		return !isRequestHeader(this.magicNumber);
	}
	
	/**
	 * Returns <code>true</code> if the specified magic number is the one of a request header, 
	 * <code>false</code> otherwise.
	 * @return <code>true</code> if the specified magic number is the one of a request header, 
	 * <code>false</code> otherwise.
	 */
	private static boolean isRequestHeader(byte magicNumber) {
		
		return magicNumber == REQUEST_MAGIC_NUMBER;
	}
    
	/**
	 * Creates a new <code>MsgHeaderBean</code> with the specified properties.
	 *     
	 * @param magicNumber the magic number used to identify the protocol version
	 * @param opCode the operation code
	 * @param status the response status
	 * @param payloadLength the length in bytes of the payload
	 * @param opaque the same opaque value in the request is copied back in the response
	 */
	private MsgHeader(byte magicNumber, OpCode opCode, int status, int payloadLength, long opaque) {
		
		this.magicNumber = magicNumber;
		this.opCode = opCode;
		this.status = status;
		this.payloadLength = payloadLength;
		this.opaque = opaque;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object object) {
		
	    if (object == this) {
		    return true;
	    }
	    if (!(object instanceof MsgHeader)) {
		    return false;
	    }
	    MsgHeader rhs = (MsgHeader) object;
	    return new EqualsBuilder().append(this.opCode, rhs.opCode)
	                              .append(this.payloadLength, rhs.payloadLength)
	                              .append(this.status, rhs.status)
	                              .append(this.opaque, rhs.opaque)
	                              .append(this.magicNumber, rhs.magicNumber)
	                              .isEquals();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		
	    return new HashCodeBuilder(-865048143, -140533801).append(this.opCode)
	                                                      .append(this.payloadLength)
	                                                      .append(this.status)
	                                                      .append(this.opaque)
	                                                      .append(this.magicNumber)
	                                                      .toHashCode();
	}
	
	
}
