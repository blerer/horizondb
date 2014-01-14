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
package io.horizondb.model.records;

import io.horizondb.io.ByteWriter;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.model.Field;
import io.horizondb.model.FieldType;
import io.horizondb.model.Record;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


/**
 * @author Benjamin
 *
 */
public class BinaryTimeSeriesRecord extends AbstractTimeSeriesRecord {
	
    /**
     * Returns <code>true</code> if the bitSet has been deserialized, <code>false</code> otherwise.
     */
    private boolean bitSetDeserialized;
    
    /**
     * Specify if this record is a delta or a full record.
     */
    private boolean delta;
    
    /**
     * The index marking the position of the next non-deserialized field.
     */
    private int deserializationIndex;
    
	/**
	 * The binary data to read from.
	 */
	private ReadableBuffer buffer;
	
	/**
	 * The number of bytes in the buffer.
	 */
	private int bufferSize;
	
	/**
	 * 
	 */
	public BinaryTimeSeriesRecord(int recordType, TimeUnit timestampUnit, FieldType... fieldTypes) {

		super(recordType, timestampUnit, fieldTypes);
	}

	/**
	 * Copy constructor.
	 * @param record the record to copy.
	 */
	private BinaryTimeSeriesRecord(BinaryTimeSeriesRecord record) {

		super(record.getType(), deepCopy(record.fields));
		
		this.bitSetDeserialized = record.bitSetDeserialized;
	    this.delta = record.delta;
	    this.deserializationIndex = record.deserializationIndex;
        this.buffer = record.buffer.duplicate();
        this.buffer.readerIndex(record.buffer.readerIndex());
		this.bufferSize = record.bufferSize;
	}
	
	/**	
	 * {@inheritDoc}
	 */
	@Override
    public Record newInstance() {
	    return new BinaryTimeSeriesRecord(this);
    }

	public BinaryTimeSeriesRecord fill(ReadableBuffer reader) {
		
		this.bitSetDeserialized = false;
		this.deserializationIndex = 0;
		
		this.buffer = reader;
		this.bufferSize = reader.readableBytes();
		
		return this;
	}

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public boolean isDelta() throws IOException {

		deserializedBitSetIfNeeded();
	    return this.delta;
    }

	/**
	 * {@inheritDoc}
	 * @throws IOException 
	 */
    @Override
    public Field getField(int index) throws IOException {
	    
    	deserializedFieldIfNeeded(index);
    	
	    return this.fields[index];
    }

    /**    
     * {@inheritDoc}
     */
	@Override
    public Field[] getFields() throws IOException {
		
		deserializedFieldIfNeeded(this.fields.length - 1);
		
	    return this.fields;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
    	
    	this.bitSetDeserialized = false;
    	this.deserializationIndex = 0;
    	
    	this.buffer.readerIndex(0);
    	
    	// TODO: Optimize with bulk move.
    	while (this.buffer.isReadable()) {
    		
    		writer.writeByte(this.buffer.readByte());
    	}
    }
    
    /**
     *     
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesRecord toTimeSeriesRecord() throws IOException {

    	return new TimeSeriesRecord(getType(), getFields());
    }

	/**    
     * {@inheritDoc}
     */
	@Override
    public int computeSerializedSize() {

	    return this.bufferSize;
    }

	/**
	 * @param index
	 * @throws IOException 
	 */
    private void deserializedFieldIfNeeded(int index) throws IOException {

    	deserializedBitSetIfNeeded();
    	
    	while (this.deserializationIndex <= index) {

    		if (this.bitSet.readBit()) {
    			this.fields[this.deserializationIndex].readFrom(this.buffer);
    		} else {
    			this.fields[this.deserializationIndex].setValueToZero();
    		}
    		
    		this.deserializationIndex++;
    	}
    }

	/**
	 * @throws IOException 
	 * 
	 */
    private void deserializedBitSetIfNeeded() throws IOException {
    	
    	if (!isBitSetDeserialized()) {
    	
    		this.buffer.readerIndex(0);
	    	this.bitSet.fill(VarInts.readUnsignedLong(this.buffer));
	    	this.delta = this.bitSet.readBit();
	    	this.bitSetDeserialized = true;
    	}	
    }

    /**
     * Checks if the <code>BitSet</code> has already been deserialized.
     *  
     * @return <code>true</code> if the <code>BitSet</code> has already been deserialized, <code>false</code> otherwise.
     */
    private boolean isBitSetDeserialized() {

        return this.bitSetDeserialized;
    }
}
