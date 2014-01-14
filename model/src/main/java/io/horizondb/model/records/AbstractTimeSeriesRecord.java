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

import io.horizondb.io.BitSet;
import io.horizondb.model.Field;
import io.horizondb.model.FieldType;
import io.horizondb.model.Record;
import io.horizondb.model.fields.TimestampField;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


/**
 * @author Benjamin
 *
 */
abstract class AbstractTimeSeriesRecord implements Record {

	/**
	 * The type of this record.
	 */
	private final int type;
	
	/**
	 * The fields of this record.
	 */
	protected final Field[] fields;
		
    /**
     * The <code>BitSet</code> used to track down the position of the fields with a value of zero.
     */
	protected final BitSet bitSet;
	
	/**
	 * 
	 */
	public AbstractTimeSeriesRecord(int recordType, TimeUnit timestampUnit, FieldType... fieldTypes) {
		
		this(recordType, createFields(timestampUnit, fieldTypes));
	}

	/**
	 * 
	 */
	protected AbstractTimeSeriesRecord(int recordType, Field... fields) {
		
		this.type = recordType;
		this.fields = fields;
		this.bitSet = new BitSet(this.fields.length + 1);
	}
	
	/**	
	 * {@inheritDoc}
	 */
    @Override
    public int getType() {
	    return this.type;
    }

	/**
     * {@inheritDoc}
     */
	@Override
    public final int getByte(int index) throws IOException {
		
		return getField(index).getByte();
	}
	
    /**
     * {@inheritDoc}
     */
	@Override
    public final int getInt(int index) throws IOException {
		
		return getField(index).getInt();
	}
	
    /**
     * {@inheritDoc}
     */
	@Override
    public final long getLong(int index) throws IOException {
		
		return getField(index).getLong();
	}
	
    /**
     * {@inheritDoc}
     */
	@Override
    public final double getDouble(int index) throws IOException {
		
		return getField(index).getDouble();
	}
	
    /**
     * {@inheritDoc}
     */
	@Override
    public final long getTimestampInNanos(int index) throws IOException {
		
		return getField(index).getTimestampInNanos();
	}
	
    /**
     * {@inheritDoc}
     */
	@Override
    public final long getTimestampInMicros(int index) throws IOException {
		
		return getField(index).getTimestampInMicros();
	}
	
    /**
     * {@inheritDoc}
     */
	@Override
    public final long getTimestampInMillis(int index) throws IOException {
		
		return getField(index).getTimestampInMillis();
	}
	
    /**
     * {@inheritDoc}
     */
	@Override
    public final long getTimestampInSeconds(int index) throws IOException {

		return getField(index).getTimestampInSeconds();
	}
	
    /**
     * {@inheritDoc}
     */
	@Override
    public final long getDecimalMantissa(int index) throws IOException {
	    
		return getField(index).getDecimalMantissa();
    }

    /**
     * {@inheritDoc}
     */
	@Override
    public final byte getDecimalExponent(int index) throws IOException {
		
		return getField(index).getDecimalExponent();
    }

	/**
	 * 
	 * {@inheritDoc}
	 */
    @Override
    public void copyTo(TimeSeriesRecord record) throws IOException {
	    
    	for (int i = 0; i < this.fields.length; i++) {
	        getField(i).copyTo(record.getField(i));
        }
    }
	
    /**
     * Creates a deep copy of the specified fields.
     * 
     * @param fields the fields
     * @return a deep copy of the specified fields.
     */
	protected static Field[] deepCopy(Field[] fields) {
		
		Field[] copy = new Field[fields.length];
		
		for (int i = 0; i < fields.length; i++) {
	        Field field = fields[i];
	        copy[i] = field.newInstance();	        
        }
		
		return copy;
	}
    
	/**
	 * Creates the field corresponding to the specified field types.
	 * 
	 * @param timestampUnit the the time unit of the timestamp field.
	 * @param fieldTypes the field type.
	 * @return the field corresponding to the specified field types.
	 */
	private static Field[] createFields(TimeUnit timestampUnit, FieldType[] fieldTypes) {
		
		Field[] fields = new Field[fieldTypes.length + 1];
		
		fields[0] = new TimestampField(timestampUnit);
		
		for (int i = 1; i <= fieldTypes.length; i++) {
	        
			fields[i] = fieldTypes[i - 1].newField();
        }
		
		return fields;
	}
}
