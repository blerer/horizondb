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
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.model.Field;
import io.horizondb.model.FieldType;
import io.horizondb.model.Record;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * @author Benjamin
 *
 */
public class TimeSeriesRecord extends AbstractTimeSeriesRecord implements Comparable<TimeSeriesRecord> {
	
    /**
     * Specify if this record is a delta or a full record.
     */
	private boolean delta;
	
	/**
	 * Creates a new <code>TimeSeriesRecord</code> of the specified type.
	 * 
	 * @param recordType the record type
	 * @param timestampUnit the time unit of the timestamp field
	 * @param fieldTypes the fields types
	 */
	public TimeSeriesRecord(int recordType, TimeUnit timestampUnit, FieldType... fieldTypes) {

		super(recordType, timestampUnit, fieldTypes);
	}
		
	/**
	 * 
	 */
	TimeSeriesRecord(int recordType, Field... fields) {
		
		super(recordType, fields);
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 */
	@Override
    public TimeSeriesRecord newInstance() {
	    return new TimeSeriesRecord(this);
    }

	/**
	 * Copy constructor.
	 * 
	 * @param record the record to copy
	 */
	private TimeSeriesRecord(TimeSeriesRecord record) {
		this(record.getType(), deepCopy(record.fields));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
    public boolean isDelta() {
		return this.delta;
	}

	/**
	 * Specify if this record is a delta or a full one.
	 * 
	 * @param delta the new delta value
	 */
	public void setDelta(boolean delta) {
		this.delta = delta;
	}

	public BitSet getBitSet() {

    	this.bitSet.reset();
    	this.bitSet.writeBit(isDelta());
    	
    	for (int i = 0, m = this.fields.length; i < m; i++) {
    		this.bitSet.writeBit(!getField(i).isZero());
    	}
    	
        return this.bitSet;
    }
	
	
	public void setInt(int index, int i) {
		
		getField(index).setInt(i);
	}
	
	public void setLong(int index, long l) {
		
		getField(index).setLong(l);
	}
	
	public void setTimestampInNanos(int index, long timestamp) {
		
		getField(index).setTimestampInNanos(timestamp);
	}
	
	public void setTimestampInMicros(int index, long timestamp) {
		
		getField(index).setTimestampInMicros(timestamp);
	}
	
	public void setTimestampInMillis(int index, long timestamp) {
		
		getField(index).setTimestampInMillis(timestamp);
	}
	
	public void setTimestampInSeconds(int index, long timestamp) {
		
		getField(index).setTimestampInSeconds(timestamp);
	}
	
	public void setByte(int index, int b) {
		
		getField(index).setByte(b);
	}


    public void setDecimal(int index, long mantissa, int exponent) {

    	getField(index).setDecimal(mantissa, exponent);
    }

    /**
	 * @param first
	 * @param second
     * @throws IOException 
	 */
    public void subtract(Record other) throws IOException {

    	this.delta = true;
    	
    	for (int i = 0; i < this.fields.length; i++) {
	        getField(i).subtract(other.getField(i));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
    	
    	BitSet bitSet = getBitSet();      
    	bitSet.readBit(); // skip isDelta
    	
        VarInts.writeUnsignedLong(writer, bitSet.toLong());
    	
    	for (int i = 0; i < this.fields.length; i++) {
	        
    		if (bitSet.readBit()) {
    			getField(i).writeTo(writer);
    		}
        }
    }
    
    /**
     * {@inheritDoc}
     */
	@Override
    public int computeSerializedSize() {

		BitSet bitSet = getBitSet();      
		
	    int size = 0;
	    
	    size += VarInts.computeUnsignedLongSize(bitSet.toLong());
	    
    	bitSet.readBit(); // skip isDelta
	    
    	for (int i = 0; i < this.fields.length; i++) {
    		if (bitSet.readBit()) {
    			size += getField(i).computeSize();
    		}	
        }
    	
    	return size;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public Field getField(int index) {
	    return this.fields[index];
    }
    
    /**
     * {@inheritDoc}
     */
	@Override
    public Field[] getFields() {
	    return this.fields;
    }

	/**
	 * @param record
	 * @throws IOException 
	 */
    public void add(Record record) throws IOException {

    	for (int i = 0; i < this.fields.length; i++) {
	        getField(i).add(record.getField(i));
        }
    }   
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesRecord toTimeSeriesRecord() {
	    return this;
    }

    /**
     * Creates a deep copy of the specified record arrays.
     * 
     * @param records the records
     * @return a deep copy of the specified record array.
     */
	public static TimeSeriesRecord[] deepCopy(TimeSeriesRecord[] records) {
		
	    TimeSeriesRecord[] copy = new TimeSeriesRecord[records.length];
		
		for (int i = 0, m = records.length; i < m; i++) {
			
			TimeSeriesRecord record = records[i];
			
			if (record != null) {
				
				copy[i] = record.newInstance();
			}
		}
	    return copy;
	}
    
	/**
     * {@inheritDoc}
     */
    @Override
	public boolean equals(Object object) {
	    if (object == this) {
		    return true;
	    }
	    if (!(object instanceof TimeSeriesRecord)) {
		    return false;
	    }
	    TimeSeriesRecord rhs = (TimeSeriesRecord) object;
	    
	    return new EqualsBuilder().append(getType(), rhs.getType())
	    		                  .append(this.delta, rhs.delta)
	                              .append(this.fields, rhs.fields)
	                              .isEquals();
	}

    /**
     * {@inheritDoc}
     */
    @Override
	public int hashCode() {
		return new HashCodeBuilder(309433041, 530340075).append(getType())
		                                                .append(this.delta)
		                                                .append(this.fields)
		                                                .toHashCode();
	}

    /**
     * {@inheritDoc}
     */
    @Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("type", getType())
		                                                                  .append("delta", this.delta)
		                                                                  .append("fields", this.fields)
		                                                                  .toString();
	}

    /**
     * {@inheritDoc}
     */
	@Override
    public int compareTo(TimeSeriesRecord o) {
	    
		try {
	        
			return Long.compare(this.getTimestampInNanos(0), o.getTimestampInNanos(0));
        
		} catch (IOException e) {
			throw new IllegalStateException(e);
        } 
    }
}
