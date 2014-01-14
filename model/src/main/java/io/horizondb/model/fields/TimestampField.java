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
package io.horizondb.model.fields;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.model.Field;
import io.horizondb.model.FieldType;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import static org.apache.commons.lang.Validate.notNull;

/**
 * @author Benjamin
 *
 */
public final class TimestampField extends AbstractField {
	
	/**
	 * The time unit.
	 */
	private final TimeUnit unit;
	
	/**
	 * The timestamp.
	 */
	private long timestamp;

	/**
	 * {@inheritDoc}
	 */
    @Override
    public FieldType getType() {
	   
    	if (this.unit.equals(TimeUnit.NANOSECONDS)) {
    		
    		return FieldType.NANOSECONDS_TIMESTAMP;
    	}
    	
    	if (this.unit.equals(TimeUnit.MICROSECONDS)) {
    		
    		return FieldType.MICROSECONDS_TIMESTAMP;
    	}
    	
    	if (this.unit.equals(TimeUnit.MILLISECONDS)) {
    		
    		return FieldType.MILLISECONDS_TIMESTAMP;
    	}
    	
    	return FieldType.SECONDS_TIMESTAMP;
    }
	
    /**    
     * {@inheritDoc}
     */
	@Override
    public Field newInstance() {

		TimestampField copy = new TimestampField(this.unit);
		copy.timestamp = this.timestamp;
		
	    return copy;
    }



	/**
	 * 
	 */
	public TimestampField(TimeUnit unit) {

		notNull(unit, "the unit parameter must not be null.");
		this.unit = unit;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isZero() {
		return this.timestamp == 0;
	}

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public void setValueToZero() {
	    this.timestamp = 0;
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void add(Field field) {
		
		this.timestamp += ((TimestampField)field).timestamp;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subtract(Field field) {
		
		this.timestamp -= ((TimestampField)field).timestamp;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void copyTo(Field field) {
		
		((TimestampField)field).timestamp = this.timestamp;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeTo(ByteWriter writer) throws IOException {
		VarInts.writeLong(writer, this.timestamp);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
    public void readFrom(ByteReader reader) throws IOException {
		this.timestamp = VarInts.readLong(reader);
    }
	
	/**	
	 * {@inheritDoc}
	 */
	@Override
    public int computeSize() {
	    return VarInts.computeLongSize(this.timestamp);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public long getTimestampInNanos() {

		return this.unit.toNanos(this.timestamp);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public long getTimestampInMicros() {
		
		return this.unit.toMicros(this.timestamp);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public long getTimestampInMillis() {
		
		return this.unit.toMillis(this.timestamp);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public long getTimestampInSeconds() {
		
		return this.unit.toSeconds(this.timestamp);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public void setTimestampInNanos(long timestamp) {
	    
		setTimestamp(timestamp, TimeUnit.NANOSECONDS);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public void setTimestampInMicros(long timestamp) {
	    
		setTimestamp(timestamp, TimeUnit.MICROSECONDS);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public void setTimestampInMillis(long timestamp) {
	    
		setTimestamp(timestamp, TimeUnit.MILLISECONDS);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public void setTimestampInSeconds(long timestamp) {
	    
		setTimestamp(timestamp, TimeUnit.SECONDS);
    }
	
	/**
	 * @param timestamp
	 * @param sourceUnit
	 */
    private void setTimestamp(long sourceTimestamp, TimeUnit sourceUnit) {
	    
    	if (sourceUnit.compareTo(this.unit) < 0) {
			
			throw new TypeConversionException("A timestamp in " + sourceUnit.toString().toLowerCase() 
			                                  + " cannot be stored in a field of type: " + getType() + ".");
		}
		
	    this.timestamp = this.unit.convert(sourceTimestamp, sourceUnit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
	public String toString() {
		
	    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("timestamp", this.timestamp)
	    		                                                          .append("unit", this.unit)
	    		                                                          .toString();
	}

    /**
     * 
     * {@inheritDoc}
     */
    @Override
	public boolean equals(Object object) {
	    if (object == this) {
		    return true;
	    }
	    if (!(object instanceof TimestampField)) {
		    return false;
	    }
	    TimestampField rhs = (TimestampField) object;
	    return new EqualsBuilder().append(this.timestamp, rhs.timestamp)
	                              .append(this.unit, rhs.unit)
	                              .isEquals();
	}

    /**
     * 
     * {@inheritDoc}
     */
    @Override
	public int hashCode() {
	    return new HashCodeBuilder(773109111, 83366071).append(this.timestamp)
	                                                   .append(this.unit)
	                                                   .toHashCode();
	}
}
