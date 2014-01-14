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
import io.horizondb.model.Field;
import io.horizondb.model.FieldType;

import java.io.IOException;


import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * <code>Field</code> containing a byte value.
 * 
 * @author Benjamin
 *
 */
public class ByteField extends AbstractField {
	
	/**
	 * The field value.
	 */
	private int value;
	
	
	
	@Override
    public Field newInstance() {
		
		ByteField copy = new ByteField();
		copy.setByte(this.value);
		
	    return copy;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isZero() {
		return this.value == 0;
	}

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public void setValueToZero() {
	    this.value = 0;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void add(Field field) {

		this.value += field.getByte();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public void subtract(Field field) {
		
		this.value -= field.getByte();
    }

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public void copyTo(Field field) {
		
		field.setByte(this.value);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeTo(ByteWriter writer) throws IOException {
		writer.writeByte(this.value);
	}

	/**
	 * {@inheritDoc}
	 */	
	@Override
    public void readFrom(ByteReader reader) throws IOException {
	    this.value = reader.readByte();
    }
	
	/**	
	 * {@inheritDoc}
	 */
	@Override
    public int computeSize() {

	    return 1;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public FieldType getType() {
	    return FieldType.BYTE;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public void setByte(int b) {
	    this.value = b;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public int getByte() {
	    return this.value;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public int getInt() {
	    return getByte();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public long getLong() {
	    return getByte();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public long getDecimalMantissa() {
	    return this.value;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public byte getDecimalExponent() {
	    return 0;
    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
	    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("value", this.value).toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object object) {
	    if (object == this) {
		    return true;
	    }
	    if (!(object instanceof ByteField)) {
		    return false;
	    }
	    ByteField rhs = (ByteField) object;
	    return new EqualsBuilder().append(this.value, rhs.value).isEquals();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
	    return new HashCodeBuilder(1633523303, -473857531).append(this.value)
	                                                      .toHashCode();
	}
}
