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


import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * <code>Field</code> containing an integer value.
 * 
 * @author Benjamin
 *
 */
public class IntegerField extends AbstractField {
	
	/**
	 * The field value.
	 */
	private int value;
	
	/**	
	 * {@inheritDoc}
	 */
	@Override
    public Field newInstance() {

		IntegerField copy = new IntegerField();
		copy.setInt(this.value);
		
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

		this.value += ((IntegerField) field).value;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subtract(Field field) {

		this.value -= ((IntegerField) field).value;
	}
	
	/**	
	 * {@inheritDoc}
	 */
	@Override
    public void copyTo(Field field) {
	    
		((IntegerField) field).value = this.value;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeTo(ByteWriter writer) throws IOException {
		VarInts.writeLong(writer, this.value);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
    public void readFrom(ByteReader reader) throws IOException {
		this.value = VarInts.readInt(reader);
    }

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public int computeSize() {
	    return VarInts.computeIntSize(this.value);
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public FieldType getType() {
	    return FieldType.INTEGER;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public void setByte(int b) {
		setInt(b);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public void setInt(int i) {
		this.value = i;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public int getByte() {
	    return getInt();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public int getInt() {
		return this.value;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public long getLong() {
	    return getInt();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public long getDecimalMantissa() {
	    return getLong();
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
	    if (!(object instanceof IntegerField)) {
		    return false;
	    }
	    IntegerField rhs = (IntegerField) object;
	    return new EqualsBuilder().append(this.value, rhs.value).isEquals();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		
	    return new HashCodeBuilder(657043163, 1790494667).append(this.value).toHashCode();
	}
}
