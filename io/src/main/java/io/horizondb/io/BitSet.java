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
package io.horizondb.io;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * A set of bits of fixed size.
 *  
 * @author Benjamin
 *
 */
public final class BitSet {
    
    /**
     * The number of bits within this <code>BitSet</code>.
     */
    private final int capacity;
	    
    /**
     * The maximum value than the writing bit mask can have.
     */
    private final long limit;
    
    /**
     * The bits container.
     */
    private long bits;
    
    /**
     * The reading bit mask.
     */
    private int readingMask; 
    
    /**
     * The writing bit mask.
     */
    private int writingMask; 
    
    /**
     * Creates a new <code>BitSet</code> instance with a capacity of 62 bits.
     */
    public BitSet() {

    	this(62);
    }
    
    /**
     * Creates a new <code>BitSet</code> instance with the specified capacity.
     * 
     * @param capacity the <code>BitSet</code> capacity.
     */
    public BitSet(int capacity) {

    	Validate.isTrue(capacity > 0 && capacity <= 62, "The capacity must be greater than zero " +
    			"and less or equals to 62");
    	
        this.readingMask = 1;
        this.writingMask = 1;
        this.limit = computeLimit(capacity);
        this.capacity = capacity;
    }

    /**
     * Writes the specified bit to this <code>BitSet</code>.
     * <p>The bits are written from right to left.</p>
     * 
     * @return this <code>BitSet</code>.
     */
    public BitSet writeBit(boolean bit) {
    	
    	if (!isWriteable()) {
    		
    		throw new IndexOutOfBoundsException("Index: " + writerIndex() + ", Size: " + this.capacity);
    	}
    	
    	int mask = this.writingMask;
    	
        doSetBit(mask, bit);
        
        this.writingMask <<= 1;
        
        return this;
    }

    /**
     * Writes a bit of value one to this <code>BitSet</code>.
     * <p>The bits are written from right to left.</p>
     * 
     * @return this <code>BitSet</code>.
     */
    public BitSet writeOne() {

        return writeBit(true);
    }

    /**
     * Writes a bit of value zero to this <code>BitSet</code>.
     * <p>The bits are written from right to left.</p>
     * 
     * @return this <code>BitSet</code>.
     */
    public BitSet writeZero() {

        return writeBit(false);
    }

    /**
     * Fill the remaining bits with zeros.
     * 
     * @return this <code>BitSet</code>.
     */
    public BitSet fillRemainingBitsWithZeros() {

        while (isWriteable()) {
            writeZero();
        }
        return this;
    }

    /**
     * Fill the remaining bits with ones. 
     * 
     * @return this <code>BitSet</code>.
     */
    public BitSet fillRemainingBitsWithOnes() {

        while (isWriteable()) {
            writeOne();
        }
        return this;
    }
    
    /**
     * Returns the next bit available. 
     * <p>The bits are read from right to left.</p>
     * 
     * @return the next bit available.
     */
    public boolean readBit() {
        
        boolean b = doGetBit(this.readingMask);
        this.readingMask <<= 1;
        
        return b;
    }
    
    /**
     * Returns the bit at the specified position. 
     * 
     * @param index the bit position.
     * @return the next bit available.
     */
    public boolean getBit(int index) {
        
    	int mask = 1 << index;
    	
        return doGetBit(mask);
    }
    
    public int writerIndex() {
    	
    	return Long.numberOfTrailingZeros(this.writingMask);
    }
    
    public BitSet writerIndex(int index) {
    	
    	this.writingMask = 1 << index;
    	return this;
    }
    
    public int readerIndex() {
    	
    	return Long.numberOfTrailingZeros(this.readingMask);
    }
    
    public BitSet readerIndex(int index) {
    	
    	this.readingMask = 1 << index;
    	return this;
    }
    
    /**
     * Returns <code>true</code> if this <code>BitSet</code> is readable, <code>false</code> otherwise.
     * 
     * @return <code>true</code> if this <code>BitSet</code> is readable, <code>false</code> otherwise.
     */
    public boolean isReadable() {

        return this.readingMask < this.writingMask;
    }
    
    /**
     * Returns <code>true</code> if this <code>BitSet</code> is writable, <code>false</code> otherwise.
     * 
     * @return <code>true</code> if this <code>BitSet</code> is writable, <code>false</code> otherwise.
     */
    public boolean isWriteable() {

        return this.writingMask < this.limit;
    }

    /**
     * Reset the cursor position.
     */
    public BitSet reset() {

        this.readingMask = 1;
        this.writingMask = 1;
        return this;
    }
    
    /**
     * Returns the long value corresponding to this <code>BitSet</code>.
     * 
     * @return the long value corresponding to this <code>BitSet</code>.
     */
    public long toLong() {

        return this.bits;
    }

    /**
     * Returns the byte value corresponding to this <code>BitSet</code>.
     * 
     * @return the byte value corresponding to this <code>BitSet</code>.
     */
    public byte toByte() {

        return (byte) this.bits;
    }

    /**
     * Fills this <code>BitSet</code> from the bits of the specified long.
     * 
     * @param l the long value.
     * @return this <code>BitSet</code>
     */
    public BitSet fill(long l) {

        reset();

        this.bits = l;
        
        return this;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof BitSet)) {
            return false;
        }
        
        BitSet rhs = (BitSet) object;

        return new EqualsBuilder().append(this.readingMask, rhs.readingMask)
        		                  .append(this.writingMask, rhs.writingMask)
                                  .append(this.bits, rhs.bits)
                                  .isEquals();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return new HashCodeBuilder(205262257, -1771534377).append(this.readingMask)
		                                                  .append(this.writingMask)
		                                                  .append(this.bits)
		                                                  .toHashCode();
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        
        StringBuilder builder = new StringBuilder(this.capacity);
        
        for (int i = this.capacity - 1; i >= 0; i--) {

        	if (getBit(i)) {
        		builder.append('1');
        	} else {
        		builder.append('0');
        	}
        }
                
        return builder.toString();
    }
    
	/**
	 * Returns the bit corresponding to the specified mask. 
	 * 
	 * @param mask the mask used to retrieve the bit.
	 * @return the bit corresponding to the specified mask.
	 */
    private boolean doGetBit(int mask) {
	    return ((this.bits & mask) != 0);
    }
    
	/**
	 * Sets the bit corresponding to the specified mask. 
	 * 
	 * @param mask the mask used to set the bit
	 * @param bit the new bit value
	 */
    private void doSetBit(int mask, boolean bit) {
	    if (bit) {
            
            this.bits |= mask;   
        
        } else if ((this.bits & mask) != 0) {
            
            this.bits ^= mask;   
        }
    }
    
	/**
	 * Computes the maximum value that the mask can have.
	 * 
	 * @param numberOfBits the number of bits.
	 * @return the maximum value that the mask can have.
	 */
    private static long computeLimit(int numberOfBits) {
    	return 1L << (numberOfBits);
    }
}
