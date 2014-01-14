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

import io.horizondb.io.BitSet;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BitSetTest {
	
    @Test
    public void testFill() {

        BitSet bitSet = new BitSet(2);

        bitSet.fill(1);

        assertEquals("01", bitSet.toString());
        assertTrue(bitSet.readBit());
        assertFalse(bitSet.readBit());

        bitSet.fill(2);

        assertEquals("10", bitSet.toString());
        assertFalse(bitSet.readBit());
        assertTrue(bitSet.readBit());

        bitSet.fill(3);

        assertEquals("11", bitSet.toString());
        assertTrue(bitSet.readBit());
        assertTrue(bitSet.readBit());

        bitSet = new BitSet(3);
        bitSet.fill(4);

        assertEquals("100", bitSet.toString());
        assertFalse(bitSet.readBit());
        assertFalse(bitSet.readBit());
        assertTrue(bitSet.readBit());
    }

    @Test
    public void testFillWithLongBiggerThanCapacity() {

        BitSet bitSet = new BitSet(2);

        bitSet.fill(4);

        assertEquals("00", bitSet.toString());
        assertFalse(bitSet.readBit());
        assertFalse(bitSet.readBit());
    }
    
    @Test
    public void testIsReadable() {

        BitSet bitSet = new BitSet(3).writeOne().writeZero().writeOne();

        assertTrue(bitSet.isReadable());
        assertTrue(bitSet.readBit());
        assertTrue(bitSet.isReadable());
        assertFalse(bitSet.readBit());
        assertTrue(bitSet.isReadable());
        assertTrue(bitSet.readBit());
        assertFalse(bitSet.isReadable());
    }

    @Test
    public void testWrite() {

        BitSet bitSet = new BitSet(3);

        bitSet.writeOne();
        assertEquals("001", bitSet.toString());

        bitSet.writeZero();
        assertEquals("001", bitSet.toString());

        bitSet.writeOne();
        assertEquals("101", bitSet.toString());
    }
        
    @Test
    public void testWriteAboveCapacity() {

        BitSet bitSet = new BitSet(2);

        try {
        
        	bitSet.writeOne().writeOne().writeOne();
        	fail();
        	
        } catch (IndexOutOfBoundsException e) {
        	
        	assertTrue(true);
        }
    }
    
    @Test
    public void testGet() {

        BitSet bitSet = new BitSet(3).writeOne().writeZero().writeOne();
        
        assertTrue(bitSet.getBit(0));
        assertFalse(bitSet.getBit(1));
        assertTrue(bitSet.getBit(2));
    }
    
    @Test
    public void testReaderIndex() {

        BitSet bitSet = new BitSet(3).writeOne().writeZero().writeOne();
        
        assertEquals(0, bitSet.readerIndex());
        
        assertTrue(bitSet.readBit());
        assertEquals(1, bitSet.readerIndex());
        assertFalse(bitSet.readBit());
        assertEquals(2, bitSet.readerIndex());
        assertTrue(bitSet.readBit());
        assertEquals(3, bitSet.readerIndex());
        
        bitSet.readerIndex(0);
        assertTrue(bitSet.readBit());
        
        bitSet.readerIndex(2);
        assertTrue(bitSet.readBit());
    }
    
    @Test
    public void testReset() {

        BitSet bitSet = new BitSet(3);

        bitSet.writeOne();
        assertEquals("001", bitSet.toString());

        bitSet.writeZero();
        assertEquals("001", bitSet.toString());

        bitSet.reset();

        bitSet.writeZero();
        assertEquals("000", bitSet.toString());
    }

    @Test
    public void testFillRemainingBitsWithZeros() {

        BitSet bitSet = new BitSet(4);

        bitSet.writeOne().writeOne().fillRemainingBitsWithZeros();
        assertEquals("0011", bitSet.toString());
    }

    @Test
    public void testToLong() {

        assertEquals(1, new BitSet(2).writeOne().toLong());
        assertEquals(2, new BitSet(2).writeZero().writeOne().toLong());
        assertEquals(3, new BitSet(2).writeOne().writeOne().toLong());
        assertEquals(3, new BitSet(3).writeOne().writeOne().writeZero().toLong());
        assertEquals(4, new BitSet(3).writeZero().writeZero().writeOne().toLong());
    } 
    
    @Test
    public void testToByte() {

        assertEquals((byte) 1, new BitSet(2).writeOne().toByte());
        assertEquals((byte) 2, new BitSet(2).writeZero().writeOne().toByte());
        assertEquals((byte) 3, new BitSet(2).writeOne().writeOne().toByte());
        assertEquals((byte) 3, new BitSet(3).writeOne().writeOne().writeZero().toByte());
        assertEquals((byte) 4, new BitSet(3).writeZero().writeZero().writeOne().toByte());
    }   
}
