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

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;

import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 *
 */
public class DecimalFieldTest {

	@Test
    public void testAddWithNoExponentChange() {

		DecimalField first = new DecimalField();
        first.setDecimal(146, 1);
                
		DecimalField second = new DecimalField();
		second.setDecimal(82, 1);
        
        first.add(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(228, 1);
        
        assertEquals(expected, first);
        
        assertEquals(2280.0, first.getDouble(), 0.0);
    }
	
	@Test
    public void testAddWithNoExponentChangeAndNegativeExponent() {

		DecimalField first = new DecimalField();
        first.setDecimal(146, -1);
                
		DecimalField second = new DecimalField();
		second.setDecimal(82, -1);
        
        first.add(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(228, -1);
        
        assertEquals(expected, first);
        
        assertEquals(22.8, first.getDouble(), 0.0);
    }
	
	@Test
    public void testAddWithExponentIncrease() {

		DecimalField first = new DecimalField();
        first.setDecimal(2, 1);
        assertEquals(20, first.getDouble(), 0.0);
                
		DecimalField second = new DecimalField();
		second.setDecimal(1, 2);
		assertEquals(100, second.getDouble(), 0.0);
        
        first.add(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(12, 1);
        
        assertEquals(expected, first);
        
        assertEquals(120, first.getDouble(), 0.0);
    }
	
	@Test
    public void testAddWithExponentIncreaseAndTwoNegativeExponents() {

		DecimalField first = new DecimalField();
        first.setDecimal(82, -2);
                
		DecimalField second = new DecimalField();
		second.setDecimal(14, -1);
        
        first.add(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(222, -2);
        
        assertEquals(expected, first);
        
        assertEquals(2.22, first.getDouble(), 0.0);
    }
	
	@Test
    public void testAddWithExponentIncreaseAndOneNegativeExponent() {

		DecimalField first = new DecimalField();
        first.setDecimal(82, -1);
                
		DecimalField second = new DecimalField();
		second.setDecimal(14, 0);
        
        first.add(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(222, -1);
        
        assertEquals(expected, first);
        
        assertEquals(22.2, first.getDouble(), 0.0);
    }
	
	@Test
    public void testAddWithExponentDecrease() {

		DecimalField first = new DecimalField();
        first.setDecimal(2, 2);
                
		DecimalField second = new DecimalField();
		second.setDecimal(1, 1);
        
        first.add(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(21, 1);
        
        assertEquals(expected, first);
        
        assertEquals(210, first.getDouble(), 0.0);
    }
	
	@Test
    public void testAddWithNanAdded() {

		DecimalField first = new DecimalField();
        first.setDecimal(2, 2);
                
		DecimalField second = new DecimalField();
		second.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
        
        first.add(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
        
        assertEquals(expected, first);
        
        assertTrue(Double.isNaN(first.getDouble()));
    }
	
	@Test
    public void testAddToNaN() {

		DecimalField first = new DecimalField();
        first.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
                
		DecimalField second = new DecimalField();
		second.setDecimal(12, 0);
        
        first.add(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(12, 0);
        
        assertEquals(expected, first);
    }
	
	@Test
    public void testAddNaNToNaN() {

		DecimalField first = new DecimalField();
        first.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
                
		DecimalField second = new DecimalField();
		second.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
        
        first.add(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
        
        assertEquals(expected, first);
    }
	
	@Test
    public void testAddWithExponentDecreaseAndTwoNegativeExponents() {

		DecimalField first = new DecimalField();
        first.setDecimal(2, -1);
                
		DecimalField second = new DecimalField();
		second.setDecimal(1, -2);
        
        first.add(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(21, -2);
        
        assertEquals(expected, first);
        
        assertEquals(0.21, first.getDouble(), 0.0);
    }
	
	@Test
    public void testAddEmptyField() {

		DecimalField first = new DecimalField();
        first.setDecimal(82, 1);
                
		DecimalField second = new DecimalField();
        
        first.add(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(82, 1);
        
        assertEquals(expected, first);
    }
	
	@Test
    public void testAddToEmptyField() {

		DecimalField first = new DecimalField();
                
		DecimalField second = new DecimalField();
		second.setDecimal(82, 1);
        
        first.add(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(82, 1);
        
        assertEquals(expected, first);
    }
	
	@Test
    public void testSubstractWithNoExponentChange() {

		DecimalField first = new DecimalField();
        first.setDecimal(146, 1);
                
		DecimalField second = new DecimalField();
		second.setDecimal(82, 1);

		first.subtract(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(64, 1);
        
        assertEquals(expected, first);
        
		first = new DecimalField();
        first.setDecimal(146, 1);

        second.subtract(first);

        expected = new DecimalField();
		expected.setDecimal(-64, 1);
        
        assertEquals(expected, second);
    }
	
	@Test
    public void testSubstractWithExponentIncrease() {

		DecimalField first = new DecimalField();
        first.setDecimal(135, -1);
                
		DecimalField second = new DecimalField();
		second.setDecimal(12, 0);
        
		first.subtract(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(15, -1);
        
        assertEquals(expected, first);
    }
	
	@Test
    public void testSubstractWithNanAsFirstDecimal() {

		DecimalField first = new DecimalField();
        first.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
                
		DecimalField second = new DecimalField();
		second.setDecimal(12, 0);
        
		first.subtract(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(12, 0);
        
        assertEquals(expected, first);
    }
	
	@Test
    public void testSubstractNaNWithNaNAsFirstDecimal() {

		DecimalField first = new DecimalField();
        first.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
                
		DecimalField second = new DecimalField();
		second.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
        
		first.subtract(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
        
        assertEquals(expected, first);
    }
	
	@Test
    public void testSubstractWithNanAsSecondDecimal() {

		DecimalField first = new DecimalField();
        first.setDecimal(12, 0);
                
		DecimalField second = new DecimalField();
		second.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
		
		first.subtract(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
        
        assertEquals(expected, first);
    }
	
	@Test
    public void testDiffWithTwoNaN() {

		DecimalField first = new DecimalField();
        first.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
                
		DecimalField second = new DecimalField();
		second.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
		
		first.subtract(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
        
        assertEquals(expected, first);
    }
	
	@Test
    public void testSubstractWithExponentDecrease() {
                
		DecimalField first = new DecimalField();
		first.setDecimal(12, 0);
        
		DecimalField second = new DecimalField();
        second.setDecimal(135, -1);
		
		first.subtract(second);

        DecimalField expected = new DecimalField();
		expected.setDecimal(-15, -1);
        
        assertEquals(expected, first);
    }
	
	@Test
    public void testWriteTo() throws IOException {
                
		DecimalField field = new DecimalField();
		field.setDecimal(12, 1);

        Buffer buffer = Buffers.allocate(2);
        field.writeTo(buffer);
        
        assertArrayEquals(new byte[] {24, 1}, buffer.array());
    }
	
	@Test
    public void testGetDouble() {

		DecimalField field = new DecimalField();
		field.setDecimal(3, 0);
		
		assertEquals(3.0, field.getDouble(), 0.0);
		
		field.setDecimal(3, 1);
		assertEquals(30.0, field.getDouble(), 0.0);
		
		field.setDecimal(3, -1);
		assertEquals(0.3, field.getDouble(), 0.0);
		
		field.setDecimal(3, 2);
		assertEquals(300.0, field.getDouble(), 0.0);
		
		field.setDecimal(3, -2);
		assertEquals(0.03, field.getDouble(), 0.0);
		
		field.setDecimal(7568, -2);
		assertEquals(75.68, field.getDouble(), 0.0);
		
		field.setDecimal(DecimalField.NaN_MANTISSA, DecimalField.NaN_EXPONENT);
		assertTrue(Double.isNaN(field.getDouble()));
    }
	
	@Test
	public void testExponent() {
		
		assertEquals(DecimalField.NaN_EXPONENT, DecimalField.exponent(Double.NaN));
		assertEquals(0, DecimalField.exponent(0.0));
		assertEquals(0, DecimalField.exponent(1.0));
		assertEquals(-1, DecimalField.exponent(0.1));
		assertEquals(-1, DecimalField.exponent(0.100));
		assertEquals(-2, DecimalField.exponent(0.0100));
		assertEquals(-2, DecimalField.exponent(0.11));
		assertEquals(-5, DecimalField.exponent(1.2E-4));  
		assertEquals(-4, DecimalField.exponent(1E-4)); 
		assertEquals(1, DecimalField.exponent(10));
		assertEquals(4, DecimalField.exponent(50000));
		assertEquals(-5, DecimalField.exponent(500.34567));
	}
	
	@Test
	public void testMantissa() {
		
		assertEquals(DecimalField.NaN_MANTISSA, DecimalField.mantissa(Double.NaN, DecimalField.NaN_EXPONENT));
		assertEquals(0, DecimalField.mantissa(0, 0));
		assertEquals(1, DecimalField.mantissa(1.0, 0));
		assertEquals(1, DecimalField.mantissa(0.1, -1));
		assertEquals(1, DecimalField.mantissa(0.100, -1));
		assertEquals(1, DecimalField.mantissa(0.0100, -2));
		assertEquals(11, DecimalField.mantissa(0.11, -2));
		assertEquals(12, DecimalField.mantissa(1.2E-4, -5));  
		assertEquals(1, DecimalField.mantissa(1E-4, -4)); 
		assertEquals(1, DecimalField.mantissa(10, 1));
		assertEquals(5, DecimalField.mantissa(50000, 4));
		assertEquals(50034567, DecimalField.mantissa(500.34567, -5));
		
        assertEquals(2, DecimalField.mantissa(2, 0));
        assertEquals(20, DecimalField.mantissa(20, 0));
        assertEquals(2, DecimalField.mantissa(0.2, -1));
        assertEquals(25, DecimalField.mantissa(0.25, -2));
        assertEquals(2, DecimalField.mantissa(0.02, -2));
        assertEquals(2, DecimalField.mantissa(0.002, -3));
        assertEquals(-2, DecimalField.mantissa(-0.002, -3));
        assertEquals(14323, DecimalField.mantissa(1.4323, -4));
        assertEquals(75, DecimalField.mantissa(7.5E-4, -5));    
        assertEquals(1234, DecimalField.mantissa(1.234E3, 0));   
	}
}
