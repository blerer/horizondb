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

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.model.FieldType;
import io.horizondb.model.RecordBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TimeseriesRecordTest {
    
	/**
	 * The record type used during the tests.
	 */
	private static final int TYPE = 0;
	
        
    @Test
    public void testSubtract() throws IOException {

        RecordBuilder builder = new RecordBuilder(TYPE, TimeUnit.MILLISECONDS, FieldType.INTEGER);
        
        builder.setTimestampInMillis(0, 1001L)
               .setInt(1, 3);
        
        TimeSeriesRecord first = builder.build(); 
        
		builder.setTimestampInMillis(0, 1002L)
		       .setInt(1, 1);
        
		TimeSeriesRecord second = builder.build(); 
        
		builder.setTimestampInMillis(0, 1003L)
	           .setInt(1, 1);
          
        TimeSeriesRecord third = builder.build(); 
        
        TimeSeriesRecord delta = second.newInstance();
        
        delta.subtract(first);
        
        assertFalse(first.isDelta());
        assertFalse(second.isDelta());
        assertTrue(delta.isDelta());
        
        assertEquals(1L, delta.getTimestampInMillis(0));
        assertEquals(-2, delta.getInt(1));
        
        Buffer buffer = Buffers.allocate(3);
        
        delta.writeTo(buffer);
        
        assertArrayEquals(new byte[] {7, 2, 3}, buffer.array());
                
        delta = third.newInstance();
        
        delta.subtract(second);
        
        assertFalse(second.isDelta());
        assertFalse(third.isDelta());
        assertTrue(delta.isDelta());
        
        assertEquals(1L, delta.getTimestampInMillis(0));
        assertEquals(0, delta.getByte(1));
        
        buffer = Buffers.allocate(2);
        
        delta.writeTo(buffer);

        assertArrayEquals(new byte[] {3, 2}, buffer.array());
    }
    
    @Test
    public void testSubstractWithDecimalFieldAndNoExponentChange() throws IOException {
    	
		TimeSeriesRecord first = new TimeSeriesRecord(TYPE,
		                                              TimeUnit.MILLISECONDS,
		                                              FieldType.DECIMAL,
		                                              FieldType.LONG);
        first.setTimestampInMillis(0, 100);
        first.setDecimal(1, 104, -1);
        first.setLong(2, 3);
                
		TimeSeriesRecord second = new TimeSeriesRecord(TYPE,
		                                               TimeUnit.MILLISECONDS,
		                                               FieldType.DECIMAL,
		                                               FieldType.LONG);
        second.setTimestampInMillis(0, 120);
        second.setDecimal(1, 146, -1);
        second.setLong(2, 3);
        
        first.subtract(second);
        
        assertTrue(first.isDelta());
        
		TimeSeriesRecord expected = new TimeSeriesRecord(TYPE,
		                                                 TimeUnit.MILLISECONDS,
		                                                 FieldType.DECIMAL,
		                                                 FieldType.LONG);

		expected.setDelta(true);
        expected.setTimestampInMillis(0, -20);
        expected.setDecimal(1, -42, -1);
        expected.setLong(2, 0);
        
        assertEquals(expected, first);
    }
    
    @Test
    public void testSubstractWithDecimalFieldAndExponentIncrease() throws IOException {
    	
		TimeSeriesRecord first = new TimeSeriesRecord(TYPE,
		                                              TimeUnit.MILLISECONDS,
		                                              FieldType.DECIMAL,
		                                              FieldType.LONG);
        first.setTimestampInMillis(0, 100);
        first.setDecimal(1, 10, 0);
        first.setLong(2, 3);
                
		TimeSeriesRecord second = new TimeSeriesRecord(TYPE,
		                                               TimeUnit.MILLISECONDS,
		                                               FieldType.DECIMAL,
		                                               FieldType.LONG);
        second.setTimestampInMillis(0, 120);
        second.setDecimal(1, 146, -1);
        second.setLong(2, 3);
        
        first.subtract(second);
        assertTrue(first.isDelta());
        
		TimeSeriesRecord expected = new TimeSeriesRecord(TYPE,
		                                                 TimeUnit.MILLISECONDS,
		                                                 FieldType.DECIMAL,
		                                                 FieldType.LONG);

		expected.setDelta(true);
        expected.setTimestampInMillis(0, -20);
        expected.setDecimal(1, -46, -1);
        expected.setLong(2, 0);
        
        assertEquals(expected, first);
    }
    
    @Test
    public void testSubstractWithDecimalFieldAndExponentDecrease() throws IOException {
    	
		TimeSeriesRecord first = new TimeSeriesRecord(TYPE,
		                                              TimeUnit.MILLISECONDS,
		                                              FieldType.DECIMAL,
		                                              FieldType.LONG);
        first.setTimestampInMillis(0, 100);
        first.setDecimal(1, 14, 0);
        first.setLong(2, 3);
                
		TimeSeriesRecord second = new TimeSeriesRecord(TYPE,
		                                               TimeUnit.MILLISECONDS,
		                                               FieldType.DECIMAL,
		                                               FieldType.LONG);
        second.setTimestampInMillis(0, 120);
        second.setDecimal(1, 135, -1);
        second.setLong(2, 3);
        
        first.subtract(second);
        
        assertTrue(first.isDelta());
        
		TimeSeriesRecord expected = new TimeSeriesRecord(TYPE,
		                                                 TimeUnit.MILLISECONDS,
		                                                 FieldType.DECIMAL,
		                                                 FieldType.LONG);

		expected.setDelta(true);
        expected.setTimestampInMillis(0, -20);
        expected.setDecimal(1, 5, -1);
        expected.setLong(2, 0);
        
        assertEquals(expected, first);
    }
    
    @Test
    public void testSubstractWithDecimalFieldAndEmptyFirst() throws IOException {
    	
		TimeSeriesRecord first = new TimeSeriesRecord(TYPE,
		                                              TimeUnit.MILLISECONDS,
		                                              FieldType.DECIMAL,
		                                              FieldType.LONG);
                
		TimeSeriesRecord second = new TimeSeriesRecord(TYPE,
		                                               TimeUnit.MILLISECONDS,
		                                               FieldType.DECIMAL,
		                                               FieldType.LONG);
        second.setTimestampInMillis(0, 120);
        second.setDecimal(1, 146, -1);
        second.setLong(2, 3);
        
        first.subtract(second);
        
        assertTrue(first.isDelta());
        
		TimeSeriesRecord expected = new TimeSeriesRecord(TYPE,
		                                                 TimeUnit.MILLISECONDS,
		                                                 FieldType.DECIMAL,
		                                                 FieldType.LONG);

		expected.setDelta(true);
        expected.setTimestampInMillis(0, -120);
        expected.setDecimal(1, -146, -1);
        expected.setLong(2, -3);
        
        assertEquals(expected, first);
    }
    
    @Test
    public void testSubstractWithDecimalFieldAndEmptySecond() throws IOException {
    	
		TimeSeriesRecord first = new TimeSeriesRecord(TYPE,
		                                              TimeUnit.MILLISECONDS,
		                                              FieldType.DECIMAL,
		                                              FieldType.LONG);
        first.setTimestampInMillis(0, 100);
        first.setDecimal(1, 10, 0);
        first.setLong(2, 3);
                
		TimeSeriesRecord second = new TimeSeriesRecord(TYPE,
		                                               TimeUnit.MILLISECONDS,
		                                               FieldType.DECIMAL,
		                                               FieldType.LONG);
        
        first.subtract(second);
        assertTrue(first.isDelta());
        
		TimeSeriesRecord expected = new TimeSeriesRecord(TYPE,
		                                                 TimeUnit.MILLISECONDS,
		                                                 FieldType.DECIMAL,
		                                                 FieldType.LONG);

		expected.setDelta(true);
        expected.setTimestampInMillis(0, 100);
        expected.setDecimal(1, 10, 0);
        expected.setLong(2, 3);
        
        assertEquals(expected, first);
    }
    
    
    @Test
    public void testAdd() throws IOException {
        		
		TimeSeriesRecord sum = new TimeSeriesRecord(TYPE,
		                                            TimeUnit.MILLISECONDS,
		                                            FieldType.INTEGER);

		RecordBuilder builder = new RecordBuilder(TYPE,
		                                          TimeUnit.MILLISECONDS,
		                                          FieldType.INTEGER);

		TimeSeriesRecord empty = builder.build(); 

		builder.setTimestampInMillis(0, 1001L)
		       .setInt(1, 3);

       TimeSeriesRecord first = builder.build(); 

       builder.setTimestampInMillis(0, 1002L)
              .setInt(1, 1);

       TimeSeriesRecord second = builder.build(); 

       builder.setTimestampInMillis(0, 1003L)
              .setInt(1, 1);
  
       TimeSeriesRecord third = builder.build(); 
       
       TimeSeriesRecord toAdd = first.newInstance();
       toAdd.subtract(empty);
       
       sum.add(toAdd);

       toAdd = second.newInstance();
       toAdd.subtract(first);    
        
       sum.add(toAdd);
        
       assertFalse(sum.isDelta());
       assertEquals(1002L, sum.getTimestampInMillis(0));
       assertEquals(1, sum.getByte(1));
        
       Buffer buffer = Buffers.allocate(4);
        
       sum.writeTo(buffer);
        
       assertArrayEquals(new byte[] {6, -44, 15, 2}, buffer.array());
        
       toAdd = third.newInstance();
       toAdd.subtract(second);    
        
       sum.add(toAdd);
        
       assertEquals(1003L, sum.getTimestampInMillis(0));
       assertEquals(1, sum.getByte(1));
        
       buffer = Buffers.allocate(4);
        
       sum.writeTo(buffer);
        
       assertArrayEquals(new byte[] {6, -42, 15, 2}, buffer.array());
    }
    
    @Test
    public void testAddToDelta() throws IOException {
		
		TimeSeriesRecord sum = new TimeSeriesRecord(TYPE,
		                                            TimeUnit.MILLISECONDS,
		                                            FieldType.INTEGER);
		sum.setDelta(true);
		

		RecordBuilder builder = new RecordBuilder(TYPE,
		                                          TimeUnit.MILLISECONDS,
		                                          FieldType.INTEGER);

		TimeSeriesRecord empty = builder.build(); 

		builder.setTimestampInMillis(0, 1001L)
		       .setInt(1, 3);

       TimeSeriesRecord first = builder.build(); 

       builder.setTimestampInMillis(0, 1002L)
              .setInt(1, 1);

       TimeSeriesRecord second = builder.build(); 

       builder.setTimestampInMillis(0, 1003L)
              .setInt(1, 1);
  
       TimeSeriesRecord third = builder.build(); 
               
       TimeSeriesRecord toAdd = first.newInstance();
       toAdd.subtract(empty);

       sum.add(toAdd);

       toAdd = second.newInstance();
       toAdd.subtract(first);    
        
       sum.add(toAdd);
        
       assertTrue(sum.isDelta());
       assertEquals(1002L, sum.getTimestampInMillis(0));
       assertEquals(1, sum.getByte(1));
        
       Buffer buffer = Buffers.allocate(4);
        
       sum.writeTo(buffer);
        
       assertArrayEquals(new byte[] {7, -44, 15, 2}, buffer.array());
        
       toAdd = third.newInstance();
       toAdd.subtract(second);    
        
       sum.add(toAdd);
        
       assertEquals(1003L, sum.getTimestampInMillis(0));
       assertEquals(1, sum.getByte(1));
        
       buffer = Buffers.allocate(4);
        
       sum.writeTo(buffer);
        
       assertArrayEquals(new byte[] {7, -42, 15, 2}, buffer.array());
    }

    @Test
    public void testAddWithDecimalFieldAndNoExponentChange() throws IOException {

		TimeSeriesRecord first = new TimeSeriesRecord(TYPE,
		                                              TimeUnit.MILLISECONDS,
		                                              FieldType.DECIMAL,
		                                              FieldType.LONG);
        first.setTimestampInMillis(0, 100);
        first.setDecimal(1, 146, 1);
        first.setLong(2, 3);
                
		TimeSeriesRecord second = new TimeSeriesRecord(TYPE,
		                                               TimeUnit.MILLISECONDS,
		                                               FieldType.DECIMAL,
		                                               FieldType.LONG);
        second.setTimestampInMillis(0, 120);
        second.setDecimal(1, 104, 1);
        second.setLong(2, 3);
        
        first.add(second);
        
		TimeSeriesRecord expected = new TimeSeriesRecord(TYPE,
		                                                 TimeUnit.MILLISECONDS,
		                                                 FieldType.DECIMAL,
		                                                 FieldType.LONG);

        expected.setTimestampInMillis(0, 220);
        expected.setDecimal(1, 250, 1);
        expected.setLong(2, 6);
        
        assertEquals(expected, first);
    }
    
    @Test
    public void testAddWithDecimalFieldAndExponentIncrease() throws IOException {
        

		TimeSeriesRecord first = new TimeSeriesRecord(TYPE,
		                                              TimeUnit.MILLISECONDS,
		                                              FieldType.DECIMAL,
		                                              FieldType.LONG);
        first.setTimestampInMillis(0, 100);
        first.setDecimal(1, 65, -1);
        first.setLong(2, 3);
                
        assertEquals(6.5, first.getDouble(1), 0.0);
        
		TimeSeriesRecord second = new TimeSeriesRecord(TYPE,
		                                               TimeUnit.MILLISECONDS,
		                                               FieldType.DECIMAL,
		                                               FieldType.LONG);
        second.setTimestampInMillis(0, 120);
        second.setDecimal(1, 14, 0);
        second.setLong(2, 3);
        
        assertEquals(14.0, second.getDouble(1), 0.0);
        
        first.add(second);
        
		TimeSeriesRecord expected = new TimeSeriesRecord(TYPE,
		                                                 TimeUnit.MILLISECONDS,
		                                                 FieldType.DECIMAL,
		                                                 FieldType.LONG);

        expected.setTimestampInMillis(0, 220);
        expected.setDecimal(1, 205, -1);
        expected.setLong(2, 6);
        
        assertEquals(expected, first);
        assertEquals(20.5, first.getDouble(1), 0.0);
    }

    @Test
    public void testAddWithDecimalFieldAndExponentDecrease() throws IOException {
        

		TimeSeriesRecord first = new TimeSeriesRecord(TYPE,
		                                              TimeUnit.MILLISECONDS,
		                                              FieldType.DECIMAL,
		                                              FieldType.LONG);
        first.setTimestampInMillis(0, 100);
        first.setDecimal(1, 14, 0);
        first.setLong(2, 3);
                
        assertEquals(14, first.getDouble(1), 0.0);
        
		TimeSeriesRecord second = new TimeSeriesRecord(TYPE,
		                                               TimeUnit.MILLISECONDS,
		                                               FieldType.DECIMAL,
		                                               FieldType.LONG);
        second.setTimestampInMillis(0, 120);
        second.setDecimal(1, 65, -1);
        second.setLong(2, 3);
        
        assertEquals(6.5, second.getDouble(1), 0.0);
        
        first.add(second);
        
		TimeSeriesRecord expected = new TimeSeriesRecord(TYPE,
		                                                 TimeUnit.MILLISECONDS,
		                                                 FieldType.DECIMAL,
		                                                 FieldType.LONG);

        expected.setTimestampInMillis(0, 220);
        expected.setDecimal(1, 205, -1);
        expected.setLong(2, 6);
        
        assertEquals(20.5, first.getDouble(1), 0.0);
        assertEquals(expected, first);
    }
      
    @Test
    public void testAddEmptyDelta() throws IOException {
		
		TimeSeriesRecord sum = new TimeSeriesRecord(TYPE,
		                                            TimeUnit.MILLISECONDS,
		                                            FieldType.INTEGER);

		TimeSeriesRecord empty = new TimeSeriesRecord(TYPE,
		                                              TimeUnit.MILLISECONDS,
		                                              FieldType.INTEGER);
		
		RecordBuilder builder = new RecordBuilder(TYPE,
		                                          TimeUnit.MILLISECONDS,
		                                          FieldType.INTEGER);
        
		builder.setTimestampInMillis(0, 1001L)
		       .setInt(1, 3);

		TimeSeriesRecord first = builder.build();

		builder.setTimestampInMillis(0, 1002L)
		       .setInt(1, 1);
				
		TimeSeriesRecord second = builder.build();
				
		TimeSeriesRecord third = builder.build();

		TimeSeriesRecord toAdd = first.newInstance();
		toAdd.subtract(empty);
        
        sum.add(toAdd);
        
        toAdd = second.newInstance();
		toAdd.subtract(first);    
        
        sum.add(toAdd);
        
        assertEquals(1002L, sum.getTimestampInMillis(0));
         assertEquals(1, sum.getByte(1));
        
         Buffer buffer = Buffers.allocate(4);
         
        sum.writeTo(buffer);
        
        assertArrayEquals(new byte[] {6, -44, 15, 2}, buffer.array());
        
        toAdd = third.newInstance();
		toAdd.subtract(second);    
        
        sum.add(toAdd);
        
        assertEquals(1002L, sum.getTimestampInMillis(0));
        assertEquals(1, sum.getByte(1));
        
        buffer = Buffers.allocate(4);
         
        sum.writeTo(buffer);
        
        assertArrayEquals(new byte[] {6, -44, 15, 2}, buffer.array());
    }
    
    @Test
    public void testAddEmptyWithDecimalField() throws IOException {

		TimeSeriesRecord first = new TimeSeriesRecord(TYPE,
		                                              TimeUnit.MILLISECONDS,
		                                              FieldType.DECIMAL,
		                                              FieldType.LONG);
        first.setTimestampInMillis(0, 100);
        first.setDecimal(1, 146, 1);
        first.setLong(2, 3);
                
		TimeSeriesRecord second = new TimeSeriesRecord(TYPE,
		                                               TimeUnit.MILLISECONDS,
		                                               FieldType.DECIMAL,
		                                               FieldType.LONG);
        
        first.add(second);
        
		TimeSeriesRecord expected = new TimeSeriesRecord(TYPE,
		                                                 TimeUnit.MILLISECONDS,
		                                                 FieldType.DECIMAL,
		                                                 FieldType.LONG);

        expected.setTimestampInMillis(0, 100);
        expected.setDecimal(1, 146, 1);
        expected.setLong(2, 3);
        
        assertEquals(expected, first);
    }
    
    @Test
    public void testAddToEmptyWithDecimalField() throws IOException {

		TimeSeriesRecord first = new TimeSeriesRecord(TYPE,
		                                              TimeUnit.MILLISECONDS,
		                                              FieldType.DECIMAL,
		                                              FieldType.LONG);
        first.setTimestampInMillis(0, 100);
        first.setDecimal(1, 146, 1);
        first.setLong(2, 3);
                
		TimeSeriesRecord second = new TimeSeriesRecord(TYPE,
		                                               TimeUnit.MILLISECONDS,
		                                               FieldType.DECIMAL,
		                                               FieldType.LONG);
        
		second.add(first);
        
		TimeSeriesRecord expected = new TimeSeriesRecord(TYPE,
		                                                 TimeUnit.MILLISECONDS,
		                                                 FieldType.DECIMAL,
		                                                 FieldType.LONG);

        expected.setTimestampInMillis(0, 100);
        expected.setDecimal(1, 146, 1);
        expected.setLong(2, 3);
        
        assertEquals(expected, second);
    }
    
    @Test
    public void testWriteTo() throws IOException {
        
		TimeSeriesRecord record = new TimeSeriesRecord(TYPE,
		                                               TimeUnit.NANOSECONDS,
		                                               FieldType.INTEGER);
    	
		record.setTimestampInNanos(0, 10001000);
		record.setInt(1, 1);
        
		Buffer buffer = Buffers.allocate(6);
         
        record.writeTo(buffer);

        assertArrayEquals(new byte[] {6, -48, -23, -60, 9, 2},
                          buffer.array());
    }
    
    @Test
    public void testWriteToWithDecimalField() throws IOException {
        
		TimeSeriesRecord record = new TimeSeriesRecord(TYPE,
		                                               TimeUnit.NANOSECONDS,
		                                               FieldType.DECIMAL);
    	
		record.setTimestampInNanos(0, 10001000);
		record.setDecimal(1, 12, 1);
        
		Buffer buffer = Buffers.allocate(7);
         
        record.writeTo(buffer);

        assertArrayEquals(new byte[] {6, -48, -23, -60, 9, 24, 1},
                          buffer.array());
    }
        
    @Test
    public void testWriteToWithEmptyRecord() throws IOException {
                
		TimeSeriesRecord record = new TimeSeriesRecord(TYPE,
		                                               TimeUnit.NANOSECONDS,
		                                               FieldType.INTEGER);

		Buffer buffer = Buffers.allocate(1);
         
        record.writeTo(buffer);
        
        assertArrayEquals(new byte[] {0}, buffer.array());
    }
    
    @Test
    public void testComputeSize() throws IOException {
    	
    	TimeSeriesRecord record = new TimeSeriesRecord(0, TimeUnit.NANOSECONDS, FieldType.MILLISECONDS_TIMESTAMP, FieldType.BYTE); 
    	record.setTimestampInNanos(0, 12000700);
    	record.setTimestampInMillis(1, 12);
    	record.setByte(2, 3);
    	
    	Buffer buffer = Buffers.allocate(100);
         
        record.writeTo(buffer);
        
        assertEquals(record.computeSerializedSize(), buffer.readableBytes());
    }	
    
    @Test
    public void testComputeSizeWithZeroValues() throws IOException {
    	
    	TimeSeriesRecord record = new TimeSeriesRecord(0, TimeUnit.NANOSECONDS, FieldType.MILLISECONDS_TIMESTAMP, FieldType.BYTE); 
    	record.setTimestampInNanos(0, 0);
    	record.setTimestampInMillis(1, 12);
    	record.setByte(2, 3);
    	
    	Buffer buffer = Buffers.allocate(100);
         
        record.writeTo(buffer);
        
        assertEquals(record.computeSerializedSize(), buffer.readableBytes());
    }	
}
