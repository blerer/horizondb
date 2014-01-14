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
package io.horizondb.io.files;

import io.horizondb.io.ByteReader;
import io.horizondb.io.files.DirectFileDataInput;
import io.horizondb.io.files.DirectFileDataOutput;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Benjamin
 *
 */
public class DirectFileDataInputTest {

	/**
	 * The test directory.
	 */
    private Path testDirectory;
	
    /**
     * The path to the file used during the tests.
     */
    private Path path;
    
    @Before
    public void setUp() throws IOException {
        
        this.testDirectory = Files.createTempDirectory("test");
        this.path = this.testDirectory.resolve("test.md");
        
        try (DirectFileDataOutput output = new DirectFileDataOutput(this.path)) {
            
	        output.writeBytes(new byte[] {0, 1, 2, 3, 4, 5, 6, 7});	        
	        output.flush();
        }    
    }
    
    @After
    public void tearDown() throws IOException {
        
        FileUtils.forceDelete(this.testDirectory);
        this.path = null;
        this.testDirectory = null;
    }

	@Test
	public void testReadByte() throws IOException {
		
        try (DirectFileDataInput input = new DirectFileDataInput(this.path, 5)) {
            
        	assertEquals(0, input.readByte());	
        	assertTrue(input.isReadable());
        	assertEquals(1, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(2, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(3, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(4, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(5, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(6, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(7, input.readByte());
        	assertFalse(input.isReadable());
        	
        	try {
        		input.readByte();
        		fail();
        	} catch (IndexOutOfBoundsException e) {
        		
        		assertTrue(true);
        	}
        } 
	}

	@Test
	public void testSkip() throws IOException {
		
        try (DirectFileDataInput input = new DirectFileDataInput(this.path, 5)) {
            
        	input.skipBytes(0);	
        	assertTrue(input.isReadable());
        	input.skipBytes(1);
        	assertTrue(input.isReadable());
        	input.skipBytes(6);
        	assertTrue(input.isReadable());
        	input.skipBytes(1);
        	assertFalse(input.isReadable());
        } 
	}
	
	@Test
	public void testSkipWithIndexOutOfBondException() throws IOException {
		
        try (DirectFileDataInput input = new DirectFileDataInput(this.path, 5)) {
            
        	try {
        		input.skipBytes(10);	
        		fail();
        		
        	} catch (IndexOutOfBoundsException e) {
        		
        		assertTrue(true);
        	}
        } 
	}
	
	@Test
	public void testReadByteWithAFileSizeMultipleOfTheBufferSize() throws IOException {
		
        try (DirectFileDataInput input = new DirectFileDataInput(this.path, 4)) {
            
        	assertEquals(0, input.readByte());	
        	assertTrue(input.isReadable());
        	assertEquals(1, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(2, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(3, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(4, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(5, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(6, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(7, input.readByte());
        	assertFalse(input.isReadable());
        } 
	}
	
	@Test
	public void testReadBytes() throws IOException {
        
		try (DirectFileDataInput input = new DirectFileDataInput(this.path, 5)) {
            
			byte[] bytes = new byte[3];
			
			input.readBytes(bytes);
			
        	assertArrayEquals(new byte[]{0, 1, 2}, bytes);	
        	assertTrue(input.isReadable());
        	input.readBytes(bytes);
        	assertArrayEquals(new byte[]{3, 4, 5}, bytes);
        	assertTrue(input.isReadable());
        	input.readBytes(bytes, 0, 2);
        	assertArrayEquals(new byte[]{6, 7, 5}, bytes);        	
        	assertFalse(input.isReadable());
        } 
	}
	
	@Test
	public void testSlice() throws IOException {
        
		try (DirectFileDataInput input = new DirectFileDataInput(this.path, 6)) {
            
			ByteReader slice = input.slice(6);
			
			byte[] bytes = new byte[3];
			
			slice.readBytes(bytes);
			
        	assertArrayEquals(new byte[]{0, 1, 2}, bytes);	
        	assertTrue(slice.isReadable());
        	slice.readBytes(bytes);
        	assertArrayEquals(new byte[]{3, 4, 5}, bytes);
        	assertFalse(slice.isReadable());
        	assertTrue(input.isReadable());
        	slice = input.slice(2);
        	slice.readBytes(bytes, 0, 2);
        	assertArrayEquals(new byte[]{6, 7, 5}, bytes);        	
        	assertFalse(slice.isReadable());
        	assertFalse(input.isReadable());
        } 
	}
}
