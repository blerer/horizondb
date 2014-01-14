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

import io.horizondb.io.files.DirectFileDataOutput;
import io.horizondb.io.files.DirectSeekableFileDataInput;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 *
 */
public class DirectSeekableFileDataInputTest {

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
        
        this.testDirectory = Files.createTempDirectory(DirectSeekableFileDataInputTest.class.getSimpleName());
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
	public void testReadByteWithSeekWithinBuffer() throws IOException {
		
        try (DirectSeekableFileDataInput input = new DirectSeekableFileDataInput(this.path, 5)) {
            
        	assertEquals(0, input.readByte());	
        	assertTrue(input.isReadable());
        	assertEquals(1, input.readByte());
        	assertTrue(input.isReadable());
        	
        	input.seek(4);
        	
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
	public void testReadByteWithSeekOutsideBuffer() throws IOException {
		
        try (DirectSeekableFileDataInput input = new DirectSeekableFileDataInput(this.path, 3)) {
            
        	assertEquals(0, input.readByte());	
        	assertTrue(input.isReadable());
        	assertEquals(1, input.readByte());
        	assertTrue(input.isReadable());
        	
        	input.seek(5);
        	
        	assertEquals(5, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(6, input.readByte());
        	assertTrue(input.isReadable());
        	assertEquals(7, input.readByte());
        	assertFalse(input.isReadable());
        } 
	}
	
	@Test
	public void testGetPositionAfterSeek() throws IOException {
		 
		try (DirectSeekableFileDataInput input = new DirectSeekableFileDataInput(this.path, 3)) {
	            	        	
	        	input.seek(5);
	        	assertEquals(5, input.getPosition());
	    } 
	}
	
	@Test
	public void testGetPositionAfterSeekAndBufferNotFull() throws IOException {
		 
		try (DirectSeekableFileDataInput input = new DirectSeekableFileDataInput(this.path, 3)) {
	            	        	
	        	input.seek(6);
	        	assertEquals(6, input.getPosition());
	    } 
	}
}
