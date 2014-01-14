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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.horizondb.test.AssertFiles.assertFileContains;
import static io.horizondb.test.AssertFiles.assertFileSize;
import static org.junit.Assert.assertEquals;

public class DirectSeekableFileDataOuputTest {

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
    }
    
    @After
    public void tearDown() throws IOException {
        
        FileUtils.forceDelete(this.testDirectory);
        this.path = null;
        this.testDirectory = null;
    }
    
    @Test
    public void testSeekBackwardWithinBuffer() throws IOException {
        
        try (DirectSeekableFileDataOutput output = new DirectSeekableFileDataOutput(this.path, 3)) {
        
	        output.writeByte((byte) 1);
	        output.writeByte((byte) 2);
	        output.writeByte((byte) 3);
	        
	        assertFileSize(0, this.path);
	        assertEquals(3, output.getPosition());
	        
	        output.seek(1);
	        assertEquals(1, output.getPosition());
	        assertFileContains(new byte[]{1, 2, 3}, this.path);
	        
	        output.writeByte((byte) 4);
	
	        output.flush();
	        
	        assertFileContains(new byte[]{1, 4, 3}, this.path);
	        assertEquals(2, output.getPosition());
        }    
    }
    
    @Test
    public void testSeekForwardWithSameOutput() throws IOException {
        
        try (DirectSeekableFileDataOutput output = new DirectSeekableFileDataOutput(this.path, 10)) {
        
	        output.writeByte((byte) 1);
	        output.writeByte((byte) 2);
	        output.writeByte((byte) 3);
	        
	        assertFileSize(0, this.path);
	        assertEquals(3, output.getPosition());
	        
	        output.seek(8);
	        assertEquals(8, output.getPosition());
	        
	        output.writeByte((byte) 4);
	
	        output.flush();
	        
	        assertFileContains(new byte[]{1, 2, 3, 0, 0, 0, 0, 0, 4}, this.path);
	        assertEquals(9, output.getPosition());
        }    
    }
    
    @Test
    public void testSeekForwardWithNewOutput() throws IOException {
        
        try (DirectSeekableFileDataOutput output = new DirectSeekableFileDataOutput(this.path, 10)) {
        
	        output.writeByte((byte) 1);
	        output.writeByte((byte) 2);
	        output.writeByte((byte) 3);
	        
	        assertFileSize(0, this.path);
	        assertEquals(3, output.getPosition());
	
	        output.flush();
	        
	        assertFileContains(new byte[]{1, 2, 3}, this.path);
        }   
        
        try (DirectSeekableFileDataOutput output = new DirectSeekableFileDataOutput(this.path, 10)) {
            	        
	        output.seek(8);
	        assertEquals(8, output.getPosition());
	        assertFileSize(3, this.path);
	        
	        output.writeByte((byte) 4);
	
	        output.flush();
	        
	        assertFileContains(new byte[]{1, 2, 3, 0, 0, 0, 0, 0, 4}, this.path);
	        assertEquals(9, output.getPosition());
        } 
    }
    
    @Test
    public void testSeekWithNoChange() throws IOException {
        
        try (DirectSeekableFileDataOutput output = new DirectSeekableFileDataOutput(this.path, 10)) {
        
	        output.writeByte((byte) 1);
	        output.writeByte((byte) 2);
	        output.writeByte((byte) 3);
	        
	        assertFileSize(0, this.path);
	        assertEquals(3, output.getPosition());
	        
	        output.seek(3);
	        assertEquals(3, output.getPosition());
	        assertFileSize(0, this.path);
	        
	        output.writeByte((byte) 4);
	
	        output.flush();
	        
	        assertFileContains(new byte[]{1, 2, 3, 4}, this.path);
	        assertEquals(4, output.getPosition());
        }    
    }
    
    @Test
    public void testSeekOutsideBuffer() throws IOException {
        
        try (DirectSeekableFileDataOutput output = new DirectSeekableFileDataOutput(this.path, 3)) {
        
	        output.writeByte((byte) 1);
	        output.writeByte((byte) 2);
	        output.writeByte((byte) 3);
	        
	        assertFileSize(0, this.path);
	        assertEquals(3, output.getPosition());
	        
	        output.seek(5);
	        assertEquals(5, output.getPosition());
	        assertFileContains(new byte[]{1, 2, 3}, this.path);
	        
	        output.writeByte((byte) 4);
	
	        output.flush();
	        
	        assertFileContains(new byte[]{1, 2, 3, 0, 0, 4}, this.path);
	        assertEquals(6, output.getPosition());
        }    
    }
}
