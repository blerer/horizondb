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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.horizondb.test.AssertFiles.assertFileContains;
import static io.horizondb.test.AssertFiles.assertFileSize;

import static org.junit.Assert.assertEquals;

public class DirectFileDataOuputTest {

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
    public void testWriteByte() throws IOException {
        
        try (DirectFileDataOutput output = new DirectFileDataOutput(this.path)) {
        
	        output.writeByte((byte) 1);
	        output.writeByte((byte) 2);
	        output.writeByte((byte) 3);
	        
	        assertFileSize(0, this.path);
	        assertEquals(3, output.getPosition());
	        
	        output.flush();
	
	        assertFileContains(new byte[]{1, 2 ,3}, this.path);
	        assertEquals(3, output.getPosition());
        }    
    }
    
    @Test
    public void testWriteBytes() throws IOException {
        
        try (DirectFileDataOutput output = new DirectFileDataOutput(this.path)) {
        
	        output.writeBytes(new byte[] {1, 2, 3, 4, 5}, 1, 3);
	        
	        assertFileSize(0, this.path);
	        assertEquals(3, output.getPosition());
	        
	        output.flush();
	
	        assertFileContains(new byte[]{2, 3, 4}, this.path);
	        assertEquals(3, output.getPosition());
        }    
    }
}
