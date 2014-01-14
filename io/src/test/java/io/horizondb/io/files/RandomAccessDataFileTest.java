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

import io.horizondb.test.AssertFiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.horizondb.io.files.FileUtils.ONE_KB;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * @author Benjamin
 *
 */
public class RandomAccessDataFileTest {

	/**
	 * 
	 */
    private static final int LOCKUP_DETECT_TIMEOUT = 100;
	/**
	 * The test directory.
	 */
    private Path testDirectory;
    
    @Before
    public void setUp() throws IOException {
        
        this.testDirectory = Files.createTempDirectory("test");
    }
    
    @After
    public void tearDown() throws IOException {
        
        FileUtils.forceDelete(this.testDirectory);
        this.testDirectory = null;
    }
	
	@Test
	public void testOpenWithNonExistingFile() throws IOException {
		
		try (RandomAccessDataFile file = RandomAccessDataFile.open(this.testDirectory.resolve("test.b3"), true)) {

			Assert.assertFalse(file.exists());
		}	
	}

	@Test
	public void testGetOutputWithRecycledOutput() throws IOException {
		
		Path path = this.testDirectory.resolve("test.b3");

		try (final RandomAccessDataFile file = RandomAccessDataFile.open(path, true)) {

			try (SeekableFileDataOutput output = file.getOutput()) {

				output.writeByte(2);
				output.flush();

			}
			
			try (SeekableFileDataOutput output = file.getOutput()) {

				output.writeByte(1);
				output.flush();

			}

		}
		AssertFiles.assertFileContains(new byte[] {2, 1}, path);
	}
	
	@Test
	public void testGetOutputWithNonRecycledOutput() throws IOException {
		
		Path path = this.testDirectory.resolve("test.b3");

		try (final RandomAccessDataFile file = RandomAccessDataFile.open(path, false)) {

			try (SeekableFileDataOutput output = file.getOutput()) {

				output.writeByte(2);
				output.flush();

			}
			
			try (SeekableFileDataOutput output = file.getOutput()) {

				output.seek(1);
				Assert.assertEquals(1, output.getPosition());
				output.writeByte(1);
				output.flush();

			}

		}
		AssertFiles.assertFileContains(new byte[] {2, 1}, path);
	}
	
	@Test
	public void testGetOutputWithMemoryMappedOutput() throws IOException {
		
		Path path = this.testDirectory.resolve("test.b3");

		try (final RandomAccessDataFile file = RandomAccessDataFile.mmap(path, 4 * ONE_KB)) {

			try (SeekableFileDataOutput output = file.getOutput()) {

				output.writeByte(2);
				output.flush();

			}
			
			try (SeekableFileDataOutput output = file.getOutput()) {

				output.writeByte(1);
				output.flush();

			}

		}
		AssertFiles.assertFileContainsAt(0, new byte[] {2, 1}, path);
	}
	
	@Test
	public void testGetOutputBlock() throws IOException, InterruptedException {
		
		Path path = this.testDirectory.resolve("test.b3");

		try (final RandomAccessDataFile file = RandomAccessDataFile.open(path, true)) {

			try (SeekableFileDataOutput output = file.getOutput()) {

				Thread taker = new Thread() {

					@Override
                    public void run() {
						
						try {
	                        file.getOutput();
	                        fail();
	                        
                        } catch (IllegalStateException success) {
                        
                        } catch (IOException e) {
                        	fail();
                        }
                    }

				};
				taker.start();
				Thread.sleep(LOCKUP_DETECT_TIMEOUT);
				taker.interrupt();
				taker.join(LOCKUP_DETECT_TIMEOUT);
				assertFalse(taker.isAlive());
			}
		}
	}
	
	@Test
	public void testGetOutputBlockWithNonRecycledOutput() throws IOException, InterruptedException {
		
		Path path = this.testDirectory.resolve("test.b3");

		try (final RandomAccessDataFile file = RandomAccessDataFile.open(path, false)) {

			try (SeekableFileDataOutput output = file.getOutput()) {

				Thread taker = new Thread() {

					@Override
                    public void run() {
						
						try {
	                        file.getOutput();
	                        fail();
	                        
                        } catch (IllegalStateException success) {
                        
                        } catch (IOException e) {
                        	fail();
                        }
                    }

				};
				taker.start();
				Thread.sleep(LOCKUP_DETECT_TIMEOUT);
				taker.interrupt();
				taker.join(LOCKUP_DETECT_TIMEOUT);
				assertFalse(taker.isAlive());
			}
		}
	}
	
	@Test
	public void testGetOutputBlockWithMemeoryMappedOutput() throws IOException, InterruptedException {
		
		Path path = this.testDirectory.resolve("test.b3");

		try (final RandomAccessDataFile file = RandomAccessDataFile.mmap(path, 4 * ONE_KB)) {

			try (SeekableFileDataOutput output = file.getOutput()) {

				Thread taker = new Thread() {

					@Override
                    public void run() {
						
						try {
	                        file.getOutput();
	                        fail();
	                        
                        } catch (IllegalStateException success) {
                        
                        } catch (IOException e) {
                        	fail();
                        }
                    }

				};
				taker.start();
				Thread.sleep(LOCKUP_DETECT_TIMEOUT);
				taker.interrupt();
				taker.join(LOCKUP_DETECT_TIMEOUT);
				assertFalse(taker.isAlive());
			}
		}
	}
}
