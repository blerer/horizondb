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

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;

import java.io.EOFException;
import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Benjamin
 *
 */
public class TruncatedSeekableFileDataInputTest {

	@Test
	public void testSize() throws IOException {
		
		Buffer buffer = Buffers.wrap(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6});
		SeekableFileDataInput input = SeekableFileDataInputs.toSeekableFileDataInput(buffer);
		assertEquals(10, input.size());
		
		try (TruncatedSeekableFileDataInput truncatedInput = new TruncatedSeekableFileDataInput(input, 5)) {
	
			assertEquals(5, truncatedInput.size());
		}
	}
	
    @Test
	public void testSeek() throws IOException {
		
		Buffer buffer = Buffers.wrap(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6});
		SeekableFileDataInput input = SeekableFileDataInputs.toSeekableFileDataInput(buffer);

		try (TruncatedSeekableFileDataInput truncatedInput = new TruncatedSeekableFileDataInput(input, 7)) {
	
			truncatedInput.seek(5);
			assertEquals(4, truncatedInput.readByte());
			
			try {
				
				truncatedInput.seek(25);
				fail();
				
			} catch (EOFException e) {
				
				assertTrue(true);
			}
		}
	}
    
    @Test
	public void testSeekWithOffSet() throws IOException {
		
		Buffer buffer = Buffers.wrap(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6});
		SeekableFileDataInput input = SeekableFileDataInputs.toSeekableFileDataInput(buffer);

		try (TruncatedSeekableFileDataInput truncatedInput = new TruncatedSeekableFileDataInput(input, 1, 7)) {
	
			truncatedInput.seek(5);
			assertEquals(5, truncatedInput.readByte());
			
			try {
				
				truncatedInput.seek(25);
				fail();
				
			} catch (EOFException e) {
				
				assertTrue(true);
			}
		}
	}

	@Test
	public void testReadByte() throws IOException {
		
		Buffer buffer = Buffers.wrap(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6});
		SeekableFileDataInput input = SeekableFileDataInputs.toSeekableFileDataInput(buffer);
		
		try (TruncatedSeekableFileDataInput truncatedInput = new TruncatedSeekableFileDataInput(input, 2)) {
			
			assertTrue(truncatedInput.isReadable());
			assertEquals(2, truncatedInput.readByte());
			assertTrue(truncatedInput.isReadable());
			assertEquals(-120, truncatedInput.readByte());
			assertFalse(truncatedInput.isReadable());
			
			try {
			
				truncatedInput.readByte();
				fail();
				
			} catch (EOFException e) {
				
				assertTrue(true);
			}
		}
	}
	
	@Test
	public void testReadByteWithOffset() throws IOException {
		
		Buffer buffer = Buffers.wrap(new byte[]{2, -120, 0, 0, 0, 4, 5, 6, 7, 6});
		SeekableFileDataInput input = SeekableFileDataInputs.toSeekableFileDataInput(buffer);
		
		try (TruncatedSeekableFileDataInput truncatedInput = new TruncatedSeekableFileDataInput(input, 5, 2)) {
			
			assertEquals(0, truncatedInput.getPosition());
			assertTrue(truncatedInput.isReadable());
			assertEquals(4, truncatedInput.readByte());
			assertTrue(truncatedInput.isReadable());
			assertEquals(1, truncatedInput.getPosition());
			assertEquals(5, truncatedInput.readByte());
			assertFalse(truncatedInput.isReadable());
			
			try {
			
				truncatedInput.readByte();
				fail();
				
			} catch (EOFException e) {
				
				assertTrue(true);
			}
		}
	}
}
