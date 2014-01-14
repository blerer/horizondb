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

import io.horizondb.io.AbstractByteWriter;

import java.io.IOException;
import java.nio.MappedByteBuffer;



/**
 * <code>SeekableFileDataOutput</code> that use a memory mapped byte buffer to write to the 
 * underlying file.
 * 
 * @author Benjamin
 *
 */
public final class MemoryMappedFileDataOutput extends AbstractByteWriter implements SeekableFileDataOutput {

    /**
     * The memory mapped byte buffer.
     */
    private MappedByteBuffer buffer;
    
    /**
     * Creates a new <code>MemoryMappedFileDataOutput</code> to write data to the 
     * specified file.
     * 
     * @param buffer the memory mapped buffer.
     */
    public MemoryMappedFileDataOutput(MappedByteBuffer buffer){

        this.buffer = buffer;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public MemoryMappedFileDataOutput writeByte(int b) throws IOException {

        this.buffer.put((byte) b);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public MemoryMappedFileDataOutput writeBytes(byte[] bytes, int offset, int length) throws IOException {
        
    	this.buffer.put(bytes, offset, length);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public MemoryMappedFileDataOutput writeZeroBytes(int length) throws IOException {
	    
    	for (int i = 0; i < length; i++) {
    		writeByte(0);
    	}
    	
    	return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public long getPosition() throws IOException {
	    return this.buffer.position();
    }
    
	/**
	 * {@inheritDoc}
	 */
    @Override
    public void seek(long position) throws IOException {
    	this.buffer.position((int) position);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() throws IOException {

        this.buffer.force();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {

    	FileUtils.munmap(this.buffer);
        this.buffer = null;
    }
}
