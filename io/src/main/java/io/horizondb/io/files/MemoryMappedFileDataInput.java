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
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;

import java.io.IOException;
import java.nio.MappedByteBuffer;

import static org.apache.commons.lang.Validate.isTrue;

/**
 * <code>SeekableFileDataInput</code> that reads from a memory mapped file.
 * 
 * @author Benjamin
 *
 */
public class MemoryMappedFileDataInput extends AbstractFileDataInput implements SeekableFileDataInput {

    /**
     * The memory mapped byte buffer.
     */
    private MappedByteBuffer buffer;

    /**
     * The <code>Buffer</code> used to return data to the user.
     */
    private Buffer slice;

    /**
     * Creates a new <code>MemoryMappedFileDataInput</code> to read data from the 
     * specified memory mapped buffer.
     * 
     * @param buffer the memory mapped buffer.
     */
    public MemoryMappedFileDataInput(MappedByteBuffer buffer) {

    	this.buffer = (MappedByteBuffer) buffer.duplicate();
    	this.slice = Buffers.wrap(this.buffer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final MemoryMappedFileDataInput skipBytes(int numberOfBytes) throws IOException {

    	isTrue(numberOfBytes >= 0, "The number of bytes to skip must be greater or equals to zero");
    	
        this.buffer.position(this.buffer.position() + numberOfBytes);
        
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final byte readByte() throws IOException {

        return this.buffer.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final MemoryMappedFileDataInput readBytes(byte[] bytes) throws IOException {

        this.buffer.get(bytes);
        
        return this;
    }

    /**
     * {@inheritDoc}
     */    
    @Override
    public MemoryMappedFileDataInput readBytes(byte[] bytes, int offset, int length) throws IOException {
        
        this.buffer.get(bytes, offset, length);
        
        return this;
    }

	/**
     * {@inheritDoc}
     */
    @Override
    public final boolean isReadable() {

        return this.buffer.hasRemaining();
    }

	/**
     * {@inheritDoc}
     */
    @Override
    public final void close() throws IOException {

        this.slice = null;
        this.buffer = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final ReadableBuffer slice(int length) throws IOException {

    	isTrue(length <= this.buffer.capacity(), "The slice must be of length smaller or equals to the buffer size: " 
    	+ this.buffer.capacity() + " but was " + length);

        int position = this.buffer.position();
        
        this.slice.subRegion(position, length);

        this.buffer.position(position + length);

        return this.slice;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long size() throws IOException {
    	
    	return this.buffer.capacity();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public final long getPosition() throws IOException {
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
     * Returns the internal buffer.
     * 
     * @return the internal buffer.
     */
    protected final MappedByteBuffer getBuffer() {
    	
    	return this.buffer;
    }
}
