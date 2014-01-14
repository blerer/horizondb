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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.horizondb.io.files.FileUtils.ONE_KB;
import static org.apache.commons.lang.Validate.isTrue;
import static org.apache.commons.lang.Validate.notNull;

/**
 * A stream used to read data from an underlying file.
 * 
 * @author Benjamin
 *
 */
public class DirectFileDataInput extends AbstractFileDataInput {

    /**
     * The default size for the direct direct buffer.
     */
    public static final int DEFAULT_BUFFER_SIZE = 8 * ONE_KB;

    /**
     * The byte buffer used to write to the file.
     */
    private ByteBuffer buffer;

    /**
     * The channel used to write to the file.
     */
    private FileChannel channel;

    /**
     * <code>true</code> if the Channel has still some remaining bytes to be 
     * loaded in the buffer.
     */
    private boolean hasChannelRemainingBytes = true;

    /**
     * The <code>Buffer</code> used to return data to the user.
     */
    private Buffer slice;

    /**
     * Creates a new <code>DirectFileDataInput</code> to read data from the 
     * specified file.
     * 
     * @param path the file path.
     */
    public DirectFileDataInput(Path path) throws IOException {

        this(path, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a new <code>DirectFileDataInput</code> to read data from the 
     * specified file.
     * 
     * @param path the file path.
     * @param bufferSize the size of the buffer being used.
     */
    public DirectFileDataInput(Path path, int bufferSize) throws IOException {

        notNull(path, "path parameter must not be null");
        isTrue(bufferSize > 0, "the buffer size must be greater than zero");

        this.channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ);
        
        this.buffer = ByteBuffer.allocateDirect(bufferSize);
        this.slice = Buffers.wrap(this.buffer);

        fillBuffer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final DirectFileDataInput skipBytes(int numberOfBytes) throws IOException {

    	isTrue(numberOfBytes >= 0, "The number of bytes to skip must be greater or equals to zero");
    	
        int numberOfByteToRead = numberOfBytes;

        try {
        	
	        while (numberOfByteToRead > 0) {
	            if (!this.buffer.hasRemaining()) {
	                readData();
	            }
	            this.buffer.get();
	            numberOfByteToRead--;
	        }
	        
        } catch (BufferUnderflowException e) {

	        throw new IndexOutOfBoundsException("Index: " + (getPosition() + numberOfByteToRead) +
                                                ", Size: " + size());
        }
        
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final byte readByte() throws IOException {

        readDataIfNeeded(1L);
        return this.buffer.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final DirectFileDataInput readBytes(byte[] bytes) throws IOException {

        readDataIfNeeded(bytes.length);
        this.buffer.get(bytes);
        
        return this;
    }

    /**
     * {@inheritDoc}
     */    
    @Override
    public DirectFileDataInput readBytes(byte[] bytes, int offset, int length) throws IOException {
        
    	readDataIfNeeded(length);
        this.buffer.get(bytes, offset, length);
        
        return this;
    }

	/**
     * {@inheritDoc}
     */
    @Override
    public final boolean isReadable() {

        if (!this.hasChannelRemainingBytes) {
            return this.buffer.hasRemaining();
        }

        return true;
    }

	/**
     * {@inheritDoc}
     */
    @Override
    public final void close() throws IOException {

        this.channel.close();
        this.buffer = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final ReadableBuffer slice(int length) throws IOException {

    	isTrue(length <= this.buffer.capacity(), "The slice must be of length smaller or equals to the buffer size: " 
    	+ this.buffer.capacity() + " but was " + length);
    	
        readDataIfNeeded(length);

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
    	
    	return this.channel.size();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public final long getPosition() throws IOException {
	    return this.channel.position() - this.buffer.remaining();
    }

	/**
     * Returns the internal buffer.
     * 
     * @return the internal buffer.
     */
    protected final ByteBuffer getBuffer() {
    	
    	return this.buffer;
    }
    
    /**
     * Returns the channel used to load the data.
     * 
     * @return the channel used to load the data.
     */
    protected final FileChannel getChannel() {
    	
    	return this.channel;
    }
    
    /**
     * Fills the buffer with the next available data from the file.
     * 
     * @return The number of bytes read, possibly zero, or <tt>-1</tt> if the channel has reached end-of-stream
     * @throws IOException if an I/O problem occurs.
     */
    protected final int fillBuffer() throws IOException {

        int numberOfBytesRead = this.channel.read(this.buffer);

        long channelRemainingBytes = this.channel.size() - this.channel.position();

        this.hasChannelRemainingBytes = channelRemainingBytes > 0;

        this.buffer.flip();
        
        return numberOfBytesRead;
    }
    
    /**
     * Reload data in the buffer if it does not contains the specified amount of bytes to read.  
     * 
     * @param numberOfBytes the number of bytes.
     * @throws IOException if a problem occurs while reading data.
     */
    private void readDataIfNeeded(long numberOfBytes) throws IOException {

        int remaining = this.buffer.remaining();
        
		if (remaining < numberOfBytes) {
			
            int numberOfBytesRead = readData();
            
            if ((remaining + numberOfBytesRead) < numberOfBytes) {
            	
            	throw new IndexOutOfBoundsException("Index: " + (getPosition() + numberOfBytes) +
                                                    ", Size: " + size());
            }
        }
    }

    /**
     * Fill the buffer.  
     * 
     * @return the number of bytes read.
     * @throws IOException if a problem occurs while reading data.
     */
    private int readData() throws IOException {

        this.buffer.compact();
        
        return fillBuffer();
    }
}
