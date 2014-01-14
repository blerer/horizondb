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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.horizondb.io.files.FileUtils.ONE_KB;



/**
 * <code>FileDataOutput</code> that use a direct buffer to write to the 
 * underlying file.
 * 
 * @author Benjamin
 *
 */
public class DirectFileDataOutput extends AbstractByteWriter implements FileDataOutput {

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
     * Creates a new <code>DirectDataOutput</code> to write data to the 
     * specified file.
     * 
     * @param path the path to the file to write to.
     * @param bufferSize the size of the internal buffer.
     */
    public DirectFileDataOutput(Path path, int bufferSize) throws IOException {

		this((FileChannel) Files.newByteChannel(path,
		                                        StandardOpenOption.CREATE,
		                                        StandardOpenOption.WRITE,
		                                        StandardOpenOption.APPEND), bufferSize);
    }

    /**
     * Creates a new <code>DirectDataOutput</code> to write data to the 
     * specified file.
     * 
     * @param path the path to the file to write to.
     */
    public DirectFileDataOutput(Path path) throws IOException {

        this(path, DEFAULT_BUFFER_SIZE);
    }
    
    /**
     * Creates a new <code>DirectDataOutput</code> to write data to the 
     * specified file.
     * 
     * @param channel the file channel.
     * @param bufferSize the size of the internal buffer.
     */
    protected DirectFileDataOutput(FileChannel channel, int bufferSize) {

        this.channel = channel;
        this.buffer = ByteBuffer.allocateDirect(bufferSize);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final DirectFileDataOutput writeByte(int b) throws IOException {

        flushIfNeeded(1);
        this.buffer.put((byte) b);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public final  DirectFileDataOutput writeBytes(byte[] bytes, int offset, int length) throws IOException {
        flushIfNeeded(length);
        this.buffer.put(bytes, offset, length);
        
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public final DirectFileDataOutput writeZeroBytes(int length) throws IOException {
	    
    	for (int i = 0; i < length; i++) {
    		writeByte(0);
    	}
    	
    	return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public final long getPosition() throws IOException {
    	
	    return this.channel.position() + this.buffer.position();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final void flush() throws IOException {

    	if (this.buffer.position() == 0) {
    		return;
    	}
    	
        this.buffer.flip();
        this.channel.write(this.buffer);
        this.buffer.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void close() throws IOException {    	
        this.channel.close();
    }

    /**
     * Returns the underlying channel.    
     * @return the underlying channel. 
     */
	protected final FileChannel getChannel() {
		return this.channel;
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
     * Flush the buffer if there are not enough space for the specified number
     * of bytes.
     * 
     * @throws IOException if a problem occurs while writing to the file. 
     */
    private final void flushIfNeeded(int aNumberOfBytes) throws IOException {

        if (this.buffer.capacity() - this.buffer.position() < aNumberOfBytes) {
            flush();
        }
    }
}
