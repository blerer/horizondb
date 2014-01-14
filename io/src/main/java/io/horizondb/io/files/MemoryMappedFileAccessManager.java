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
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;

/**
 * @author Benjamin
 *
 */
final class MemoryMappedFileAccessManager implements FileAccessManager {

	/**
	 * The file path.
	 */
	private final Path path;
	
    /**
	 * The memory mapped buffer.
	 */
	private MappedByteBuffer buffer;
	
	/**
	 * Creates a new <code>MemoryMappedFileAccessManager</code> for the specified path.
	 * 
	 * @param path the file path
	 * @throws IOException if the file cannot be mapped in memory.
	 */
    public MemoryMappedFileAccessManager(Path path) throws IOException {
	    
    	this.path = path;
	    this.buffer = FileUtils.mmap(path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Path getPath() {
		return this.path;
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public SeekableFileDataInput newInput() throws IOException {
		MemoryMappedFileDataInput input = new MemoryMappedFileDataInput(this.buffer);
		input.order(ByteOrder.LITTLE_ENDIAN);
		return input;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
    public long size() throws IOException {
	    return this.buffer.capacity();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public SeekableFileDataOutput newOuput() {
		
		MemoryMappedFileDataOutput output = new MemoryMappedFileDataOutput(this.buffer);
		output.order(ByteOrder.LITTLE_ENDIAN);
		return output;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void close() throws IOException {

    	FileUtils.munmap(this.buffer);
    }
}
