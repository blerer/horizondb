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
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Benjamin
 *
 */
final class DirectFileAccessManager implements FileAccessManager {

    /**
	 * The file path.
	 */
	private final Path path;
	
	/**
	 * Creates a new <code>DirectFileAccessManager</code> for the specified path.
	 * 
	 * @param path the file path
	 */
    public DirectFileAccessManager(Path path) {
	    
	    this.path = path;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SeekableFileDataInput newInput() throws IOException {
		
		DirectSeekableFileDataInput input = new DirectSeekableFileDataInput(this.path);
		input.order(ByteOrder.LITTLE_ENDIAN);
		return input;
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
    public long size() throws IOException {
	    return Files.size(this.path);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public SeekableFileDataOutput newOuput() throws IOException {
		    	
		if (!Files.exists(this.path)) {
			
			Files.createFile(this.path);
		}
		
		DirectSeekableFileDataOutput output = new DirectSeekableFileDataOutput(this.path);
		output.order(ByteOrder.LITTLE_ENDIAN);
		return output;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void close() throws IOException {

    }
}
