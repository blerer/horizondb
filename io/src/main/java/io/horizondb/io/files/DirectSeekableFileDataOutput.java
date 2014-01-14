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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * @author Benjamin
 *
 */
public final class DirectSeekableFileDataOutput extends DirectFileDataOutput implements SeekableFileDataOutput {

	/**
	 * @param path
	 * @param bufferSize
	 * @throws IOException
	 */
    public DirectSeekableFileDataOutput(Path path, int bufferSize) throws IOException {
	    super(((FileChannel) Files.newByteChannel(path, 
	                                              StandardOpenOption.CREATE,
	        		                              StandardOpenOption.WRITE)), 
	        		                              bufferSize);
    }

    public DirectSeekableFileDataOutput(Path path) throws IOException {
	    this(path, DEFAULT_BUFFER_SIZE);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void seek(long position) throws IOException {
		
		long currentPosition = getPosition();
		
		if (position == currentPosition) {
			return;
		}

		flush();
		getChannel().position(position);
	}
}
