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
import java.nio.file.Path;

/**
 * @author Benjamin
 *
 */
public final class DirectSeekableFileDataInput extends DirectFileDataInput implements SeekableFileDataInput {

	/**
	 * @param path
	 * @param bufferSize
	 * @throws IOException
	 */
    public DirectSeekableFileDataInput(Path path, int bufferSize) throws IOException {
	    super(path, bufferSize);
    }

    public DirectSeekableFileDataInput(Path path) throws IOException {
	    super(path);
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void seek(long position) throws IOException {
		
		long bufferEnd = getChannel().position();
		int bufferSize = getBuffer().limit();
		long bufferStart = bufferEnd - bufferSize;
		
		if(position >= bufferStart && position < bufferEnd) {
			
			getBuffer().position((int)(position - bufferStart));
		
		} else {
			
			getBuffer().clear();
			
			getChannel().position(position);
			fillBuffer();
		}
	}
}
