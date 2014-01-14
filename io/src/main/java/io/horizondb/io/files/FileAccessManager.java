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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * @author Benjamin
 *
 */
interface FileAccessManager extends Closeable {
	
	/**
	 * Return the path to the file.
	 * @return the path to the file.
	 */
	Path getPath();
	
	/**
	 * Returns a new input to read the file data. 
	 * 
	 * @return a new <code>SeekableFileDataInput</code>.
	 * @throws IOException if a problem occurs while creating the input.
	 */
    SeekableFileDataInput newInput() throws IOException;
    
	/**
	 * Creates a new output to write the file data. 
	 * 
	 * @return a new <code>SeekableFileDataOutput</code>.
	 * @throws IOException if a problem occurs while creating the output.
	 */
    SeekableFileDataOutput newOuput() throws IOException;

    /**
     * Returns the file size.
     * 
     * @return the file size.
     * @throws IOException if a problem occurs while retrieving the file size. 
     */
    long size() throws IOException;
}
