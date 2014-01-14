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

import io.horizondb.io.ByteWriter;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * Interface used to write Data to an underlying file.
 * 
 * @author Benjamin
 *
 */
public interface FileDataOutput extends ByteWriter, Closeable, Flushable {
    
    /**
     * Returns the number of bytes from the beginning of the file to the current position.
     * 
     * @return the number of bytes from the beginning of the file to the current position.
     * @throws IOException if an I/O error occurs.
     */
    long getPosition() throws IOException;
}