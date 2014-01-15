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
package io.horizondb.db.btree;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * The writer used to write the B+Tree data to the disk.
 * 
 * @author Benjamin
 * 
 */
public interface NodeWriter<K extends Comparable<K>, V> extends Closeable, Flushable {

    /**
     * Writes the specified node to the underlying file.
     * 
     * @param node the node to write.
     * @returns the size on disk of the sub-tree.
     * @throws IOException if an I/O problem occurs while writing the node.
     */
    int writeNode(Node<K, V> node) throws IOException;

    /**
     * Writes the specified data to the underlying file.
     * 
     * @param value the data value.
     * @returns the size on disk of the value.
     * @throws IOException if an I/O problem occurs while writing the data.
     */
    int writeData(V value) throws IOException;

    /**
     * Writes the header corresponding to the specified root node.
     * 
     * @param node the root node.
     * @throws IOException if an I/O problem occurs while writing the data.
     */
    void writeRoot(Node<K, V> node) throws IOException;

    /**
     * Returns the current position within the file.
     * 
     * @return the current position within the underlying file.
     * @throws IOException if an I/O problem occurs while writing the data.
     */
    long getPosition() throws IOException;

    /**
     * Close this node writer without throwing an exception.
     */
    void closeQuietly();
}
