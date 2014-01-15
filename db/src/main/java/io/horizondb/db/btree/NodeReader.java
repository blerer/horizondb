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
import java.io.IOException;

/**
 * The reader used to read the B+Tree data from the disk.
 * 
 * @author Benjamin
 * 
 */
public interface NodeReader<K extends Comparable<K>, V> extends Closeable {

    /**
     * Reads the file header and returns the corresponding root node.
     * 
     * @param btree the B+Tree to which belongs the root node.
     * @throws IOException if an I/O problem occurs while reading the data.
     */
    Node<K, V> readRoot(BTree<K, V> btree) throws IOException;

    /**
     * Reads the node data located at the specified position.
     * 
     * @param btree the B+Tree to which belongs the node.
     * @param position the position of the node data within the file.
     * @return the node
     * @throws IOException if an I/O problem occurs while reading the data.
     */
    Node<K, V> readNode(BTree<K, V> btree, long position) throws IOException;

    /**
     * Returns the data located at the specified position.
     * 
     * @param position the position of the data within the file.
     * @return the data located at the specified position.
     * @throws IOException if an I/O problem occurs while reading the data.
     */
    V readData(long position) throws IOException;

    /**
     * Close this node reader without throwing an exception.
     */
    void closeQuietly();
}
