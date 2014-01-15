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

import java.io.IOException;

/**
 * Manager in charge of saving and loading BTree nodes.
 * 
 * @author Benjamin
 * 
 * @param <K> the Key type.
 * @param <V> the Value type.
 */
public interface NodeManager<K extends Comparable<K>, V> {

    /**
     * Returns the root node of the specified <code>BTree</code>.
     * 
     * @param btree the BTree.
     * @return the root node.
     * @throws IOException if an I/O problem occurs.
     */
    Node<K, V> getRoot(BTree<K, V> btree) throws IOException;

    /**
     * Sets the new root node.
     * 
     * @param root the new root node.
     * @throws IOException if an I/O problem occurs.
     */
    void setRoot(Node<K, V> root) throws IOException;

    /**
     * Allows this manager to take control of the storage of specified node.
     * 
     * @param node the node.
     * @return the decorated node.
     * @throws IOException if an I/O problem occurs.
     */
    @SuppressWarnings("unchecked")
    Node<K, V>[] wrapNodes(Node<K, V>... nodes) throws IOException;

    /**
     * Allows this manager to take control of the storage of specified node.
     * 
     * @param node the node.
     * @return the decorated node.
     * @throws IOException if an I/O problem occurs.
     */
    Node<K, V> wrapNode(Node<K, V> node) throws IOException;

    /**
     * Allows the user of the node to retrieve the original node.
     * 
     * @param node the decorated node.
     * @return the node.
     * @throws IOException if an I/O problem occurs.
     */
    Node<K, V> unwrapNode(Node<K, V> node) throws IOException;

    /**
     * Allows this manager to take control of the storage of specified value.
     * 
     * @param value the value.
     * @return the decorated value.
     * @throws IOException if an I/O problem occurs.
     */
    ValueWrapper<V> wrapValue(V value) throws IOException;
}
