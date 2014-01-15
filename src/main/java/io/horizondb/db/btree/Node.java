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

import io.horizondb.db.btree.NodeVisitor.NodeVisitResult;

import java.io.IOException;

/**
 * A node of the B+Tree.
 * 
 * @author Benjamin
 * 
 */
interface Node<K extends Comparable<K>, V> {

    /**
     * The internal node type.
     */
    static final int INTERNAL_NODE = 0;

    /**
     * The leaf node type.
     */
    static final int LEAF_NODE = 1;

    /**
     * Returns the node type.
     * 
     * @return the node type.
     * @throws IOException if an I/O problem occurs.
     */
    int getType() throws IOException;

    /**
     * Inserts the specified record.
     * 
     * @param key the record key.
     * @param value the value key.
     * @return the nodes resulting from the insertion.
     * @throws IOException if an I/O problem occurs.
     */
    Node<K, V>[] insert(K key, V value) throws IOException;

    /**
     * Deletes the record with the specified key.
     * 
     * @param key the key of the record to delete.
     * @return the nodes resulting from the deletion.
     * @throws IOException if an I/O problem occurs.
     */
    Node<K, V> delete(K key) throws IOException;

    /**
     * Returns the first key of this node.
     * 
     * @return the first key of this node.
     * @throws IOException if an I/O problem occurs.
     */
    K getFirstKey() throws IOException;

    /**
     * Returns the value associated to the specified key or <code>null</code> if it does not exists.
     * 
     * @param key the key of the value to retrieve
     * @return the value associated to the specified key if it exists or <code>null</code> if it does not.
     * @throws IOException if an I/O problem occurs.
     */
    V get(K key) throws IOException;

    /**
     * Returns <code>true</code> if this node or its children contains the specified key.
     * 
     * @param key the key to check.
     * @return <code>true</code> if this node or its children contains the specified key.
     * @throws IOException if an I/O problem occurs.
     */
    boolean contains(K key) throws IOException;

    /**
     * Checks if this node is full.
     * 
     * @return <code>true</code> if this node is full, <code>false</code> otherwise.
     * @throws IOException if an I/O problem occurs.
     */
    boolean isFull() throws IOException;

    /**
     * Returns <code>true</code> if this node has less than the minimum number of elements allowed, <code>false</code>
     * otherwise.
     * 
     * @return <code>true</code> if this node has less than the minimum number of elements allowed, <code>false</code>
     * otherwise.
     * @throws IOException if an I/O problem occurs.
     */
    boolean hasLessThanMinimumNumberOfElement() throws IOException;

    /**
     * Returns <code>true</code> if this node has more than the minimum number of elements allowed, <code>false</code>
     * otherwise.
     * 
     * @return <code>true</code> if this node has more than the minimum number of elements allowed, <code>false</code>
     * otherwise.
     * @throws IOException if an I/O problem occurs.
     */
    boolean hasMoreThanMinimumNumberOfElement() throws IOException;

    /**
     * Re-balances this node with its right sibling.
     * 
     * @param rightNode the right sibling of this node.
     * @return the nodes resulting from the re-balancing.
     * @throws IOException if an I/O problem occurs.
     */
    Node<K, V>[] rebalanceWithRightNode(Node<K, V> rightNode) throws IOException;

    /**
     * Re-balances this node with its left sibling.
     * 
     * @param leftNode the left sibling of this node.
     * @return the nodes resulting from the re-balancing.
     * @throws IOException if an I/O problem occurs.
     */
    Node<K, V>[] rebalanceWithLeftNode(Node<K, V> leftNode) throws IOException;

    /**
     * Merges this node with its right sibling.
     * 
     * @param rightNode the right sibling of this node.
     * @return the node resulting from the merge.
     */
    Node<K, V> merge(Node<K, V> rightNode) throws IOException;

    /**
     * Accepts the specified visitor.
     * 
     * @param visitor the visitor
     * @return the result of the visit.
     */
    NodeVisitResult accept(NodeVisitor<K, V> visitor) throws IOException;

    /**
     * Returns the B+Tree to which this node belongs.
     * 
     * @return
     */
    BTree<K, V> getBTree();
}
