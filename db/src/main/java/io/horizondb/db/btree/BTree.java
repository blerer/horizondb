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

import org.apache.commons.lang.Validate;

/**
 * B+Tree implementation.
 * 
 * @author Benjamin
 * 
 * @param <K> the key type.
 * @param <V> the value type.
 */
public final class BTree<K extends Comparable<K>, V> {

    /**
     * The branching factor
     */
    private final int b;

    /**
     * The <code>NodeManager</code> in charge of loading and saving the node information.
     */
    private final NodeManager<K, V> manager;

    /**
     * Creates a new <code>BTree</code> instance.
     * 
     * @param manager The <code>NodeManager</code> in charge of loading and saving the node information.
     * @param b The branching factor
     */
    public BTree(NodeManager<K, V> manager, int b) {

        Validate.notNull(manager, "manager parameter must not be null");

        this.manager = manager;
        this.b = b;
    }

    /**
     * Inserts the specified record into this tree.
     * 
     * @param key the record key.
     * @param value the record value.
     * @throws IOException if an IO problem occurs.
     */
    public synchronized void insert(K key, V value) throws IOException {

        Node<K, V>[] nodes = getRoot().insert(key, value);

        if (nodes.length == 1) {

            setRoot(nodes[0]);

        } else {

            setRoot(new InternalNode<>(this, nodes));
        }
    }

    /**
     * Inserts the specified record into this tree if no record exists with the specified key.
     * 
     * @param key the record key.
     * @param value the record value.
     * @throws IOException if an IO problem occurs.
     */
    public synchronized boolean insertIfAbsent(K key, V value) throws IOException {

        if (contains(key)) {
            return false;
        }

        insert(key, value);
        return true;
    }

    /**
     * Delete the record with the specified key.
     * 
     * @param key the key of the record to delete.
     * @throws IOException if an I/O problem occurs.
     */
    public synchronized void delete(K key) throws IOException {

        setRoot(getRoot().delete(key));
    }

    /**
     * Returns the value associated to the specified key if it exists or <code>null</code> if it does not.
     * 
     * @param key the key of the value to retrieve
     * @return the value associated to the specified key if it exists or <code>null</code> if it does not.
     * @throws IOException if an I/O problem occurs.
     */
    public V get(K key) throws IOException {

        return getRoot().get(key);
    }

    /**
     * Returns <code>true</code> if this B+Tree contains the specified key, <code>false</code> otherwise.
     * 
     * @param key the key to check
     * @return <code>true</code> if this B+Tree contains the specified key, <code>false</code> otherwise.
     * @throws IOException if an I/O problem occurs.
     */
    public boolean contains(K key) throws IOException {

        return getRoot().contains(key);
    }

    /**
     * Accepts the specified visitor.
     * 
     * @param visitor the visitor
     * @return the result of the visit.
     * @throws IOException if an I/O problem occurs.
     */
    public void accept(NodeVisitor<K, V> visitor) throws IOException {

        if (visitor == null) {
            return;
        }

        Node<K, V> root = getRoot();

        NodeVisitResult result = visitor.preVisitNode(root.getFirstKey(), root);

        if (result == NodeVisitResult.TERMINATE || result == NodeVisitResult.SKIP_SIBLINGS) {

            return;
        }

        if (result == NodeVisitResult.CONTINUE) {
            result = root.accept(visitor);
        }

        if (result != NodeVisitResult.TERMINATE) {

            return;
        }

        visitor.postVisitNode(root.getFirstKey(), root);
    }

    /**
     * Returns the root node.
     * 
     * @return the root node.
     * @throws IOException if an I/O exception occurs.
     */
    synchronized Node<K, V> getRoot() throws IOException {

        return this.manager.getRoot(this);
    }

    /**
     * Returns the branching factor.
     * 
     * @return the branching factor.
     */
    int getBranchingFactor() {
        return this.b;
    }

    /**
     * Returns the node manager used by this B+Tree.
     * 
     * @return the node manager used by this B+Tree.
     */
    NodeManager<K, V> getManager() {
        return this.manager;
    }

    /**
     * Allows this manager to take control of the storage of specified node.
     * 
     * @param node the node.
     * @return the decorated node.
     * @throws IOException if an I/O problem occurs.
     */
    Node<K, V> wrapNode(Node<K, V> node) throws IOException {

        return this.manager.wrapNode(node);
    }

    /**
     * Allows the user of the node to retrieve the original node.
     * 
     * @param node the decorated node.
     * @return the node.
     * @throws IOException if an I/O problem occurs.
     */
    Node<K, V> unwrapNode(Node<K, V> node) throws IOException {

        return this.manager.unwrapNode(node);
    }

    /**
     * Allows this manager to take control of the storage of specified value.
     * 
     * @param value the value.
     * @return the decorated value.
     * @throws IOException if an I/O problem occurs.
     */
    ValueWrapper<V> wrapValue(V value) throws IOException {

        return this.manager.wrapValue(value);
    }

    /**
     * Returns the root node.
     * 
     * @return the root node.
     * @throws IOException if an IO problem occurs while writing the data.
     */
    private void setRoot(Node<K, V> root) throws IOException {

        this.manager.setRoot(root);
    }
}
