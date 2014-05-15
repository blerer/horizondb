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
 * Proxy implementation of a <code>Node</code>.
 * <p>
 * This class will used the <code>OnDiskNodeManager</code> to load the real node on the fly.
 * </p>
 * 
 * @author Benjamin
 * 
 */
final class NodeProxy<K extends Comparable<K>, V> extends DataPointer<K, V> implements Node<K, V> {

    /**
     * The B+Tree to which the node belongs.
     */
    private final BTree<K, V> btree;

    /**
     * Creates a new proxy toward a node stored on disk.
     * 
     * @param manager The manager associated to this pointer.
     * @param position The position of the node data within the file.
     * @param subTreeSize The sub-tree size in bytes.
     */
    public NodeProxy(BTree<K, V> btree, long position, int subTreeSize) {

        super((OnDiskNodeManager<K, V>) btree.getManager(), position, subTreeSize);

        this.btree = btree;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getType() throws IOException {

        return loadNode().getType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V>[] insert(K key, V value) throws IOException {

        return loadNode().insert(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V> delete(K key) throws IOException {

        return loadNode().delete(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K getFirstKey() throws IOException {

        return loadNode().getFirstKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(K key) throws IOException {

        return loadNode().get(key);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public KeyValueIterator<K, V> iterator(K fromKey, K toKey) throws IOException {
        
        return loadNode().iterator(fromKey, toKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(K key) throws IOException {
        return loadNode().contains(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isFull() throws IOException {
        return loadNode().isFull();
    }

    /**
     * {@inheritDoc}
     * 
     * @throws IOException
     */
    @Override
    public boolean hasLessThanMinimumNumberOfElement() throws IOException {
        return loadNode().hasLessThanMinimumNumberOfElement();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasMoreThanMinimumNumberOfElement() throws IOException {
        return loadNode().hasMoreThanMinimumNumberOfElement();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V>[] rebalanceWithRightNode(Node<K, V> rightNode) throws IOException {
        return loadNode().rebalanceWithRightNode(rightNode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V>[] rebalanceWithLeftNode(Node<K, V> leftNode) throws IOException {
        return loadNode().rebalanceWithLeftNode(leftNode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V> merge(Node<K, V> rightNode) throws IOException {

        return loadNode().merge(rightNode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BTree<K, V> getBTree() {

        return this.btree;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeVisitResult accept(NodeVisitor<K, V> visitor) throws IOException {

        return loadNode().accept(visitor);
    }

    /**
     * Load the node into memory.
     * 
     * @return the real node.
     * @throws IOException if an I/O problem occurs while retrieving the node.
     */
    Node<K, V> loadNode() throws IOException {

        return getManager().loadNode(this);
    }
}
