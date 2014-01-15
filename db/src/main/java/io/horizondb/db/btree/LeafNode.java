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
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.google.common.collect.Iterables;

import static io.horizondb.db.utils.ArrayUtils.toArray;

/**
 * A leaf node of a B+Tree.
 * 
 * @author Benjamin
 * 
 * @param <K> the Key type.
 * @param <V> the Value type.
 */
final class LeafNode<K extends Comparable<K>, V> extends AbstractNode<K, V> {

    /**
     * The node records.
     */
    private final SortedMap<K, ValueWrapper<V>> records;

    /**
     * Creates a new empty <code>LeafNode</code>.
     * 
     * @param btree the b+Tree to which this node belongs.
     */
    public LeafNode(BTree<K, V> btree) {

        this(btree, new TreeMap<K, ValueWrapper<V>>());
    }

    /**
     * Creates a new <code>LeafNode</code> that contains the specified records.
     * 
     * @param btree the b+Tree to which this node belongs.
     * @param records the node records.
     */
    private LeafNode(BTree<K, V> btree, SortedMap<K, ValueWrapper<V>> records) {

        super(btree);
        this.records = records;
    }

    /**
     * Creates a new <code>LeafNode</code> that contains the specified records.
     * 
     * @param btree the b+Tree to which this node belongs.
     * @param records the node records.
     */
    public static <K extends Comparable<K>, V> LeafNode<K, V> newInstance(BTree<K, V> btree,
                                                                          SortedMap<K, ValueWrapper<V>> records) {

        return new LeafNode<K, V>(btree, new TreeMap<K, ValueWrapper<V>>(records));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getType() {
        return LEAF_NODE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V>[] insert(K key, V value) throws IOException {

        LeafNode<K, V> newNode = copy().addRecord(key, value);

        if (newNode.isFull()) {

            return newNode.split();
        }

        return toArray(newNode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V> delete(K key) {

        return copy().removeRecord(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isFull() {

        return getNumberOfElements() >= getBranchingFactor();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K getFirstKey() {
        return this.records.firstKey();
    }

    /**
     * Returns the last key of this node.
     * 
     * @return the last key of this node.
     */
    public K getLastKey() {
        return this.records.lastKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {

        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("records", this.records).toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V> merge(Node<K, V> rightNode) throws IOException {

        LeafNode<K, V> newNode = new LeafNode<>(getBTree(), new TreeMap<>(this.records));
        newNode.records.putAll(((LeafNode<K, V>) getBTree().unwrapNode(rightNode)).records);

        return newNode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V>[] rebalanceWithRightNode(Node<K, V> rightNode) throws IOException {

        LeafNode<K, V> rightLeafNode = (LeafNode<K, V>) getBTree().unwrapNode(rightNode);

        K firstKey = rightLeafNode.getFirstKey();
        V firstKeyValue = rightLeafNode.get(firstKey);

        LeafNode<K, V> newRightNode = rightLeafNode.copy().removeRecord(firstKey);
        LeafNode<K, V> newLeftNode = copy().addRecord(firstKey, firstKeyValue);

        return toArray(newLeftNode, newRightNode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V>[] rebalanceWithLeftNode(Node<K, V> leftNode) throws IOException {

        LeafNode<K, V> leftLeafNode = (LeafNode<K, V>) getBTree().unwrapNode(leftNode);

        K lastKey = leftLeafNode.getLastKey();
        V LastKeyValue = leftLeafNode.get(lastKey);

        LeafNode<K, V> newRightNode = leftLeafNode.copy().removeRecord(lastKey);
        LeafNode<K, V> newLeftNode = copy().addRecord(lastKey, LastKeyValue);

        return toArray(newLeftNode, newRightNode);
    }

    /**
     * {@inheritDoc}
     * 
     * @throws IOException
     */
    @Override
    public NodeVisitResult accept(NodeVisitor<K, V> visitor) throws IOException {

        for (Entry<K, ValueWrapper<V>> entry : this.records.entrySet()) {

            NodeVisitResult result = visitor.visitRecord(entry.getKey(), entry.getValue());

            if (result == NodeVisitResult.TERMINATE) {

                return NodeVisitResult.TERMINATE;
            }

            if (result == NodeVisitResult.SKIP_SIBLINGS) {

                break;
            }
        }

        return NodeVisitResult.CONTINUE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int getNumberOfElements() {

        return this.records.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int getMinimumNumberOfElements() {

        return (int) Math.floor(getBranchingFactor() / 2.0);
    }

    /**
     * A <code>Map</code> containing the key-value mapping of this node.
     * 
     * @return a <code>Map</code> containing the key-value mapping of this node.
     * @throws IOException if an I/O problem occurs.
     */
    Map<K, V> toMap() throws IOException {

        Map<K, V> map = new TreeMap<>();

        for (Entry<K, ValueWrapper<V>> entry : this.records.entrySet()) {

            map.put(entry.getKey(), entry.getValue().getValue());
        }

        return map;
    }

    Map<K, ValueWrapper<V>> getRecords() {

        return Collections.unmodifiableMap(this.records);
    }

    /**
     * Splits this node in two.
     * 
     * @return the two new nodes created by the split.
     */
    private Node<K, V>[] split() {

        int half = getBranchingFactor() >> 1;

        K halfKey = Iterables.get(this.records.keySet(), half);

        SortedMap<K, ValueWrapper<V>> head = this.records.headMap(halfKey);
        SortedMap<K, ValueWrapper<V>> tail = this.records.tailMap(halfKey);

        return toArray(new LeafNode<K, V>(getBTree(), head), new LeafNode<K, V>(getBTree(), tail));
    }

    /**
     * 
     * {@inheritDoc}
     */
    @Override
    public V get(K key) throws IOException {

        ValueWrapper<V> value = this.records.get(key);

        if (value == null) {
            return null;
        }

        return value.getValue();
    }

    /**
     * 
     * {@inheritDoc}
     */
    @Override
    public boolean contains(K key) throws IOException {
        return this.records.containsKey(key);
    }

    /**
     * Adds the specified record to this node.
     * 
     * @param key the record key.
     * @param value the record value.
     * @return this node.
     * @throws IOException if an IO errors occurs while adding the record.
     */
    private LeafNode<K, V> addRecord(K key, V value) throws IOException {

        this.records.put(key, getBTree().wrapValue(value));

        return this;
    }

    /**
     * Returns a copy of this node.
     * 
     * @return a copy of this node.
     */
    private LeafNode<K, V> copy() {

        return new LeafNode<>(getBTree(), new TreeMap<>(this.records));
    }

    /**
     * Removes the record with the specified key.
     * 
     * @param key the key of the record to remove.
     * @return this node.
     */
    private LeafNode<K, V> removeRecord(K key) {

        this.records.remove(key);
        return this;
    }
}
