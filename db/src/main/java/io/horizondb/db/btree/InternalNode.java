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
import java.util.SortedMap;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.google.common.collect.Iterables;

import static io.horizondb.db.utils.ArrayUtils.toArray;

/**
 * An internal node of a BTree.
 * 
 * @author Benjamin
 * 
 * @param <K> the Key type.
 * @param <V> the Value type.
 */
@Immutable
final class InternalNode<K extends Comparable<K>, V> extends AbstractNode<K, V> {

    /**
     * The children of this node per first key.
     */
    private final NavigableMap<K, Node<K, V>> children;

    /**
     * Creates a new <code>InternalNode</code> that has the specified children.
     * 
     * @param btree the b+Tree to which this node belongs.
     * @param nodes the node children.
     * @throws IOException if an I/O problem occurs.
     */
    @SafeVarargs
    public InternalNode(BTree<K, V> btree, Node<K, V>... nodes) throws IOException {

        this(btree, new TreeMap<K, Node<K, V>>());
        addChildren(nodes);
    }

    /**
     * Creates a new <code>InternalNode</code> that contains the specified records.
     * 
     * @param btree the b+Tree to which this node belongs.
     * @param children the node children.
     */
    public static <K extends Comparable<K>, V> InternalNode<K, V> newInstance(BTree<K, V> btree,
                                                                              SortedMap<K, Node<K, V>> records) {

        return new InternalNode<K, V>(btree, new TreeMap<K, Node<K, V>>(records));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getType() {
        return INTERNAL_NODE;
    }

    /**
     * Creates a new <code>InternalNode</code> that has the specified children.
     * 
     * @param btree the b+Tree to which this node belongs.
     * @param children the node children.
     */
    private InternalNode(BTree<K, V> btree, NavigableMap<K, Node<K, V>> children) {

        super(btree);
        this.children = children;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V>[] insert(K key, V value) throws IOException {

        Node<K, V> oldNode = find(key);
        Node<K, V>[] newNodes = oldNode.insert(key, value);

        InternalNode<K, V> newInternalNode = copy().removeChild(oldNode).addChildren(newNodes);

        if (newInternalNode.isFull()) {

            return newInternalNode.split();
        }

        return toArray(newInternalNode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V>[] rebalanceWithRightNode(Node<K, V> rightNode) throws IOException {

        InternalNode<K, V> rightInternalNode = (InternalNode<K, V>) getBTree().unwrapNode(rightNode);

        Node<K, V> firstChild = rightInternalNode.getFirstChild();

        InternalNode<K, V> newRightNode = rightInternalNode.copy().removeChild(firstChild);
        InternalNode<K, V> newLeftNode = copy().addChild(firstChild);

        return toArray(newLeftNode, newRightNode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V>[] rebalanceWithLeftNode(Node<K, V> leftNode) throws IOException {

        InternalNode<K, V> leftInternalNode = (InternalNode<K, V>) getBTree().unwrapNode(leftNode);

        Node<K, V> lastChild = leftInternalNode.getLastChild();

        InternalNode<K, V> newLeftNode = leftInternalNode.copy().removeChild(lastChild);
        InternalNode<K, V> newRightNode = copy().addChild(lastChild);

        return toArray(newLeftNode, newRightNode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V> delete(K key) throws IOException {

        Node<K, V> oldNode = find(key);
        Node<K, V> newNode = oldNode.delete(key);

        if (newNode.hasLessThanMinimumNumberOfElement()) {

            return rebalanceOrMerge(oldNode, newNode);
        }

        return copy().removeChild(oldNode).addChild(newNode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K getFirstKey() {

        return this.children.firstKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(K key) throws IOException {
        return find(key).get(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(K key) throws IOException {
        return find(key).contains(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isFull() {

        return getNumberOfElements() > getBranchingFactor();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {

        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("children", this.children).toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V> merge(Node<K, V> right) {

        return copy().addChildrenFrom((InternalNode<K, V>) right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeVisitResult accept(NodeVisitor<K, V> visitor) throws IOException {

        for (Entry<K, Node<K, V>> entry : this.children.entrySet()) {

            K key = entry.getKey();
            Node<K, V> node = entry.getValue();

            NodeVisitResult result = visitor.preVisitNode(key, node);

            if (result == NodeVisitResult.TERMINATE) {

                return NodeVisitResult.TERMINATE;
            }

            if (result == NodeVisitResult.SKIP_SIBLINGS) {

                break;
            }

            if (result != NodeVisitResult.SKIP_SUBTREE) {

                if (node.accept(visitor) == NodeVisitResult.TERMINATE) {

                    return NodeVisitResult.TERMINATE;
                }
            }

            result = visitor.postVisitNode(key, node);

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

        return this.children.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int getMinimumNumberOfElements() {

        return (int) Math.ceil(getBranchingFactor() / 2.0);
    }

    /**
     * Returns an unmodifiable <code>Map</code> representation of this node.
     * 
     * @return an unmodifiable <code>Map</code> representation of this node.
     */
    Map<K, Node<K, V>> toMap() {

        return Collections.unmodifiableMap(this.children);
    }

    /**
     * Returns the child node with the specified index.
     * 
     * <p>
     * This method is intended for test only.
     * </p>
     * 
     * @param index the child index.
     * @return the child node with the specified index.
     */
    Node<K, V> getChild(int index) {

        return Iterables.get(this.children.values(), index);
    }

    /**
     * Returns an <code>Iterable</code> containing the keys of this node.
     * 
     * @return an <code>Iterable</code> containing the keys of this node.
     */
    Iterable<K> getKeys() {

        return Collections.unmodifiableSet(this.children.keySet());
    }

    /**
     * Finds the node which contains the specified key.
     * 
     * @param key the key for which the node must be found.
     * @return the node which contains the specified key.
     */
    private Node<K, V> find(K key) {

        Entry<K, Node<K, V>> entry = this.children.floorEntry(key);

        if (entry == null) {
            return this.children.firstEntry().getValue();
        }

        return entry.getValue();
    }

    /**
     * Returns the right sibling of the node which contains the specified key.
     * 
     * @param key the key.
     * @return the right sibling of the node which contains the specified key or <code>null</code> if the node has no
     * right sibling.
     */
    private Node<K, V> getRightNode(K key) {

        Entry<K, Node<K, V>> entry = this.children.higherEntry(key);

        if (entry == null) {

            return null;
        }

        return entry.getValue();
    }

    /**
     * Returns the left sibling of the node which contains the specified key.
     * 
     * @param key the key.
     * @return the left sibling of the node which contains the specified key or <code>null</code> if the node has no
     * left sibling.
     */
    private Node<K, V> getLeftNode(K key) {

        Entry<K, Node<K, V>> entry = this.children.lowerEntry(key);

        if (entry == null) {

            return null;
        }

        return entry.getValue();
    }

    /**
     * Adds to the children of this node the children of the specified one.
     * 
     * @param node the node from which the children must be added.
     * @return this node.
     */
    private InternalNode<K, V> addChildrenFrom(InternalNode<K, V> node) {

        this.children.putAll(node.children);
        return this;
    }

    /**
     * Returns <code>true</code> if the specified node is not null and has more than the minimum number of elements,
     * <code>false</code> otherwise.
     * 
     * @param node the node to check.
     * @return <code>true</code> if the specified node is not null and has more than the minimum number of elements,
     * <code>false</code> otherwise.
     * @throws IOException if an I/O problem occurs.
     */
    private static <K extends Comparable<K>, V>
            boolean
            hasNodeMoreThanMinimumNumberOfElement(Node<K, V> node) throws IOException {

        return node != null && node.hasMoreThanMinimumNumberOfElement();
    }

    /**
     * Returns <code>true</code> if this node has only one child, <code>false</code> otherwise.
     * 
     * @return <code>true</code> if this node has only one child, <code>false</code> otherwise.
     */
    private boolean hasOnlyOneChild() {

        return this.children.size() == 1;
    }

    /**
     * @return
     */
    private Node<K, V>[] split() {

        int half = getBranchingFactor() >> 1;

        K halfKey = Iterables.get(this.children.keySet(), half + 1);

        NavigableMap<K, Node<K, V>> head = (NavigableMap<K, Node<K, V>>) this.children.headMap(halfKey);
        NavigableMap<K, Node<K, V>> tail = (NavigableMap<K, Node<K, V>>) this.children.tailMap(halfKey);

        return toArray(new InternalNode<K, V>(getBTree(), head), new InternalNode<K, V>(getBTree(), tail));
    }

    /**
     * Returns the first child of this internal node.
     * 
     * @return the first child of this internal node.
     */
    private Node<K, V> getFirstChild() {

        return this.children.firstEntry().getValue();
    }

    /**
     * Returns the last child of this internal node.
     * 
     * @return the last child of this internal node.
     */
    private Node<K, V> getLastChild() {

        return this.children.lastEntry().getValue();
    }

    /**
     * Adds the specified nodes to the children of this node.
     * 
     * @param nodes the nodes to add.
     * @return this node.
     * @throws IOException if an I/O problem occurs.
     */
    @SafeVarargs
    private final InternalNode<K, V> addChildren(Node<K, V>... nodes) throws IOException {

        for (Node<K, V> node : nodes) {

            addChild(node);
        }
        return this;
    }

    /**
     * Adds the specified node to the children of this node.
     * 
     * @param node the node to add.
     * @return this node.
     * @throws IOException if an I/O problem occurs.
     */
    private InternalNode<K, V> addChild(Node<K, V> node) throws IOException {

        this.children.put(node.getFirstKey(), getBTree().wrapNode(node));
        return this;
    }

    /**
     * Removes the specified children of this node.
     * 
     * @param nodes the children to remove.
     * @return this node.
     * @throws IOException if an I/O problem occurs.
     */
    @SafeVarargs
    private final InternalNode<K, V> removeChildren(Node<K, V>... nodes) throws IOException {

        for (Node<K, V> node : nodes) {

            removeChild(node);
        }
        return this;
    }

    /**
     * Re-balances if possible the specified node with its siblings or if it is not possible merge it with one of its
     * siblings.
     * 
     * @param index the node index.
     * @param node the node on which re-balancing or merging must be performed.
     * @throws IOException if an IO problem occurs.
     */
    private Node<K, V> rebalanceOrMerge(Node<K, V> oldNode, Node<K, V> newNode) throws IOException {

        Node<K, V> rightNode = getRightNode(oldNode.getFirstKey());

        if (hasNodeMoreThanMinimumNumberOfElement(rightNode)) {

            return copy().removeChildren(oldNode, rightNode).addChildren(newNode.rebalanceWithRightNode(rightNode));
        }

        Node<K, V> leftNode = getLeftNode(oldNode.getFirstKey());

        if (hasNodeMoreThanMinimumNumberOfElement(leftNode)) {

            return copy().removeChildren(leftNode, oldNode).addChildren(newNode.rebalanceWithLeftNode(leftNode));
        }

        if (rightNode != null) {

            Node<K, V> mergedNode = newNode.merge(rightNode);

            InternalNode<K, V> newInternalNode = copy().removeChildren(oldNode, rightNode).addChildren(mergedNode);

            if (newInternalNode.hasOnlyOneChild()) {

                return mergedNode;
            }

            return newInternalNode;
        }

        Node<K, V> mergedNode = leftNode.merge(newNode);

        InternalNode<K, V> newInternalNode = copy().removeChildren(leftNode, oldNode).addChildren(mergedNode);

        if (newInternalNode.hasOnlyOneChild()) {

            return mergedNode;
        }

        return newInternalNode;
    }

    /**
     * Removes the specified child of this node.
     * 
     * @param node the child to remove.
     * @return this node.
     * @throws IOException if an I/O problem occurs.
     */
    private InternalNode<K, V> removeChild(Node<K, V> node) throws IOException {

        this.children.remove(node.getFirstKey());
        return this;
    }

    /**
     * Returns a copy of this node.
     * 
     * @return a copy of this node.
     */
    private InternalNode<K, V> copy() {

        return new InternalNode<K, V>(getBTree(), new TreeMap<>(this.children));
    }
}
