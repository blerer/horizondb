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

/**
 * Base class for the <code>Node</code> classes.
 * 
 * @author Benjamin
 * 
 */
abstract class AbstractNode<K extends Comparable<K>, V> implements Node<K, V> {

    /**
     * The B+Tree to which this node belongs.
     */
    private final BTree<K, V> btree;

    /**
     * Creates a new <code>AbstractNode</code>.
     * 
     * @param btree the B+Tree to which this node belongs.
     */
    public AbstractNode(BTree<K, V> btree) {
        this.btree = btree;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean hasMoreThanMinimumNumberOfElement() {

        return getNumberOfElements() > getMinimumNumberOfElements();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean hasLessThanMinimumNumberOfElement() {

        return getNumberOfElements() < getMinimumNumberOfElements();
    }

    /**
     * Returns the B+Tree branching factor.
     * 
     * @return the B+Tree branching factor.
     */
    protected int getBranchingFactor() {

        return this.btree.getBranchingFactor();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BTree<K, V> getBTree() {

        return this.btree;
    }

    /**
     * Returns the number of elements from this node.
     * 
     * @return the number of elements from this node.
     */
    protected abstract int getNumberOfElements();

    /**
     * Returns the minimum number of elements that this node must contains.
     * 
     * @return the minimum number of elements that this node must contains.
     */
    protected abstract int getMinimumNumberOfElements();

    /**
     * {@inheritDoc}
     */
    public int getSubTreeSize() {
        return 0;
    }
}
