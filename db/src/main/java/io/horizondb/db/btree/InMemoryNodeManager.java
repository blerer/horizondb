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
import java.util.concurrent.atomic.AtomicReference;

/**
 * <code>NodeManager</code> that keeps all the <code>Node</code>s in memory.
 * 
 * <p>
 * This <code>NodeManager</code> should only be used for testing purpose.
 * </p>
 * 
 * @param <K> the key type.
 * @param <V> the value type.
 * 
 * @author Benjamin
 */
public class InMemoryNodeManager<K extends Comparable<K>, V> implements NodeManager<K, V> {

    /**
     * The root node.
     */
    private AtomicReference<Node<K, V>> root = new AtomicReference<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V> getRoot(BTree<K, V> btree) {

        Node<K, V> node = this.root.get();

        if (node == null) {

            this.root.compareAndSet(null, new LeafNode<>(btree));
            node = this.root.get();
        }

        return node;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setRoot(Node<K, V> root) {

        this.root.set(root);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SafeVarargs
    public final Node<K, V>[] wrapNodes(Node<K, V>... nodes) throws IOException {

        return nodes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V> wrapNode(Node<K, V> node) throws IOException {

        return node;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V> unwrapNode(Node<K, V> node) throws IOException {

        return node;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ValueWrapper<V> wrapValue(V value) {

        return new DefaultValueWrapper<V>(value);
    }
}
