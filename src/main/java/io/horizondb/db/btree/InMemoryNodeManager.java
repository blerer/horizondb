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

import com.codahale.metrics.MetricRegistry;

import static org.apache.commons.lang.Validate.notNull;

/**
 * <code>NodeManager</code> that keeps all the <code>Node</code>s in memory.
 * 
 * <p>
 * This <code>NodeManager</code> provides no durability and should only be used for 
 * testing purpose.
 * </p>
 * 
 * @param <K> the key type.
 * @param <V> the value type.
 */
public final class InMemoryNodeManager<K extends Comparable<K>, V> implements NodeManager<K, V> {

    /**
     * This manager name.
     */
    private final String name;
    
    /**
     * The root node.
     */
    private final AtomicReference<Node<K, V>> root = new AtomicReference<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * Creates a new <code>InMemoryNodeManager</code> with the spcified name.
     * @param name the node manager name
     */
    public InMemoryNodeManager(String name)
    {
        notNull(name, "the name parameter must not be null.");
        this.name = name;
    }
    
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

    }
}
