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
 * <code>NodeVisitor</code> that simply visit the nodes and records without doing anything.
 * 
 * @author Benjamin
 * 
 */
public class SimpleNodeVisitor<K extends Comparable<K>, V> implements NodeVisitor<K, V> {

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeVisitResult preVisitNode(K key, Node<K, V> node) throws IOException {
        return NodeVisitResult.CONTINUE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeVisitResult postVisitNode(K key, Node<K, V> node) throws IOException {
        return NodeVisitResult.CONTINUE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeVisitResult visitRecord(K key, ValueWrapper<V> pointer) throws IOException {
        return NodeVisitResult.CONTINUE;
    }
}
