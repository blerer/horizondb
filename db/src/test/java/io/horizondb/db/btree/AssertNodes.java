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

import io.horizondb.db.btree.InternalNode;
import io.horizondb.db.btree.LeafNode;
import io.horizondb.db.btree.Node;
import io.horizondb.db.btree.NodeProxy;
import io.horizondb.test.AssertCollections;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Assert;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 * 
 */
public final class AssertNodes {

    /**
     * Assert that the specified node is a leaf node and contains the specified key-value pair.
     * 
     * @param actual the actual node to check.
     * @param key the expected key
     * @param value the expected value
     * @throws IOException if an I/O problem occurs.
     */
    public static <K extends Comparable<K>, V>
            void
            assertLeafNodeContains(Node<K, V> actual, K key, V value) throws IOException {

        assertLeafNodeContains(actual, singletonMap(key, value));
    }

    /**
     * Assert that the specified node is a leaf node and contains the specified key-value pair.
     * 
     * @param actual the actual node to check.
     * @param key1 the first key
     * @param value1 the value associated to the first key
     * @param key2 the second key
     * @param value2 the value associated to the second key
     * @throws IOException if an I/O problem occurs.
     */
    public static <K extends Comparable<K>, V> void assertLeafNodeContains(Node<K, V> actual,
                                                                           K key1,
                                                                           V value1,
                                                                           K key2,
                                                                           V value2) throws IOException {

        assertLeafNodeContains(actual, newMap(key1, value1, key2, value2));
    }

    /**
     * Assert that the specified node is a leaf node and contains the specified key-value pair.
     * 
     * @param actual the actual node to check.
     * @param key1 the first key
     * @param value1 the value associated to the first key
     * @param key2 the second key
     * @param value2 the value associated to the second key
     * @param key3 the third key
     * @param value3 the value associated to the third key
     * @throws IOException if an I/O problem occurs.
     */
    public static <K extends Comparable<K>, V> void assertLeafNodeContains(Node<K, V> actual,
                                                                           K key1,
                                                                           V value1,
                                                                           K key2,
                                                                           V value2,
                                                                           K key3,
                                                                           V value3) throws IOException {

        assertLeafNodeContains(actual, newMap(key1, value1, key2, value2, key3, value3));
    }

    /**
     * Assert that the specified node is a leaf node and contains the specified key-value pair.
     * 
     * @param actual the actual node to check.
     * @param key1 the first key
     * @param value1 the value associated to the first key
     * @param key2 the second key
     * @param value2 the value associated to the second key
     * @param key3 the third key
     * @param value3 the value associated to the third key
     * @param key4 the fourth key
     * @param value4 the value associated to the fourth key
     * @throws IOException if an I/O problem occurs.
     */
    public static <K extends Comparable<K>, V> void assertLeafNodeContains(Node<K, V> actual,
                                                                           K key1,
                                                                           V value1,
                                                                           K key2,
                                                                           V value2,
                                                                           K key3,
                                                                           V value3,
                                                                           K key4,
                                                                           V value4) throws IOException {

        assertLeafNodeContains(actual, newMap(key1, value1, key2, value2, key3, value3, key4, value4));
    }

    /**
     * Assert that the specified node is an empty leaf node.
     * 
     * @param actual the actual node to check.
     * @throws IOException if an I/O problem occurs.
     */
    public static <K extends Comparable<K>, V> void assertLeafNodeEmpty(Node<K, V> actual) throws IOException {

        assertLeafNodeContains(actual, Collections.<K, V> emptyMap());
    }

    /**
     * Assert that the specified node is a leaf node and contains the specified key-value pairs.
     * 
     * @param actual the actual node to check.
     * @param expected the expected key-value pairs.
     * @throws IOException if an I/O problem occurs.
     */
    private static <K extends Comparable<K>, V>
            void
            assertLeafNodeContains(Node<K, V> actual, Map<K, V> expected) throws IOException {

        Node<K, V> actualNode = actual;

        if (actual instanceof NodeProxy) {

            actualNode = ((NodeProxy<K, V>) actualNode).loadNode();
        }

        assertTrue("The node must be a leaf node but is: " + actualNode.getClass(), actualNode instanceof LeafNode);

        LeafNode<K, V> node = (LeafNode<K, V>) actualNode;

        assertEquals(expected, node.toMap());
    }

    /**
     * Assert that the specified node is an internal node and contains the specified keys.
     * 
     * @param actual the actual node to check.
     * @param keys the expected keys.
     * @throws IOException if an I/O problem occurs.
     */
    @SafeVarargs
    public final static <K extends Comparable<K>, V>
            void
            assertInternalNode(Node<K, V> actual, K... keys) throws IOException {

        Node<K, V> actualNode = actual;

        if (actual instanceof NodeProxy) {

            actualNode = ((NodeProxy<K, V>) actualNode).loadNode();
        }

        Assert.assertTrue("The node must be an internal node.", actualNode instanceof InternalNode);

        InternalNode<K, V> node = (InternalNode<K, V>) actualNode;

        AssertCollections.assertIterableContains(node.getKeys(), keys);
    }

    private AssertNodes() {
    }

    /**
     * Creates a new <code>LinkedHashMap</code> with the specified keys and values.
     * 
     * @param key1 the first key
     * @param value1 the value associated to the first key
     * @param key2 the second key
     * @param value2 the value associated to the second key
     * @return a new <code>LinkedHashMap</code> with the specified keys and values.
     */
    private static <K, V> Map<K, V> newMap(K key1, V value1, K key2, V value2) {

        LinkedHashMap<K, V> map = new LinkedHashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);

        return map;
    }

    /**
     * Creates a new <code>LinkedHashMap</code> with the specified keys and values.
     * 
     * @param key1 the first key
     * @param value1 the value associated to the first key
     * @param key2 the second key
     * @param value2 the value associated to the second key
     * @param key3 the third key
     * @param value3 the value associated to the third key
     * @return a new <code>LinkedHashMap</code> with the specified keys and values.
     */
    private static <K, V> Map<K, V> newMap(K key1, V value1, K key2, V value2, K key3, V value3) {

        Map<K, V> map = newMap(key1, value1, key2, value2);
        map.put(key3, value3);

        return map;
    }

    /**
     * Creates a new <code>LinkedHashMap</code> with the specified keys and values.
     * 
     * @param key1 the first key
     * @param value1 the value associated to the first key
     * @param key2 the second key
     * @param value2 the value associated to the second key
     * @param key3 the third key
     * @param value3 the value associated to the third key
     * @param key4 the fourth key
     * @param value4 the value associated to the fourth key
     * @return a new <code>LinkedHashMap</code> with the specified keys and values.
     */
    private static <K, V> Map<K, V> newMap(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4) {

        Map<K, V> map = newMap(key1, value1, key2, value2, key3, value3);
        map.put(key4, value4);

        return map;
    }

}
