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
 * A visitor of B+Tree nodes.
 * 
 * @author Benjamin
 * 
 */
public interface NodeVisitor<K extends Comparable<K>, V> {

    /**
     * Invoked for a node before children or records of the node are visited.
     * 
     * <p>
     * If this method returns {@link NodeVisitResult#CONTINUE CONTINUE}, then children or records of the node. If this
     * method returns {@link NodeVisitResult#SKIP_SUBTREE SKIP_SUBTREE} or {@link NodeVisitResult#SKIP_SIBLINGS
     * SKIP_SIBLINGS} then children or records of the node will not be visited.
     * 
     * @param key the key to which the node is associated
     * @param node the node that will be visited
     * @return the visit result
     * @throws IOException if an I/O problem occurs.
     */
    NodeVisitResult preVisitNode(K key, Node<K, V> node) throws IOException;

    /**
     * Invoked for a node after children or records of the node are visited, and all of their descendants, have been
     * visited. This method is also invoked when iteration of the node completes prematurely (by a {@link #visitRecord}
     * method returning {@link NodeVisitResult#SKIP_SIBLINGS SKIP_SIBLINGS}.
     * 
     * @param key the key to which the node is associated
     * @param node the node that will be visited
     * @return the visit result
     * @throws IOException if an I/O problem occurs.
     */
    NodeVisitResult postVisitNode(K key, Node<K, V> node) throws IOException;

    /**
     * Invoked for a record in a leaf node.
     * 
     * @param key the record key.
     * @param wrapper the value wrapper.
     * @return the visit result
     * @throws IOException if an I/O problem occurs.
     */
    NodeVisitResult visitRecord(K key, ValueWrapper<V> wrapper) throws IOException;

    /**
     * The result type of a {@link NodeVisitor}.
     */
    public static enum NodeVisitResult {

        /**
         * Continue. When returned from a {@link NodeVisitor#preVisitNode(Comparable, Node)} method then the children or
         * records of the node should also be visited.
         */
        CONTINUE,

        /**
         * Terminate.
         */
        TERMINATE,

        /**
         * Continue without visiting the children or records of the node. This result is only meaningful when returned
         * from the {@link {@link NodeVisitor#preVisitNode(Comparable, Node)} method; otherwise this result type is
         * the same as returning {@link #CONTINUE}.
         */
        SKIP_SUBTREE,

        /**
         * Continue without visiting the <em>siblings</em> of this node or record. If returned from the
         * {@link NodeVisitor#preVisitNode(Comparable, Node)} method then children or records of the node are also
         * skipped and the {@link NodeVisitor#postVisitNode(Comparable, Node)} method is not invoked.
         */
        SKIP_SIBLINGS;
    }
}
