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

import io.horizondb.test.AssertCollections;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.horizondb.db.btree.AssertNodes.assertInternalNode;
import static io.horizondb.db.btree.AssertNodes.assertLeafNodeContains;
import static io.horizondb.db.btree.AssertNodes.assertLeafNodeEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BTreeTest {

    /**
     * The manager used during the tests.
     */
    private NodeManager<Integer, String> manager;

    @Before
    public void setUp() {

        this.manager = new InMemoryNodeManager<Integer, String>();
    }

    @After
    public void tearDown() {

        this.manager = null;
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionWithOnlyARootNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");

        assertLeafNodeContains(btree.getRoot(), 2, "B");

        btree.insert(4, "D");

        assertLeafNodeContains(btree.getRoot(), 2, "B", 4, "D");

        btree.insert(3, "C");

        assertLeafNodeContains(btree.getRoot(), 2, "B", 3, "C", 4, "D");

        btree.insert(1, "A");

        assertLeafNodeContains(btree.getRoot(), 1, "A", 2, "B", 3, "C", 4, "D");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertingExistingKeyWithdOnlyARootNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(3, "D");
        btree.insert(1, "A");

        assertLeafNodeContains(btree.getRoot(), 1, "A", 2, "B", 3, "D");

        btree.insert(3, "C");

        assertLeafNodeContains(btree.getRoot(), 1, "A", 2, "B", 3, "C");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testGetWithOnlyARootNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(1, "A");

        assertEquals("C", btree.get(3));
        assertEquals("B", btree.get(2));
        assertEquals("A", btree.get(1));
        assertNull(btree.get(5));
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testContainsWithOnlyARootNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(1, "A");

        assertTrue(btree.contains(3));
        assertTrue(btree.contains(2));
        assertTrue(btree.contains(1));
        assertFalse(btree.contains(10));
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionWithOnlyARootNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(1, "A");

        btree.delete(1);

        assertLeafNodeContains(btree.getRoot(), 2, "B", 3, "C", 4, "D");

        btree.delete(3);

        assertLeafNodeContains(btree.getRoot(), 2, "B", 4, "D");

        btree.delete(2);

        assertLeafNodeContains(btree.getRoot(), 4, "D");

        btree.delete(2);

        assertLeafNodeContains(btree.getRoot(), 4, "D");

        btree.delete(4);

        assertLeafNodeEmpty(btree.getRoot());
    }
    
    @Test
    @SuppressWarnings({ "boxing" })
    public void testIteratorWithOnlyARootNodeAndFullScan() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(1, "A");

        KeyValueIterator<Integer, String> iterator = btree.iterator(0, 5);
        
        assertNextContains(iterator, 1, "A");
        assertNextContains(iterator, 2, "B");
        assertNextContains(iterator, 3, "C");
        assertNextContains(iterator, 4, "D");
        assertFalse(iterator.next());
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testIteratorWithOnlyARootNodeAndPartialScan() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(1, "A");

        KeyValueIterator<Integer, String> iterator = btree.iterator(2, 3);
        
        assertNextContains(iterator, 2, "B");
        assertNextContains(iterator, 3, "C");
        assertFalse(iterator.next());
    }
    
    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionWithRootNodeSplit() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(1, "A");
        btree.insert(6, "E");

        assertInternalNode(btree.getRoot(), 1, 3);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
        assertLeafNodeContains(internalNode.getChild(1), 3, "C", 4, "D", 6, "E");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertingExistingKeyWithInternalNodes() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(1, "A");
        btree.insert(6, "F");

        btree.insert(6, "E");

        assertInternalNode(btree.getRoot(), 1, 3);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
        assertLeafNodeContains(internalNode.getChild(1), 3, "C", 4, "D", 6, "E");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testIteratorWithInternalNodesAndPartialScanOverTwoNodes() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(1, "A");
        btree.insert(6, "F");

        KeyValueIterator<Integer, String> iterator = btree.iterator(2, 3);
        
        assertNextContains(iterator, 2, "B");
        assertNextContains(iterator, 3, "C");
        assertFalse(iterator.next());
    }
    
    @Test
    @SuppressWarnings({ "boxing" })
    public void testIteratorWithInternalNodesAndPartialScanOverOneNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(1, "A");
        btree.insert(6, "F");

        KeyValueIterator<Integer, String> iterator = btree.iterator(1, 1);
        
        assertNextContains(iterator, 1, "A");
        assertFalse(iterator.next());
    }
        
    @Test
    @SuppressWarnings({ "boxing" })
    public void testIteratorWithInternalNodesAndFullScan() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(1, "A");
        btree.insert(6, "F");

        KeyValueIterator<Integer, String> iterator = btree.iterator(0, 10);
        
        assertNextContains(iterator, 1, "A");
        assertNextContains(iterator, 2, "B");
        assertNextContains(iterator, 3, "C");
        assertNextContains(iterator, 4, "D");
        assertNextContains(iterator, 6, "F");
        assertFalse(iterator.next());
    }
    
    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionWithInternalNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(1, "A");
        btree.insert(6, "E");

        btree.delete(4);

        assertInternalNode(btree.getRoot(), 1, 3);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
        assertLeafNodeContains(internalNode.getChild(1), 3, "C", 6, "E");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfFirstKeyOfLeafNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(1, "A");
        btree.insert(6, "E");

        btree.delete(3);

        assertInternalNode(btree.getRoot(), 1, 4);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
        assertLeafNodeContains(internalNode.getChild(1), 4, "D", 6, "E");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionWithRootNodeSplitAndBranchingFactor4() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 4);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(1, "A");

        assertInternalNode(btree.getRoot(), 1, 3);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
        assertLeafNodeContains(internalNode.getChild(1), 3, "C", 4, "D");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionOnLeftLeafNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(6, "F");
        btree.insert(5, "E");

        btree.insert(1, "A");

        assertInternalNode(btree.getRoot(), 1, 4);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C");
        assertLeafNodeContains(internalNode.getChild(1), 4, "D", 5, "E", 6, "F");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionOnRightLeafNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(6, "F");
        btree.insert(5, "E");

        btree.insert(7, "G");

        assertInternalNode(btree.getRoot(), 2, 4);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 2, "B", 3, "C");
        assertLeafNodeContains(internalNode.getChild(1), 4, "D", 5, "E", 6, "F", 7, "G");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testGetWithOneInternalNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(6, "F");
        btree.insert(5, "E");

        assertEquals("B", btree.get(2));
        assertEquals("C", btree.get(3));
        assertEquals("E", btree.get(5));
        assertEquals("F", btree.get(6));
        assertNull(btree.get(10));
        assertNull(btree.get(1));
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testContainsWithOneInternalNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(6, "F");
        btree.insert(5, "E");

        assertTrue(btree.contains(2));
        assertTrue(btree.contains(3));
        assertTrue(btree.contains(5));
        assertTrue(btree.contains(6));
        assertFalse(btree.contains(10));
        assertFalse(btree.contains(1));
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionOnRightLeafNodeWithSplit() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(3, "C");
        btree.insert(6, "F");
        btree.insert(5, "E");

        btree.insert(7, "G");
        btree.insert(9, "I");

        assertInternalNode(btree.getRoot(), 2, 4, 6);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 2, "B", 3, "C");
        assertLeafNodeContains(internalNode.getChild(1), 4, "D", 5, "E");
        assertLeafNodeContains(internalNode.getChild(2), 6, "F", 7, "G", 9, "I");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionOnLeftLeafNodeWithSplit() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(2, "B");
        btree.insert(3, "C");
        btree.insert(6, "F");
        btree.insert(7, "G");
        btree.insert(9, "I");

        btree.insert(1, "A");
        btree.insert(5, "E");
        btree.insert(4, "D");

        assertInternalNode(btree.getRoot(), 1, 3, 6);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
        assertLeafNodeContains(internalNode.getChild(1), 3, "C", 4, "D", 5, "E");
        assertLeafNodeContains(internalNode.getChild(2), 6, "F", 7, "G", 9, "I");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionOnLeafNodeWithLeafAndParentFull() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(16, "P");

        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(17, "Q");

        btree.insert(18, "R");

        assertInternalNode(btree.getRoot(), 1, 10);

        InternalNode<Integer, String> root = (InternalNode<Integer, String>) btree.getRoot();

        assertInternalNode(root.getChild(0), 1, 5, 8);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) root.getChild(0);

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
        assertLeafNodeContains(internalNode.getChild(1), 5, "E", 6, "F", 7, "G");
        assertLeafNodeContains(internalNode.getChild(2), 8, "H", 9, "I");

        assertInternalNode(root.getChild(1), 10, 14, 16);

        internalNode = (InternalNode<Integer, String>) root.getChild(1);

        assertLeafNodeContains(internalNode.getChild(0), 10, "J", 11, "K", 12, "L");
        assertLeafNodeContains(internalNode.getChild(1), 14, "N", 15, "O");
        assertLeafNodeContains(internalNode.getChild(2), 16, "P", 17, "Q", 18, "R");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertWith3LevelDepth() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 3);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(16, "P");

        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(17, "Q");

        btree.insert(18, "R");
        btree.insert(13, "M");

        assertInternalNode(btree.getRoot(), 1, 8, 14);

        InternalNode<Integer, String> root = (InternalNode<Integer, String>) btree.getRoot();

        InternalNode<Integer, String> internalNodeDepth1 = (InternalNode<Integer, String>) root.getChild(0);

        assertInternalNode(internalNodeDepth1, 1, 5);

        InternalNode<Integer, String> internalNodeDepth2 = (InternalNode<Integer, String>) internalNodeDepth1.getChild(0);

        assertInternalNode(internalNodeDepth2, 1, 2, 3);

        assertLeafNodeContains(internalNodeDepth2.getChild(0), 1, "A");
        assertLeafNodeContains(internalNodeDepth2.getChild(1), 2, "B");
        assertLeafNodeContains(internalNodeDepth2.getChild(2), 3, "C", 4, "D");

        internalNodeDepth2 = (InternalNode<Integer, String>) internalNodeDepth1.getChild(1);

        assertInternalNode(internalNodeDepth2, 5, 6);

        assertLeafNodeContains(internalNodeDepth2.getChild(0), 5, "E");
        assertLeafNodeContains(internalNodeDepth2.getChild(1), 6, "F", 7, "G");

        internalNodeDepth1 = (InternalNode<Integer, String>) root.getChild(1);

        assertInternalNode(internalNodeDepth1, 8, 10);

        internalNodeDepth2 = (InternalNode<Integer, String>) internalNodeDepth1.getChild(0);

        assertInternalNode(internalNodeDepth2, 8, 9);

        assertLeafNodeContains(internalNodeDepth2.getChild(0), 8, "H");
        assertLeafNodeContains(internalNodeDepth2.getChild(1), 9, "I");

        internalNodeDepth2 = (InternalNode<Integer, String>) internalNodeDepth1.getChild(1);

        assertInternalNode(internalNodeDepth2, 10, 11, 12);

        assertLeafNodeContains(internalNodeDepth2.getChild(0), 10, "J");
        assertLeafNodeContains(internalNodeDepth2.getChild(1), 11, "K");
        assertLeafNodeContains(internalNodeDepth2.getChild(2), 12, "L", 13, "M");

        internalNodeDepth1 = (InternalNode<Integer, String>) root.getChild(2);

        assertInternalNode(internalNodeDepth1, 14, 16);

        internalNodeDepth2 = (InternalNode<Integer, String>) internalNodeDepth1.getChild(0);

        assertInternalNode(internalNodeDepth2, 14, 15);

        assertLeafNodeContains(internalNodeDepth2.getChild(0), 14, "N");
        assertLeafNodeContains(internalNodeDepth2.getChild(1), 15, "O");

        internalNodeDepth2 = (InternalNode<Integer, String>) internalNodeDepth1.getChild(1);

        assertInternalNode(internalNodeDepth2, 16, 17);

        assertLeafNodeContains(internalNodeDepth2.getChild(0), 16, "P");
        assertLeafNodeContains(internalNodeDepth2.getChild(1), 17, "Q", 18, "R");
    }
    
    @Test
    @SuppressWarnings({ "boxing" })
    public void testIteratorWith3LevelDepthAndFullScan() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 3);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(16, "P");

        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(17, "Q");

        btree.insert(18, "R");
        btree.insert(13, "M");

        KeyValueIterator<Integer, String> iterator = btree.iterator(0, 100);
        
        assertNextContains(iterator, 1, "A");
        assertNextContains(iterator, 2, "B");
        assertNextContains(iterator, 3, "C");
        assertNextContains(iterator, 4, "D");
        assertNextContains(iterator, 5, "E");
        assertNextContains(iterator, 6, "F");
        assertNextContains(iterator, 7, "G");
        assertNextContains(iterator, 8, "H");
        assertNextContains(iterator, 9, "I");
        assertNextContains(iterator, 10, "J");
        assertNextContains(iterator, 11, "K");
        assertNextContains(iterator, 12, "L");
        assertNextContains(iterator, 13, "M");
        assertNextContains(iterator, 14, "N");
        assertNextContains(iterator, 15, "O");
        assertNextContains(iterator, 16, "P");
        assertNextContains(iterator, 17, "Q");
        assertNextContains(iterator, 18, "R");
        assertFalse(iterator.next());
    }
    
    @Test
    @SuppressWarnings({ "boxing" })
    public void testIteratorWith3LevelDepthAndPartialScan() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 3);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(16, "P");

        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(17, "Q");

        btree.insert(18, "R");
        btree.insert(13, "M");

        KeyValueIterator<Integer, String> iterator = btree.iterator(13, 14);
        
        assertNextContains(iterator, 13, "M");
        assertNextContains(iterator, 14, "N");
        assertFalse(iterator.next());
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testGetWith3LevelDepth() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 3);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(16, "P");

        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(17, "Q");

        btree.insert(18, "R");
        btree.insert(13, "M");

        assertEquals("M", btree.get(13));
        assertEquals("E", btree.get(5));
        assertNull(btree.get(100));
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testContainsWith3LevelDepth() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 3);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(16, "P");

        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(17, "Q");

        btree.insert(18, "R");
        btree.insert(13, "M");

        assertTrue(btree.contains(1));
        assertTrue(btree.contains(18));
        assertFalse(btree.contains(19));
        assertFalse(btree.contains(21));
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionOnLeafNodeWithLeafFullAndParentNot() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(13, "M");
        btree.insert(14, "N");
        btree.insert(20, "T");
        btree.insert(22, "V");
        btree.insert(23, "W");
        btree.insert(6, "F");
        btree.insert(8, "H");
        btree.insert(7, "G");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(15, "O");
        btree.insert(17, "Q");
        btree.insert(18, "R");
        btree.insert(24, "X");
        btree.insert(25, "Y");
        btree.insert(5, "E");
        btree.insert(9, "I");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");

        assertInternalNode(btree.getRoot(), 1, 8, 15);

        InternalNode<Integer, String> root = (InternalNode<Integer, String>) btree.getRoot();

        assertInternalNode(root.getChild(0), 1, 3, 6);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) root.getChild(0);

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
        assertLeafNodeContains(internalNode.getChild(1), 3, "C", 4, "D", 5, "E");
        assertLeafNodeContains(internalNode.getChild(2), 6, "F", 7, "G");

        assertInternalNode(root.getChild(1), 8, 10, 13);

        internalNode = (InternalNode<Integer, String>) root.getChild(1);

        assertLeafNodeContains(internalNode.getChild(0), 8, "H", 9, "I");
        assertLeafNodeContains(internalNode.getChild(1), 10, "J", 11, "K", 12, "L");
        assertLeafNodeContains(internalNode.getChild(2), 13, "M", 14, "N");

        assertInternalNode(root.getChild(2), 15, 20, 23);

        internalNode = (InternalNode<Integer, String>) root.getChild(2);

        assertLeafNodeContains(internalNode.getChild(0), 15, "O", 17, "Q", 18, "R");
        assertLeafNodeContains(internalNode.getChild(1), 20, "T", 22, "V");
        assertLeafNodeContains(internalNode.getChild(2), 23, "W", 24, "X", 25, "Y");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertWithInternalNodeSplitWithParentNotFull() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(13, "M");
        btree.insert(14, "N");
        btree.insert(20, "T");
        btree.insert(22, "V");
        btree.insert(23, "W");

        btree.insert(6, "F");
        btree.insert(8, "H");
        btree.insert(7, "G");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(15, "O");
        btree.insert(17, "Q");
        btree.insert(18, "R");
        btree.insert(24, "X");
        btree.insert(25, "Y");
        btree.insert(18, "S");

        assertInternalNode(btree.getRoot(), 1, 15);

        InternalNode<Integer, String> root = (InternalNode<Integer, String>) btree.getRoot();

        assertInternalNode(root.getChild(0), 1, 6, 13);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) root.getChild(0);

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
        assertLeafNodeContains(internalNode.getChild(1), 6, "F", 7, "G", 8, "H");
        assertLeafNodeContains(internalNode.getChild(2), 13, "M", 14, "N");

        assertInternalNode(root.getChild(1), 15, 20, 23);

        internalNode = (InternalNode<Integer, String>) root.getChild(1);

        assertLeafNodeContains(internalNode.getChild(0), 15, "O", 17, "Q", 18, "R", 18, "S");
        assertLeafNodeContains(internalNode.getChild(1), 20, "T", 22, "V");
        assertLeafNodeContains(internalNode.getChild(2), 23, "W", 24, "X", 25, "Y");

        btree.insert(26, "Z");
        btree.insert(27, "AA");
        btree.insert(28, "BB");
        btree.insert(29, "CC");
        btree.insert(30, "DD");
        btree.insert(31, "EE");

        assertInternalNode(btree.getRoot(), 1, 15, 25);

        root = (InternalNode<Integer, String>) btree.getRoot();

        assertInternalNode(root.getChild(0), 1, 6, 13);

        internalNode = (InternalNode<Integer, String>) root.getChild(0);

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
        assertLeafNodeContains(internalNode.getChild(1), 6, "F", 7, "G", 8, "H");
        assertLeafNodeContains(internalNode.getChild(2), 13, "M", 14, "N");

        assertInternalNode(root.getChild(1), 15, 20, 23);

        internalNode = (InternalNode<Integer, String>) root.getChild(1);

        assertLeafNodeContains(internalNode.getChild(0), 15, "O", 17, "Q", 18, "R", 18, "S");
        assertLeafNodeContains(internalNode.getChild(1), 20, "T", 22, "V");
        assertLeafNodeContains(internalNode.getChild(2), 23, "W", 24, "X");

        assertInternalNode(root.getChild(2), 25, 27, 29);

        internalNode = (InternalNode<Integer, String>) root.getChild(2);

        assertLeafNodeContains(internalNode.getChild(0), 25, "Y", 26, "Z");
        assertLeafNodeContains(internalNode.getChild(1), 27, "AA", 28, "BB");
        assertLeafNodeContains(internalNode.getChild(2), 29, "CC", 30, "DD", 31, "EE");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletingKeysWhichAreWithinTheRoot() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(13, "M");
        btree.insert(14, "N");
        btree.insert(20, "T");
        btree.insert(22, "V");
        btree.insert(23, "W");

        btree.insert(6, "F");
        btree.insert(8, "H");
        btree.insert(7, "G");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(15, "O");
        btree.insert(17, "Q");
        btree.insert(18, "R");
        btree.insert(24, "X");
        btree.insert(25, "Y");
        btree.insert(18, "S");

        assertInternalNode(btree.getRoot(), 1, 15);

        InternalNode<Integer, String> root = (InternalNode<Integer, String>) btree.getRoot();

        assertInternalNode(root.getChild(0), 1, 6, 13);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) root.getChild(0);

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
        assertLeafNodeContains(internalNode.getChild(1), 6, "F", 7, "G", 8, "H");
        assertLeafNodeContains(internalNode.getChild(2), 13, "M", 14, "N");

        assertInternalNode(root.getChild(1), 15, 20, 23);

        internalNode = (InternalNode<Integer, String>) root.getChild(1);

        assertLeafNodeContains(internalNode.getChild(0), 15, "O", 17, "Q", 18, "R", 18, "S");
        assertLeafNodeContains(internalNode.getChild(1), 20, "T", 22, "V");
        assertLeafNodeContains(internalNode.getChild(2), 23, "W", 24, "X", 25, "Y");

        btree.delete(15);
        btree.delete(1);

        assertInternalNode(btree.getRoot(), 2, 17);

        root = (InternalNode<Integer, String>) btree.getRoot();

        assertInternalNode(root.getChild(0), 2, 6, 13);

        internalNode = (InternalNode<Integer, String>) root.getChild(0);

        assertLeafNodeContains(internalNode.getChild(0), 2, "B", 3, "C", 4, "D");
        assertLeafNodeContains(internalNode.getChild(1), 6, "F", 7, "G", 8, "H");
        assertLeafNodeContains(internalNode.getChild(2), 13, "M", 14, "N");

        assertInternalNode(root.getChild(1), 17, 20, 23);

        internalNode = (InternalNode<Integer, String>) root.getChild(1);

        assertLeafNodeContains(internalNode.getChild(0), 17, "Q", 18, "R", 18, "S");
        assertLeafNodeContains(internalNode.getChild(1), 20, "T", 22, "V");
        assertLeafNodeContains(internalNode.getChild(2), 23, "W", 24, "X", 25, "Y");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionWithInternalNodeRebalancingFromTheRightNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(16, "P");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(20, "T");
        btree.insert(21, "U");
        btree.insert(13, "M");
        btree.insert(23, "W");
        btree.insert(24, "X");
        btree.insert(25, "Y");
        btree.insert(26, "Z");
        btree.insert(27, "AA");
        btree.insert(28, "BB");
        btree.insert(17, "Q");
        btree.insert(18, "R");
        btree.insert(19, "S");

        assertInternalNode(btree.getRoot(), 1, 10, 21);

        InternalNode<Integer, String> root = (InternalNode<Integer, String>) btree.getRoot();

        assertInternalNode(root.getChild(0), 1, 5, 8);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) root.getChild(0);

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
        assertLeafNodeContains(internalNode.getChild(1), 5, "E", 6, "F", 7, "G");
        assertLeafNodeContains(internalNode.getChild(2), 8, "H", 9, "I");

        assertInternalNode(root.getChild(1), 10, 14, 16, 18);

        internalNode = (InternalNode<Integer, String>) root.getChild(1);

        assertLeafNodeContains(internalNode.getChild(0), 10, "J", 11, "K", 12, "L", 13, "M");
        assertLeafNodeContains(internalNode.getChild(1), 14, "N", 15, "O");
        assertLeafNodeContains(internalNode.getChild(2), 16, "P", 17, "Q");
        assertLeafNodeContains(internalNode.getChild(3), 18, "R", 19, "S", 20, "T");

        assertInternalNode(root.getChild(2), 21, 24, 26);

        internalNode = (InternalNode<Integer, String>) root.getChild(2);

        assertLeafNodeContains(internalNode.getChild(0), 21, "U", 23, "W");
        assertLeafNodeContains(internalNode.getChild(1), 24, "X", 25, "Y");
        assertLeafNodeContains(internalNode.getChild(2), 26, "Z", 27, "AA", 28, "BB");

        btree.delete(25);
        btree.delete(4);
        btree.delete(2);
        btree.delete(7);
        btree.delete(5);

        assertInternalNode(btree.getRoot(), 1, 14, 21);

        root = (InternalNode<Integer, String>) btree.getRoot();

        assertInternalNode(root.getChild(0), 1, 6, 10);

        internalNode = (InternalNode<Integer, String>) root.getChild(0);

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 3, "C");
        assertLeafNodeContains(internalNode.getChild(1), 6, "F", 8, "H", 9, "I");
        assertLeafNodeContains(internalNode.getChild(2), 10, "J", 11, "K", 12, "L", 13, "M");

        assertInternalNode(root.getChild(1), 14, 16, 18);

        internalNode = (InternalNode<Integer, String>) root.getChild(1);

        assertLeafNodeContains(internalNode.getChild(0), 14, "N", 15, "O");
        assertLeafNodeContains(internalNode.getChild(1), 16, "P", 17, "Q");
        assertLeafNodeContains(internalNode.getChild(2), 18, "R", 19, "S", 20, "T");

        assertInternalNode(root.getChild(2), 21, 24, 27);

        internalNode = (InternalNode<Integer, String>) root.getChild(2);

        assertLeafNodeContains(internalNode.getChild(0), 21, "U", 23, "W");
        assertLeafNodeContains(internalNode.getChild(1), 24, "X", 26, "Z");
        assertLeafNodeContains(internalNode.getChild(2), 27, "AA", 28, "BB");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeleteWithInternalNodeRebalancingFromTheLeftNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(16, "P");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(20, "T");
        btree.insert(21, "U");
        btree.insert(13, "M");
        btree.insert(23, "W");
        btree.insert(24, "X");
        btree.insert(25, "Y");
        btree.insert(26, "Z");
        btree.insert(27, "AA");
        btree.insert(28, "BB");
        btree.insert(17, "Q");
        btree.insert(18, "R");
        btree.insert(19, "S");
        btree.insert(22, "V");

        assertInternalNode(btree.getRoot(), 1, 10, 21);

        InternalNode<Integer, String> root = (InternalNode<Integer, String>) btree.getRoot();

        assertInternalNode(root.getChild(0), 1, 5, 8);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) root.getChild(0);

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
        assertLeafNodeContains(internalNode.getChild(1), 5, "E", 6, "F", 7, "G");
        assertLeafNodeContains(internalNode.getChild(2), 8, "H", 9, "I");

        assertInternalNode(root.getChild(1), 10, 14, 16, 18);

        internalNode = (InternalNode<Integer, String>) root.getChild(1);

        assertLeafNodeContains(internalNode.getChild(0), 10, "J", 11, "K", 12, "L", 13, "M");
        assertLeafNodeContains(internalNode.getChild(1), 14, "N", 15, "O");
        assertLeafNodeContains(internalNode.getChild(2), 16, "P", 17, "Q");
        assertLeafNodeContains(internalNode.getChild(3), 18, "R", 19, "S", 20, "T");

        assertInternalNode(root.getChild(2), 21, 24, 26);

        internalNode = (InternalNode<Integer, String>) root.getChild(2);

        assertLeafNodeContains(internalNode.getChild(0), 21, "U", 22, "V", 23, "W");
        assertLeafNodeContains(internalNode.getChild(1), 24, "X", 25, "Y");
        assertLeafNodeContains(internalNode.getChild(2), 26, "Z", 27, "AA", 28, "BB");

        btree.delete(27);
        btree.delete(24);
        btree.delete(22);

        assertInternalNode(btree.getRoot(), 1, 10, 18);

        root = (InternalNode<Integer, String>) btree.getRoot();

        assertInternalNode(root.getChild(0), 1, 5, 8);

        internalNode = (InternalNode<Integer, String>) root.getChild(0);

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
        assertLeafNodeContains(internalNode.getChild(1), 5, "E", 6, "F", 7, "G");
        assertLeafNodeContains(internalNode.getChild(2), 8, "H", 9, "I");

        assertInternalNode(root.getChild(1), 10, 14, 16);

        internalNode = (InternalNode<Integer, String>) root.getChild(1);

        assertLeafNodeContains(internalNode.getChild(0), 10, "J", 11, "K", 12, "L", 13, "M");
        assertLeafNodeContains(internalNode.getChild(1), 14, "N", 15, "O");
        assertLeafNodeContains(internalNode.getChild(2), 16, "P", 17, "Q");

        assertInternalNode(root.getChild(2), 18, 21, 26);

        internalNode = (InternalNode<Integer, String>) root.getChild(2);

        assertLeafNodeContains(internalNode.getChild(0), 18, "R", 19, "S", 20, "T");
        assertLeafNodeContains(internalNode.getChild(1), 21, "U", 23, "W", 25, "Y");
        assertLeafNodeContains(internalNode.getChild(2), 26, "Z", 28, "BB");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyFromLeafNodeWithRedistributionOnRightNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(3, "C");
        btree.insert(6, "F");
        btree.insert(10, "J");

        btree.delete(1);
        btree.delete(2);

        assertInternalNode(btree.getRoot(), 3, 5, 7);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 3, "C", 4, "D");
        assertLeafNodeContains(internalNode.getChild(1), 5, "E", 6, "F");
        assertLeafNodeContains(internalNode.getChild(2), 7, "G", 8, "H", 9, "I", 10, "J");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyFromLeafNodeWithFirstKeyDeletedAndRedistribution() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(3, "C");
        btree.insert(6, "F");
        btree.insert(10, "J");

        btree.delete(4);
        btree.delete(5);

        assertInternalNode(btree.getRoot(), 1, 6, 8);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C");
        assertLeafNodeContains(internalNode.getChild(1), 6, "F", 7, "G");
        assertLeafNodeContains(internalNode.getChild(2), 8, "H", 9, "I", 10, "J");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyFromLeafNodeWithRedistributionOnLeftNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(3, "C");
        btree.insert(6, "F");
        btree.insert(10, "J");

        btree.delete(8);
        btree.delete(9);

        assertInternalNode(btree.getRoot(), 1, 4, 7);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C");
        assertLeafNodeContains(internalNode.getChild(1), 4, "D", 5, "E", 6, "F");
        assertLeafNodeContains(internalNode.getChild(2), 7, "G", 10, "J");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyFromLeafNodeWithMergeOnTheRight() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(3, "C");
        btree.insert(6, "F");

        btree.delete(3);
        btree.delete(8);
        btree.delete(5);
        btree.delete(1);

        assertInternalNode(btree.getRoot(), 2, 7);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 2, "B", 4, "D", 6, "F");
        assertLeafNodeContains(internalNode.getChild(1), 7, "G", 9, "I");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyFromLeafNodeWithMergeOnTheLeft() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(3, "C");
        btree.insert(6, "F");

        btree.delete(3);
        btree.delete(8);
        btree.delete(5);
        btree.delete(4);

        assertInternalNode(btree.getRoot(), 1, 6);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
        assertLeafNodeContains(internalNode.getChild(1), 6, "F", 7, "G", 9, "I");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyFromLeafNodeWithMergeAndNoRightNode() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(4, "D");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(3, "C");
        btree.insert(6, "F");

        btree.delete(4);
        btree.delete(8);
        btree.delete(5);
        btree.delete(7);

        assertInternalNode(btree.getRoot(), 1, 3);

        InternalNode<Integer, String> internalNode = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
        assertLeafNodeContains(internalNode.getChild(1), 3, "C", 6, "F", 9, "I");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionWithLeafNodeMergeAndInternalNodeMerge() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(13, "M");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(16, "P");
        btree.insert(17, "Q");

        btree.delete(12);
        btree.delete(5);
        btree.delete(10);

        assertInternalNode(btree.getRoot(), 1, 6, 8, 11, 15);

        InternalNode<Integer, String> root = (InternalNode<Integer, String>) btree.getRoot();

        assertLeafNodeContains(root.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
        assertLeafNodeContains(root.getChild(1), 6, "F", 7, "G");
        assertLeafNodeContains(root.getChild(2), 8, "H", 9, "I");
        assertLeafNodeContains(root.getChild(3), 11, "K", 13, "M", 14, "N");
        assertLeafNodeContains(root.getChild(4), 15, "O", 16, "P", 17, "Q");
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testAccept() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(13, "M");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(16, "P");
        btree.insert(17, "Q");

        KeyCollector collector = new KeyCollector();

        btree.accept(collector);

        AssertCollections.assertListContains(collector.getKeys(),
                                             1,
                                             1,
                                             1,
                                             1,
                                             2,
                                             3,
                                             4,
                                             5,
                                             5,
                                             6,
                                             7,
                                             8,
                                             8,
                                             9,
                                             10,
                                             10,
                                             10,
                                             11,
                                             12,
                                             13,
                                             13,
                                             14,
                                             15,
                                             15,
                                             16,
                                             17);
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testAcceptWithSkipSubTree() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(13, "M");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(16, "P");
        btree.insert(17, "Q");

        KeyCollector collector = new KeyCollector() {

            @Override
            public NodeVisitResult preVisitNode(Integer key, Node<Integer, String> node) throws IOException {

                NodeVisitResult result = super.preVisitNode(key, node);

                if (node.getType() == Node.LEAF_NODE) {
                    return NodeVisitResult.SKIP_SUBTREE;
                }

                return result;
            }

        };

        btree.accept(collector);

        AssertCollections.assertListContains(collector.getKeys(), 1, 1, 1, 5, 8, 10, 10, 13, 15);
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testAcceptWithSkipSiblings() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(13, "M");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(16, "P");
        btree.insert(17, "Q");

        KeyCollector collector = new KeyCollector() {

            @Override
            public NodeVisitResult preVisitNode(Integer key, Node<Integer, String> node) throws IOException {

                NodeVisitResult result = super.preVisitNode(key, node);

                if (node.getType() == Node.LEAF_NODE) {
                    return NodeVisitResult.SKIP_SIBLINGS;
                }

                return result;
            }

        };

        btree.accept(collector);

        AssertCollections.assertListContains(collector.getKeys(), 1, 1, 1, 10, 10);
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testAcceptWithSkipSiblingsOnRecords() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(13, "M");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(16, "P");
        btree.insert(17, "Q");

        KeyCollector collector = new KeyCollector() {

            @Override
            public NodeVisitResult visitRecord(Integer key, ValueWrapper<String> pointer) {

                getKeys().add(key);
                return NodeVisitResult.SKIP_SIBLINGS;
            }
        };

        btree.accept(collector);

        AssertCollections.assertListContains(collector.getKeys(), 1, 1, 1, 1, 5, 5, 8, 8, 10, 10, 10, 13, 13, 15, 15);
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testAcceptWithTerminate() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(13, "M");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(16, "P");
        btree.insert(17, "Q");

        KeyCollector collector = new KeyCollector() {

            @Override
            public NodeVisitResult preVisitNode(Integer key, Node<Integer, String> node) throws IOException {

                NodeVisitResult result = super.preVisitNode(key, node);

                if (node.getType() == Node.LEAF_NODE) {
                    return NodeVisitResult.TERMINATE;
                }

                return result;
            }

        };

        btree.accept(collector);

        AssertCollections.assertListContains(collector.getKeys(), 1, 1, 1);
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testAcceptWithNull() throws IOException {

        BTree<Integer, String> btree = new BTree<>(this.manager, 5);

        btree.insert(1, "A");
        btree.insert(2, "B");
        btree.insert(8, "H");
        btree.insert(9, "I");
        btree.insert(13, "M");
        btree.insert(14, "N");
        btree.insert(15, "O");
        btree.insert(5, "E");
        btree.insert(7, "G");
        btree.insert(6, "F");
        btree.insert(3, "C");
        btree.insert(4, "D");
        btree.insert(10, "J");
        btree.insert(11, "K");
        btree.insert(12, "L");
        btree.insert(16, "P");
        btree.insert(17, "Q");

        btree.accept(null);
    }

    /**
     * Asserts that the key and the value of the next record returned by the iterator are equals to
     * the specified ones.
     * 
     * @param iterator the iterator to check
     * @param key the expected key
     * @param value the expected value
     * @throws IOException if an I/O problem occurs
     */
    private static void assertNextContains(KeyValueIterator<Integer, String> iterator, Integer key, String value) 
            throws IOException {
        
        assertTrue(iterator.next());
        assertEquals(key, iterator.getKey());
        assertEquals(value, iterator.getValue());
    }
        
    /**
     * The visitor used during the tests.
     * 
     * @author Benjamin
     * 
     */
    public class KeyCollector implements NodeVisitor<Integer, String> {

        /**
         * The collected keys.
         */
        private final List<Integer> keys = new ArrayList<>();

        /**
         * Returns the collected keys.
         * 
         * @return the collected keys.
         */
        public List<Integer> getKeys() {
            return this.keys;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public NodeVisitResult preVisitNode(Integer key, Node<Integer, String> node) throws IOException {

            this.keys.add(key);
            return NodeVisitResult.CONTINUE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public NodeVisitResult postVisitNode(Integer key, Node<Integer, String> node) {

            return NodeVisitResult.CONTINUE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public NodeVisitResult visitRecord(Integer key, ValueWrapper<String> pointer) {

            this.keys.add(key);
            return NodeVisitResult.CONTINUE;
        }
    }
}
