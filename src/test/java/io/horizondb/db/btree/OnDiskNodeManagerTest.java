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

import io.horizondb.db.btree.BTree;
import io.horizondb.db.btree.InternalNode;
import io.horizondb.db.btree.Node;
import io.horizondb.db.btree.NodeProxy;
import io.horizondb.db.btree.OnDiskNodeManager;
import io.horizondb.io.files.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

import static org.junit.Assert.assertFalse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static io.horizondb.db.btree.AssertNodes.assertInternalNode;
import static io.horizondb.db.btree.AssertNodes.assertLeafNodeContains;
import static io.horizondb.db.btree.AssertNodes.assertLeafNodeEmpty;

public class OnDiskNodeManagerTest {

    /**
     * The test directory.
     */
    private Path testDirectory;

    /**
     * The test file.
     */
    private Path testFile;

    @Before
    public void setUp() throws IOException {

        this.testDirectory = Files.createTempDirectory(this.getClass().getSimpleName());
        this.testFile = this.testDirectory.resolve("test.b3");
    }

    @After
    public void tearDown() throws IOException {

        FileUtils.forceDelete(this.testDirectory);
        this.testDirectory = null;
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testSingleInsertionWithOnlyARootNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {

            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {

            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertLeafNodeContains(btree.getRoot(), 2, "B");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionWithOnlyARootNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertLeafNodeContains(btree.getRoot(), 1, "A", 2, "B", 3, "C", 4, "D");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertingExistingKeyAndOnlyARootNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 4);

            btree.insert(2, "B");
            btree.insert(3, "D");
            btree.insert(1, "A");

            assertLeafNodeContains(btree.getRoot(), 1, "A", 2, "B", 3, "D");

            btree.insert(3, "C");

            assertLeafNodeContains(btree.getRoot(), 1, "A", 2, "B", 3, "C");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertLeafNodeContains(btree.getRoot(), 1, "A", 2, "B", 3, "C");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testGetWithOnlyARootNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {

            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);
            assertEquals("C", btree.get(3));
            assertEquals("B", btree.get(2));
            assertEquals("A", btree.get(1));
            assertNull(btree.get(5));
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionAfterRestartWithOnlyARootNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(5, "E");
            btree.insert(3, "C");
            btree.insert(1, "A");

            assertLeafNodeContains(btree.getRoot(), 1, "A", 3, "C", 5, "E");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);
            btree.insert(2, "B");

            assertLeafNodeContains(btree.getRoot(), 1, "A", 2, "B", 3, "C", 5, "E");
        }
    }
        
    @Test
    @SuppressWarnings({ "boxing" })
    public void testIteratorWithOnlyARootNodeAndFullScan() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {

            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {

            BTree<Integer, String> btree = new BTree<>(manager, 5);

            KeyValueIterator<Integer, String> iterator = btree.iterator(0, 5);

            assertNextContains(iterator, 1, "A");
            assertNextContains(iterator, 2, "B");
            assertNextContains(iterator, 3, "C");
            assertNextContains(iterator, 4, "D");
            assertFalse(iterator.next());
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testIteratorWithOnlyARootNodeAndPartialScan() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            KeyValueIterator<Integer, String> iterator = btree.iterator(2, 4);

            assertNextContains(iterator, 2, "B");
            assertNextContains(iterator, 3, "C");
            assertFalse(iterator.next());
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionWithOnlyARootNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertLeafNodeEmpty(btree.getRoot());
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionAfterRestartWithOnlyARootNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.delete(4);

            assertLeafNodeEmpty(btree.getRoot());
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionWithExistingKeyAndInternalNodes() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
            btree.insert(6, "F");

            btree.insert(6, "E");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 3);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
            assertLeafNodeContains(internalNode.getChild(1), 3, "C", 4, "D", 6, "E");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionWithRootNodeSplit() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
            btree.insert(6, "E");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 3);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
            assertLeafNodeContains(internalNode.getChild(1), 3, "C", 4, "D", 6, "E");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionWithRootNodeSplitAfterRestart() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(6, "E");

            assertInternalNode(btree.getRoot(), 1, 3);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
            assertLeafNodeContains(internalNode.getChild(1), 3, "C", 4, "D", 6, "E");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionWithInternalNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
            btree.insert(6, "E");

            btree.delete(4);
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 3);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
            assertLeafNodeContains(internalNode.getChild(1), 3, "C", 6, "E");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testGetWithOneInternalNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {

            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testGetWithOneInternalNodeAfterRestart() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {

            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(6, "F");
            btree.insert(5, "E");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {

            BTree<Integer, String> btree = new BTree<>(manager, 4);

            assertEquals("B", btree.get(2));
            assertEquals("C", btree.get(3));
            assertEquals("E", btree.get(5));
            assertEquals("F", btree.get(6));
            assertNull(btree.get(10));
            assertNull(btree.get(1));
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionWithInternalNodeAfterRestart() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
            btree.insert(6, "E");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.delete(4);

            assertInternalNode(btree.getRoot(), 1, 3);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
            assertLeafNodeContains(internalNode.getChild(1), 3, "C", 6, "E");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfFirstKeyOfLeafNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
            btree.insert(6, "E");

            btree.delete(3);
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 4);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
            assertLeafNodeContains(internalNode.getChild(1), 4, "D", 6, "E");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testIteratorWithInternalNodesAndPartialScanOverTwoNodes() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {

            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
            btree.insert(6, "F");
        }
        
        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {

            BTree<Integer, String> btree = new BTree<>(manager, 5);

            KeyValueIterator<Integer, String> iterator = btree.iterator(2, 4);

            assertNextContains(iterator, 2, "B");
            assertNextContains(iterator, 3, "C");
            assertFalse(iterator.next());
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testIteratorWithInternalNodesAndPartialScanOverOneNode() throws IOException {
        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
            btree.insert(6, "F");
        }
        
        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            KeyValueIterator<Integer, String> iterator = btree.iterator(0, 2);

            assertNextContains(iterator, 1, "A");
            assertFalse(iterator.next());
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testIteratorWithInternalNodesAndFullScan() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {

            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
            btree.insert(6, "F");
        }
        
        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {

            BTree<Integer, String> btree = new BTree<>(manager, 5);

            KeyValueIterator<Integer, String> iterator = btree.iterator(0, 10);

            assertNextContains(iterator, 1, "A");
            assertNextContains(iterator, 2, "B");
            assertNextContains(iterator, 3, "C");
            assertNextContains(iterator, 4, "D");
            assertNextContains(iterator, 6, "F");
            assertFalse(iterator.next());
        }
    }
    
    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfFirstKeyOfLeafNodeAfterRestart() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
            btree.insert(6, "E");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.delete(3);

            assertInternalNode(btree.getRoot(), 1, 4);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
            assertLeafNodeContains(internalNode.getChild(1), 4, "D", 6, "E");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionWithRootNodeSplitAndBranchingFactor4() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 4);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(1, "A");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 4);

            assertInternalNode(btree.getRoot(), 1, 3);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
            assertLeafNodeContains(internalNode.getChild(1), 3, "C", 4, "D");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionOnLeftLeafNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(6, "F");
            btree.insert(5, "E");

            btree.insert(1, "A");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 4);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C");
            assertLeafNodeContains(internalNode.getChild(1), 4, "D", 5, "E", 6, "F");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionOnRightLeafNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(6, "F");
            btree.insert(5, "E");

            btree.insert(7, "G");
        }
        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 2, 4);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 2, "B", 3, "C");
            assertLeafNodeContains(internalNode.getChild(1), 4, "D", 5, "E", 6, "F", 7, "G");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionOnRightLeafNodeWithSplit() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(3, "C");
            btree.insert(6, "F");
            btree.insert(5, "E");

            btree.insert(7, "G");
            btree.insert(9, "I");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 2, 4, 6);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 2, "B", 3, "C");
            assertLeafNodeContains(internalNode.getChild(1), 4, "D", 5, "E");
            assertLeafNodeContains(internalNode.getChild(2), 6, "F", 7, "G", 9, "I");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionOnLeftLeafNodeWithSplit() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(2, "B");
            btree.insert(3, "C");
            btree.insert(6, "F");
            btree.insert(7, "G");
            btree.insert(9, "I");

            btree.insert(1, "A");
            btree.insert(5, "E");
            btree.insert(4, "D");

        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 3, 6);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
            assertLeafNodeContains(internalNode.getChild(1), 3, "C", 4, "D", 5, "E");
            assertLeafNodeContains(internalNode.getChild(2), 6, "F", 7, "G", 9, "I");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionOnLeafNodeWithLeafAndParentFull() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 10);

            InternalNode<Integer, String> root = toInternalNode(btree.getRoot());

            assertInternalNode(root.getChild(0), 1, 5, 8);

            InternalNode<Integer, String> internalNode = toInternalNode(root.getChild(0));

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
            assertLeafNodeContains(internalNode.getChild(1), 5, "E", 6, "F", 7, "G");
            assertLeafNodeContains(internalNode.getChild(2), 8, "H", 9, "I");

            assertInternalNode(root.getChild(1), 10, 14, 16);

            internalNode = toInternalNode(root.getChild(1));

            assertLeafNodeContains(internalNode.getChild(0), 10, "J", 11, "K", 12, "L");
            assertLeafNodeContains(internalNode.getChild(1), 14, "N", 15, "O");
            assertLeafNodeContains(internalNode.getChild(2), 16, "P", 17, "Q", 18, "R");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionOnLeafNodeWithLeafAndParentFullAfterRestart() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(18, "R");

            assertInternalNode(btree.getRoot(), 1, 10);

            InternalNode<Integer, String> root = toInternalNode(btree.getRoot());

            assertInternalNode(root.getChild(0), 1, 5, 8);

            InternalNode<Integer, String> internalNode = toInternalNode(root.getChild(0));

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
            assertLeafNodeContains(internalNode.getChild(1), 5, "E", 6, "F", 7, "G");
            assertLeafNodeContains(internalNode.getChild(2), 8, "H", 9, "I");

            assertInternalNode(root.getChild(1), 10, 14, 16);

            internalNode = toInternalNode(root.getChild(1));

            assertLeafNodeContains(internalNode.getChild(0), 10, "J", 11, "K", 12, "L");
            assertLeafNodeContains(internalNode.getChild(1), 14, "N", 15, "O");
            assertLeafNodeContains(internalNode.getChild(2), 16, "P", 17, "Q", 18, "R");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertWith3LevelDepth() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 3);

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
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 3);

            assertInternalNode(btree.getRoot(), 1, 8, 14);

            InternalNode<Integer, String> root = toInternalNode(btree.getRoot());

            InternalNode<Integer, String> internalNodeDepth1 = toInternalNode(root.getChild(0));

            assertInternalNode(internalNodeDepth1, 1, 5);

            InternalNode<Integer, String> internalNodeDepth2 = toInternalNode(internalNodeDepth1.getChild(0));

            assertInternalNode(internalNodeDepth2, 1, 2, 3);

            assertLeafNodeContains(internalNodeDepth2.getChild(0), 1, "A");
            assertLeafNodeContains(internalNodeDepth2.getChild(1), 2, "B");
            assertLeafNodeContains(internalNodeDepth2.getChild(2), 3, "C", 4, "D");

            internalNodeDepth2 = toInternalNode(internalNodeDepth1.getChild(1));

            assertInternalNode(internalNodeDepth2, 5, 6);

            assertLeafNodeContains(internalNodeDepth2.getChild(0), 5, "E");
            assertLeafNodeContains(internalNodeDepth2.getChild(1), 6, "F", 7, "G");

            internalNodeDepth1 = toInternalNode(root.getChild(1));

            assertInternalNode(internalNodeDepth1, 8, 10);

            internalNodeDepth2 = toInternalNode(internalNodeDepth1.getChild(0));

            assertInternalNode(internalNodeDepth2, 8, 9);

            assertLeafNodeContains(internalNodeDepth2.getChild(0), 8, "H");
            assertLeafNodeContains(internalNodeDepth2.getChild(1), 9, "I");

            internalNodeDepth2 = toInternalNode(internalNodeDepth1.getChild(1));

            assertInternalNode(internalNodeDepth2, 10, 11, 12);

            assertLeafNodeContains(internalNodeDepth2.getChild(0), 10, "J");
            assertLeafNodeContains(internalNodeDepth2.getChild(1), 11, "K");
            assertLeafNodeContains(internalNodeDepth2.getChild(2), 12, "L", 13, "M");

            internalNodeDepth1 = toInternalNode(root.getChild(2));

            assertInternalNode(internalNodeDepth1, 14, 16);

            internalNodeDepth2 = toInternalNode(internalNodeDepth1.getChild(0));

            assertInternalNode(internalNodeDepth2, 14, 15);

            assertLeafNodeContains(internalNodeDepth2.getChild(0), 14, "N");
            assertLeafNodeContains(internalNodeDepth2.getChild(1), 15, "O");

            internalNodeDepth2 = toInternalNode(internalNodeDepth1.getChild(1));

            assertInternalNode(internalNodeDepth2, 16, 17);

            assertLeafNodeContains(internalNodeDepth2.getChild(0), 16, "P");
            assertLeafNodeContains(internalNodeDepth2.getChild(1), 17, "Q", 18, "R");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertionOnLeafNodeWithLeafFullAndParentNot() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 8, 15);

            InternalNode<Integer, String> root = toInternalNode(btree.getRoot());

            assertInternalNode(root.getChild(0), 1, 3, 6);

            InternalNode<Integer, String> internalNode = toInternalNode(root.getChild(0));

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
            assertLeafNodeContains(internalNode.getChild(1), 3, "C", 4, "D", 5, "E");
            assertLeafNodeContains(internalNode.getChild(2), 6, "F", 7, "G");

            assertInternalNode(root.getChild(1), 8, 10, 13);

            internalNode = toInternalNode(root.getChild(1));

            assertLeafNodeContains(internalNode.getChild(0), 8, "H", 9, "I");
            assertLeafNodeContains(internalNode.getChild(1), 10, "J", 11, "K", 12, "L");
            assertLeafNodeContains(internalNode.getChild(2), 13, "M", 14, "N");

            assertInternalNode(root.getChild(2), 15, 20, 23);

            internalNode = toInternalNode(root.getChild(2));

            assertLeafNodeContains(internalNode.getChild(0), 15, "O", 17, "Q", 18, "R");
            assertLeafNodeContains(internalNode.getChild(1), 20, "T", 22, "V");
            assertLeafNodeContains(internalNode.getChild(2), 23, "W", 24, "X", 25, "Y");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertWithInternalNodeSplitWithParentNotFull() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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

            btree.insert(26, "Z");
            btree.insert(27, "AA");
            btree.insert(28, "BB");
            btree.insert(29, "CC");
            btree.insert(30, "DD");
            btree.insert(31, "EE");
        }
        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 15, 25);

            InternalNode<Integer, String> root = toInternalNode(btree.getRoot());

            assertInternalNode(root.getChild(0), 1, 6, 13);

            InternalNode<Integer, String> internalNode = toInternalNode(root.getChild(0));

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
            assertLeafNodeContains(internalNode.getChild(1), 6, "F", 7, "G", 8, "H");
            assertLeafNodeContains(internalNode.getChild(2), 13, "M", 14, "N");

            assertInternalNode(root.getChild(1), 15, 20, 23);

            internalNode = toInternalNode(root.getChild(1));

            assertLeafNodeContains(internalNode.getChild(0), 15, "O", 17, "Q", 18, "R", 18, "S");
            assertLeafNodeContains(internalNode.getChild(1), 20, "T", 22, "V");
            assertLeafNodeContains(internalNode.getChild(2), 23, "W", 24, "X");

            assertInternalNode(root.getChild(2), 25, 27, 29);

            internalNode = toInternalNode(root.getChild(2));

            assertLeafNodeContains(internalNode.getChild(0), 25, "Y", 26, "Z");
            assertLeafNodeContains(internalNode.getChild(1), 27, "AA", 28, "BB");
            assertLeafNodeContains(internalNode.getChild(2), 29, "CC", 30, "DD", 31, "EE");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testInsertAfterRestartWithInternalNodeSplitWithParentNotFull() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
        }
        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(26, "Z");
            btree.insert(27, "AA");
            btree.insert(28, "BB");
            btree.insert(29, "CC");
            btree.insert(30, "DD");
            btree.insert(31, "EE");

            assertInternalNode(btree.getRoot(), 1, 15, 25);

            InternalNode<Integer, String> root = toInternalNode(btree.getRoot());

            assertInternalNode(root.getChild(0), 1, 6, 13);

            InternalNode<Integer, String> internalNode = toInternalNode(root.getChild(0));

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
            assertLeafNodeContains(internalNode.getChild(1), 6, "F", 7, "G", 8, "H");
            assertLeafNodeContains(internalNode.getChild(2), 13, "M", 14, "N");

            assertInternalNode(root.getChild(1), 15, 20, 23);

            internalNode = toInternalNode(root.getChild(1));

            assertLeafNodeContains(internalNode.getChild(0), 15, "O", 17, "Q", 18, "R", 18, "S");
            assertLeafNodeContains(internalNode.getChild(1), 20, "T", 22, "V");
            assertLeafNodeContains(internalNode.getChild(2), 23, "W", 24, "X");

            assertInternalNode(root.getChild(2), 25, 27, 29);

            internalNode = toInternalNode(root.getChild(2));

            assertLeafNodeContains(internalNode.getChild(0), 25, "Y", 26, "Z");
            assertLeafNodeContains(internalNode.getChild(1), 27, "AA", 28, "BB");
            assertLeafNodeContains(internalNode.getChild(2), 29, "CC", 30, "DD", 31, "EE");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletingKeysWhichAreWithinTheRoot() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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

            btree.delete(15);
            btree.delete(1);
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 2, 17);

            InternalNode<Integer, String> root = toInternalNode(btree.getRoot());

            assertInternalNode(root.getChild(0), 2, 6, 13);

            InternalNode<Integer, String> internalNode = toInternalNode(root.getChild(0));

            assertLeafNodeContains(internalNode.getChild(0), 2, "B", 3, "C", 4, "D");
            assertLeafNodeContains(internalNode.getChild(1), 6, "F", 7, "G", 8, "H");
            assertLeafNodeContains(internalNode.getChild(2), 13, "M", 14, "N");

            assertInternalNode(root.getChild(1), 17, 20, 23);

            internalNode = toInternalNode(root.getChild(1));

            assertLeafNodeContains(internalNode.getChild(0), 17, "Q", 18, "R", 18, "S");
            assertLeafNodeContains(internalNode.getChild(1), 20, "T", 22, "V");
            assertLeafNodeContains(internalNode.getChild(2), 23, "W", 24, "X", 25, "Y");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionWithInternalNodeRebalancingFromTheRightNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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

            btree.delete(25);
            btree.delete(4);
            btree.delete(2);
            btree.delete(7);
            btree.delete(5);
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 14, 21);

            InternalNode<Integer, String> root = toInternalNode(btree.getRoot());

            assertInternalNode(root.getChild(0), 1, 6, 10);

            InternalNode<Integer, String> internalNode = toInternalNode(root.getChild(0));

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 3, "C");
            assertLeafNodeContains(internalNode.getChild(1), 6, "F", 8, "H", 9, "I");
            assertLeafNodeContains(internalNode.getChild(2), 10, "J", 11, "K", 12, "L", 13, "M");

            assertInternalNode(root.getChild(1), 14, 16, 18);

            internalNode = toInternalNode(root.getChild(1));

            assertLeafNodeContains(internalNode.getChild(0), 14, "N", 15, "O");
            assertLeafNodeContains(internalNode.getChild(1), 16, "P", 17, "Q");
            assertLeafNodeContains(internalNode.getChild(2), 18, "R", 19, "S", 20, "T");

            assertInternalNode(root.getChild(2), 21, 24, 27);

            internalNode = toInternalNode(root.getChild(2));

            assertLeafNodeContains(internalNode.getChild(0), 21, "U", 23, "W");
            assertLeafNodeContains(internalNode.getChild(1), 24, "X", 26, "Z");
            assertLeafNodeContains(internalNode.getChild(2), 27, "AA", 28, "BB");

        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionAfterRestartWithInternalNodeRebalancingFromTheRightNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.delete(25);
            btree.delete(4);
            btree.delete(2);
            btree.delete(7);
            btree.delete(5);

            assertInternalNode(btree.getRoot(), 1, 14, 21);

            InternalNode<Integer, String> root = toInternalNode(btree.getRoot());

            assertInternalNode(root.getChild(0), 1, 6, 10);

            InternalNode<Integer, String> internalNode = toInternalNode(root.getChild(0));

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 3, "C");
            assertLeafNodeContains(internalNode.getChild(1), 6, "F", 8, "H", 9, "I");
            assertLeafNodeContains(internalNode.getChild(2), 10, "J", 11, "K", 12, "L", 13, "M");

            assertInternalNode(root.getChild(1), 14, 16, 18);

            internalNode = toInternalNode(root.getChild(1));

            assertLeafNodeContains(internalNode.getChild(0), 14, "N", 15, "O");
            assertLeafNodeContains(internalNode.getChild(1), 16, "P", 17, "Q");
            assertLeafNodeContains(internalNode.getChild(2), 18, "R", 19, "S", 20, "T");

            assertInternalNode(root.getChild(2), 21, 24, 27);

            internalNode = toInternalNode(root.getChild(2));

            assertLeafNodeContains(internalNode.getChild(0), 21, "U", 23, "W");
            assertLeafNodeContains(internalNode.getChild(1), 24, "X", 26, "Z");
            assertLeafNodeContains(internalNode.getChild(2), 27, "AA", 28, "BB");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeleteWithInternalNodeRebalancingFromTheLeftNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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

            btree.delete(27);
            btree.delete(24);
            btree.delete(22);
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 10, 18);

            InternalNode<Integer, String> root = toInternalNode(btree.getRoot());

            assertInternalNode(root.getChild(0), 1, 5, 8);

            InternalNode<Integer, String> internalNode = toInternalNode(root.getChild(0));

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
            assertLeafNodeContains(internalNode.getChild(1), 5, "E", 6, "F", 7, "G");
            assertLeafNodeContains(internalNode.getChild(2), 8, "H", 9, "I");

            assertInternalNode(root.getChild(1), 10, 14, 16);

            internalNode = toInternalNode(root.getChild(1));

            assertLeafNodeContains(internalNode.getChild(0), 10, "J", 11, "K", 12, "L", 13, "M");
            assertLeafNodeContains(internalNode.getChild(1), 14, "N", 15, "O");
            assertLeafNodeContains(internalNode.getChild(2), 16, "P", 17, "Q");

            assertInternalNode(root.getChild(2), 18, 21, 26);

            internalNode = toInternalNode(root.getChild(2));

            assertLeafNodeContains(internalNode.getChild(0), 18, "R", 19, "S", 20, "T");
            assertLeafNodeContains(internalNode.getChild(1), 21, "U", 23, "W", 25, "Y");
            assertLeafNodeContains(internalNode.getChild(2), 26, "Z", 28, "BB");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyFromLeafNodeWithRedistributionOnRightNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 6, 8);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C");
            assertLeafNodeContains(internalNode.getChild(1), 6, "F", 7, "G");
            assertLeafNodeContains(internalNode.getChild(2), 8, "H", 9, "I", 10, "J");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyFromLeafNodeWithFirstKeyDeletedAndRedistribution() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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

        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 6, 8);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C");
            assertLeafNodeContains(internalNode.getChild(1), 6, "F", 7, "G");
            assertLeafNodeContains(internalNode.getChild(2), 8, "H", 9, "I", 10, "J");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyAfterRestartFromLeafNodeWithFirstKeyDeletedAndRedistribution() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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

        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.delete(4);
            btree.delete(5);

            assertInternalNode(btree.getRoot(), 1, 6, 8);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C");
            assertLeafNodeContains(internalNode.getChild(1), 6, "F", 7, "G");
            assertLeafNodeContains(internalNode.getChild(2), 8, "H", 9, "I", 10, "J");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyFromLeafNodeWithRedistributionOnLeftNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 4, 7);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B", 3, "C");
            assertLeafNodeContains(internalNode.getChild(1), 4, "D", 5, "E", 6, "F");
            assertLeafNodeContains(internalNode.getChild(2), 7, "G", 10, "J");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyFromLeafNodeWithMergeOnTheRight() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 2, 7);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 2, "B", 4, "D", 6, "F");
            assertLeafNodeContains(internalNode.getChild(1), 7, "G", 9, "I");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyFromLeafNodeWithMergeOnTheLeft() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 6);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
            assertLeafNodeContains(internalNode.getChild(1), 6, "F", 7, "G", 9, "I");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyFromLeafNodeWithMergeAndNoRightNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 3);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
            assertLeafNodeContains(internalNode.getChild(1), 3, "C", 6, "F", 9, "I");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionOfKeyAfterRestartFromLeafNodeWithMergeAndNoRightNode() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.insert(1, "A");
            btree.insert(2, "B");
            btree.insert(4, "D");
            btree.insert(5, "E");
            btree.insert(7, "G");
            btree.insert(8, "H");
            btree.insert(9, "I");
            btree.insert(3, "C");
            btree.insert(6, "F");
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.delete(4);
            btree.delete(8);
            btree.delete(5);
            btree.delete(7);

            assertInternalNode(btree.getRoot(), 1, 3);

            InternalNode<Integer, String> internalNode = toInternalNode(btree.getRoot());

            assertLeafNodeContains(internalNode.getChild(0), 1, "A", 2, "B");
            assertLeafNodeContains(internalNode.getChild(1), 3, "C", 6, "F", 9, "I");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionWithLeafNodeMergeAndInternalNodeMerge() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            assertInternalNode(btree.getRoot(), 1, 6, 8, 11, 15);

            InternalNode<Integer, String> root = toInternalNode(btree.getRoot());

            assertLeafNodeContains(root.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
            assertLeafNodeContains(root.getChild(1), 6, "F", 7, "G");
            assertLeafNodeContains(root.getChild(2), 8, "H", 9, "I");
            assertLeafNodeContains(root.getChild(3), 11, "K", 13, "M", 14, "N");
            assertLeafNodeContains(root.getChild(4), 15, "O", 16, "P", 17, "Q");
        }
    }

    @Test
    @SuppressWarnings({ "boxing" })
    public void testDeletionAfterRestartWithLeafNodeMergeAndInternalNodeMerge() throws IOException {

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

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
        }

        try (OnDiskNodeManager<Integer, String> manager = new OnDiskNodeManager<>("test",
                                                                                  this.testFile,
                                                                                  IntegerAndStringNodeWriter.FACTORY,
                                                                                  IntegerAndStringNodeReader.FACTORY)) {
            BTree<Integer, String> btree = new BTree<>(manager, 5);

            btree.delete(12);
            btree.delete(5);
            btree.delete(10);

            assertInternalNode(btree.getRoot(), 1, 6, 8, 11, 15);

            InternalNode<Integer, String> root = toInternalNode(btree.getRoot());

            assertLeafNodeContains(root.getChild(0), 1, "A", 2, "B", 3, "C", 4, "D");
            assertLeafNodeContains(root.getChild(1), 6, "F", 7, "G");
            assertLeafNodeContains(root.getChild(2), 8, "H", 9, "I");
            assertLeafNodeContains(root.getChild(3), 11, "K", 13, "M", 14, "N");
            assertLeafNodeContains(root.getChild(4), 15, "O", 16, "P", 17, "Q");
        }
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
     * Converts the specified node into an internal node.
     * 
     * @param node the node to convert
     * @return an internal node.
     * @throws IOException if an I/O problem occurs.
     */
    private static InternalNode<Integer, String> toInternalNode(Node<Integer, String> node) throws IOException {

        Node<Integer, String> n = node;

        if (n instanceof NodeProxy) {

            n = ((NodeProxy<Integer, String>) n).loadNode();
        }

        return (InternalNode<Integer, String>) n;
    }
}
