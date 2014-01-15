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

import io.horizondb.io.ByteWriter;
import io.horizondb.io.checksum.ChecksumByteWriter;
import io.horizondb.io.files.FileDataOutput;

import java.io.IOException;

import static io.horizondb.io.encoding.VarInts.computeUnsignedIntSize;
import static io.horizondb.io.encoding.VarInts.computeUnsignedLongSize;
import static io.horizondb.io.encoding.VarInts.writeByte;
import static io.horizondb.io.encoding.VarInts.writeUnsignedInt;
import static io.horizondb.io.encoding.VarInts.writeUnsignedLong;

import static org.apache.commons.lang.Validate.notNull;

/**
 * Base class for writing B+Tree nodes to the disk.
 * 
 * @author Benjamin
 */
public abstract class AbstractNodeWriter<K extends Comparable<K>, V> implements NodeWriter<K, V> {

    /**
     * The length in byte needed to write the file format version.
     */
    private static final int FILE_FORMAT_VERSION_LENGTH = 1;

    /**
     * The compression type.
     */
    private static final int COMPRESSION_TYPE_LENGTH = 1;

    /**
     * The length in byte needed to write the node type.
     */
    private static final int NODE_TYPE_LENGTH = 1;

    /**
     * The file output.
     */
    private final BlockOrganizedFileDataOutput output;

    /**
     * The writer decorator used to compute checksums.
     */
    private final ChecksumByteWriter checksumWriter;

    /**
     * Creates a new writer that will write to the specified output.
     * 
     * @param output the output towards the file.
     * @throws IOException if a problem occurs while writing to the disk.
     */
    public AbstractNodeWriter(FileDataOutput output) throws IOException {

        notNull(output, "the output parameter must not be null.");

        this.output = new BlockOrganizedFileDataOutput(Constants.BLOCK_SIZE, output);
        this.checksumWriter = ChecksumByteWriter.wrap(this.output);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void closeQuietly() {

        try {

            close();

        } catch (Exception e) {

            // Do nothing.
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void close() throws IOException {

        this.output.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void flush() throws IOException {

        this.output.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int writeNode(Node<K, V> node) throws IOException {

        NodeSizeCalculator calculator = new NodeSizeCalculator();
        node.accept(calculator);

        int nodeSize = calculator.getNodeSize();
        int subTreeSize = calculator.getSubTreeSize();

        int length = NODE_TYPE_LENGTH + computeUnsignedIntSize(subTreeSize) + nodeSize;

        writeUnsignedInt(this.output, length);
        writeNode(node, subTreeSize);

        return subTreeSize;
    }

    /**
     * @param node
     * @param subTreeSize
     * @throws IOException
     */
    private void writeNode(Node<K, V> node, int subTreeSize) throws IOException {

        writeNodeType(node);
        writeUnsignedInt(this.checksumWriter, subTreeSize);

        NodePersister persister = new NodePersister(this.checksumWriter);

        node.accept(persister);

        this.checksumWriter.writeChecksum();
        this.checksumWriter.reset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int writeData(V value) throws IOException {

        long position = getPosition();

        writeUnsignedInt(this.output, computeValueSize(value));
        writeValue(this.checksumWriter, value);
        this.checksumWriter.writeChecksum();
        this.checksumWriter.reset();

        return (int) (getPosition() - position);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getPosition() throws IOException {

        return this.output.getPosition();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void writeRoot(Node<K, V> node) throws IOException {

        this.output.switchBlockType();

        NodeSizeCalculator calculator = new NodeSizeCalculator();
        node.accept(calculator);

        int nodeSize = calculator.getNodeSize();
        int subTreeSize = calculator.getSubTreeSize();

        int length = FILE_FORMAT_VERSION_LENGTH + COMPRESSION_TYPE_LENGTH + NODE_TYPE_LENGTH
                + computeUnsignedIntSize(subTreeSize) + nodeSize;

        writeUnsignedInt(this.output, length);

        writeVersion();
        writeCompressionType();
        writeNode(node, subTreeSize);

        this.output.switchBlockType();
    }

    /**
     * Computes the size in bytes needed to store the specified key.
     * 
     * @param key the key.
     * @return the size in bytes needed to store the specified key.
     */
    protected abstract int computeKeySize(K key);

    protected abstract int computeValueSize(V value);

    /**
     * Writes the specified key to the disk.
     * 
     * @param writer the writer that must be used to write the data.
     * @param key the key to write.
     * @throws IOException if an I/O problem occurs while writing the data to the disk.
     */
    protected abstract void writeKey(ByteWriter writer, K key) throws IOException;

    /**
     * Writes the specified value to the disk.
     * 
     * @param writer the writer that must be used to write the data.
     * @param value the value to write.
     * @throws IOException if an I/O problem occurs while writing the data to the disk.
     */
    protected abstract void writeValue(ByteWriter writer, V value) throws IOException;

    /**
     * Writes the type of node.
     * 
     * @param node the node for which the type must be written.
     * @throws IOException if an I/O problem occurs while writing.
     */
    private void writeNodeType(Node<K, V> node) throws IOException {
        writeByte(this.checksumWriter, node.getType());
    }

    /**
     * Writes the type of compression used within the file.
     * 
     * @throws IOException if an I/O problem occurs while writing.
     */
    private void writeCompressionType() throws IOException {
        writeByte(this.checksumWriter, Constants.NO_COMPRESSION);
    }

    /**
     * Writes the version of the file format.
     * 
     * @throws IOException if an I/O problem occurs while writing.
     */
    private void writeVersion() throws IOException {
        writeByte(this.checksumWriter, Constants.CURRENT_VERSION);
    }

    /**
     * <code>NodeVisitor</code> that compute the size in bytes required to store the visited node.
     */
    private final class NodeSizeCalculator extends SimpleNodeVisitor<K, V> {

        /**
         * The size in bytes of the node.
         */
        private int nodeSize;

        /**
         * The size in bytes used by the sub-tree.
         */
        private int subTreeSize;

        /**
         * Returns the size in bytes of the node.
         * 
         * @return the size in bytes of the node.
         */
        public int getNodeSize() {
            return this.nodeSize;
        }

        /**
         * Returns the size in bytes used by the sub-tree.
         * 
         * @return the size in bytes used by the sub-tree.
         */
        public int getSubTreeSize() {
            return this.subTreeSize;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public NodeVisitResult preVisitNode(K key, Node<K, V> node) {

            NodeProxy<K, V> proxy = (NodeProxy<K, V>) node;

            this.nodeSize += computeKeySize(key) + computeUnsignedLongSize(proxy.getPosition())
                    + computeUnsignedIntSize(proxy.getSubTreeSize());

            this.subTreeSize += proxy.getSubTreeSize();

            return NodeVisitResult.SKIP_SUBTREE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @SuppressWarnings("unchecked")
        public NodeVisitResult visitRecord(K key, ValueWrapper<V> wrapper) {

            DataPointer<K, V> pointer = (DataPointer<K, V>) wrapper;

            this.nodeSize += computeKeySize(key) + computeUnsignedLongSize(pointer.getPosition())
                    + computeUnsignedIntSize(pointer.getSubTreeSize());

            this.subTreeSize += pointer.getSubTreeSize();

            return NodeVisitResult.CONTINUE;
        }
    }

    /**
     * <code>NodeVisitor</code> that serialize to the disk the node that it is visiting.
     */
    private final class NodePersister extends SimpleNodeVisitor<K, V> {

        /**
         * The writer used to write the data to the disk.
         */
        private final ByteWriter writer;

        /**
         * Creates a new <code>NodePersister</code> that use the specified writer to write data to the disk.
         * 
         * @param writer the writer.
         */
        public NodePersister(ByteWriter writer) {

            this.writer = writer;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public NodeVisitResult preVisitNode(K key, Node<K, V> node) throws IOException {

            NodeProxy<K, V> proxy = (NodeProxy<K, V>) node;

            writeKey(this.writer, key);
            writeUnsignedLong(this.writer, proxy.getPosition());
            writeUnsignedInt(this.writer, proxy.getSubTreeSize());

            return NodeVisitResult.SKIP_SUBTREE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @SuppressWarnings("unchecked")
        public NodeVisitResult visitRecord(K key, ValueWrapper<V> wrapper) throws IOException {

            DataPointer<K, V> pointer = (DataPointer<K, V>) wrapper;

            writeKey(this.writer, key);
            writeUnsignedLong(this.writer, pointer.getPosition());
            writeUnsignedInt(this.writer, pointer.getSubTreeSize());

            return NodeVisitResult.CONTINUE;
        }
    }
}
