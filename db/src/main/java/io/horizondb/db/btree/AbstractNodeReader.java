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

import io.horizondb.io.ByteReader;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.checksum.ChecksumByteReader;
import io.horizondb.io.files.SeekableFileDataInput;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.horizondb.io.encoding.VarInts.readByte;
import static io.horizondb.io.encoding.VarInts.readUnsignedInt;
import static io.horizondb.io.encoding.VarInts.readUnsignedLong;

/**
 * @author Benjamin
 * 
 */
public abstract class AbstractNodeReader<K extends Comparable<K>, V> implements NodeReader<K, V> {

    /**
     * The instance logger.
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * The file input.
     */
    private final BlockOrganizedFileDataInput input;

    /**
     * The reader decorator used to validate checksums.
     */
    private final ChecksumByteReader checksumReader;

    /**
     * The version of the file format.
     */
    private int version = Constants.CURRENT_VERSION;

    /**
     * The compression that has been used to compress the data.
     */
    private int compression = Constants.NO_COMPRESSION;

    public AbstractNodeReader(SeekableFileDataInput input) throws IOException {

        this.input = new BlockOrganizedFileDataInput(Constants.BLOCK_SIZE, input);
        this.checksumReader = ChecksumByteReader.wrap(this.input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Node<K, V> readRoot(BTree<K, V> btree) throws IOException {

        if (!this.input.seekHeader()) {

            this.logger.debug("No header found within the file creating root node.");

            return new LeafNode<>(btree);
        }

        int length = readUnsignedInt(this.input);

        this.checksumReader.resetChecksum();
        ByteReader nodeReader = this.checksumReader.slice(length);

        this.version = readByte(nodeReader);
        this.compression = readByte(nodeReader);

        return readNode(btree, nodeReader);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V readData(long position) throws IOException {

        this.input.seek(position);
        int length = readUnsignedInt(this.input);

        this.checksumReader.resetChecksum();

        ReadableBuffer slice = this.checksumReader.slice(length);

        if (!this.checksumReader.readChecksum()) {

            throw new IllegalStateException("A CRC mismatch has occured while reading data at position: " + position
                    + " of length: " + length);
        }

        return readValue(slice);
    }

    /**
     * @param slice
     * @return
     * @throws IOException
     */
    protected abstract V readValue(ByteReader reader) throws IOException;

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {

        this.input.close();
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
    public Node<K, V> readNode(BTree<K, V> btree, long position) throws IOException {

        this.input.seek(position);

        int length = readUnsignedInt(this.input);

        this.checksumReader.resetChecksum();

        ReadableBuffer slice = this.checksumReader.slice(length);

        if (!this.checksumReader.readChecksum()) {

            throw new IllegalStateException("A CRC mismatch has occured while reading node data at position: "
                    + position + " of length: " + length);
        }

        return readNode(btree, slice);
    }

    /**
     * @param btree
     * @param nodeReader
     * @return
     * @throws IOException
     */
    private Node<K, V> readNode(BTree<K, V> btree, ByteReader reader) throws IOException {

        int type = readByte(reader);

        int subTreeSize = readUnsignedInt(reader);

        if (isLeafNode(type)) {

            SortedMap<K, ValueWrapper<V>> records = readNodeRecords(btree, reader);

            return LeafNode.<K, V> newInstance(btree, records);
        }

        SortedMap<K, Node<K, V>> children = readNodeChildren(btree, reader);

        return InternalNode.<K, V> newInstance(btree, children);
    }

    /**
     * @param btree
     * @param reader
     * @return
     * @throws IOException
     */
    private SortedMap<K, ValueWrapper<V>> readNodeRecords(BTree<K, V> btree, ByteReader reader) throws IOException {

        SortedMap<K, ValueWrapper<V>> records = new TreeMap<K, ValueWrapper<V>>();

        while (reader.isReadable()) {

            records.put(readKey(reader), readValueWrapper(btree, reader));
        }

        return records;
    }

    private SortedMap<K, Node<K, V>> readNodeChildren(BTree<K, V> btree, ByteReader reader) throws IOException {

        SortedMap<K, Node<K, V>> children = new TreeMap<K, Node<K, V>>();

        while (reader.isReadable()) {

            children.put(readKey(reader), readNodeProxy(btree, reader));
        }

        return children;
    }

    protected ValueWrapper<V> readValueWrapper(BTree<K, V> btree, ByteReader reader) throws IOException {

        long position = readUnsignedLong(reader);
        int lenght = readUnsignedInt(reader);

        OnDiskNodeManager<K, V> manager = (OnDiskNodeManager<K, V>) btree.getManager();

        return new ValueProxy<K, V>(manager, position, lenght);
    }

    protected Node<K, V> readNodeProxy(BTree<K, V> btree, ByteReader reader) throws IOException {

        long position = readUnsignedLong(reader);
        int subTreeSize = readUnsignedInt(reader);

        return new NodeProxy<>(btree, position, subTreeSize);
    }

    /**
     * @return
     * @throws IOException
     */
    protected abstract K readKey(ByteReader reader) throws IOException;

    /**
     * @param type
     * @return
     */
    private static boolean isLeafNode(int type) {
        return Node.LEAF_NODE == type;
    }

}
