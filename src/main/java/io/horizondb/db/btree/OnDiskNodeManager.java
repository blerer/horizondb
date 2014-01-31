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

import io.horizondb.db.metrics.Monitorable;
import io.horizondb.io.files.RandomAccessDataFile;
import io.horizondb.io.files.SeekableFileDataOutput;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.codahale.metrics.MetricRegistry;

/**
 * @author Benjamin
 * 
 */
public final class OnDiskNodeManager<K extends Comparable<K>, V> implements Closeable, NodeManager<K, V>, Monitorable {

    /**
     * The name of this component.
     */
    private final String name;

    /**
     * The file path.
     */
    private final RandomAccessDataFile file;

    /**
     * The writer used to write to the file.
     */
    private final NodeWriter<K, V> writer;

    /**
     * The reader used to read the file.
     */
    private final NodeReader<K, V> reader;

    /**
     * The cache used to reduce disk read.
     */
    private final NodeCache<K, V> cache;

    /**
     * The current root node.
     */
    private Node<K, V> root;

    /**
     * Creates a new <code>OnDiskNodeManager</code> instance.
     * 
     * @param name the name of this component
     * @param path the file path
     * @param writerFactory the writer factory
     * @param readerFactory the reader factory
     * @throws IOException if an I/O problem occurs while opening the file.
     */
    public OnDiskNodeManager(String name, 
                             Path path, 
                             NodeWriterFactory<K, V> writerFactory, 
                             NodeReaderFactory<K, V> readerFactory) 
                                     throws IOException {

        this(name, path, writerFactory, readerFactory, new NodeCache<K, V>("NoopCache", 0));
    }
    
    /**
     * Creates a new <code>OnDiskNodeManager</code> instance.
     * 
     * @param name the name of this component
     * @param path the file path
     * @param writerFactory the writer factory
     * @param readerFactory the reader factory
     * @param cache the node cache
     * @throws IOException if an I/O problem occurs while opening the file.
     */
    public OnDiskNodeManager(String name, 
                             Path path, 
                             NodeWriterFactory<K, V> writerFactory, 
                             NodeReaderFactory<K, V> readerFactory,
                             NodeCache<K, V> cache) throws IOException {

        this.name = name;
        this.file = RandomAccessDataFile.open(path, true);

        SeekableFileDataOutput output = this.file.getOutput();
        output.seek(this.file.size());

        this.writer = writerFactory.newWriter(output);
        this.reader = readerFactory.newReader(this.file.newInput());
        
        this.cache = cache;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.name;
    }

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
    public Node<K, V> getRoot(BTree<K, V> btree) throws IOException {

        if (this.root == null) {

            this.root = this.reader.readRoot(btree);
        }

        return this.root;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setRoot(Node<K, V> root) throws IOException {

        this.root = root;

        this.writer.writeRoot(this.root);
        this.writer.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

        this.writer.closeQuietly();
        this.reader.closeQuietly();
        this.file.closeQuietly();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V> wrapNode(Node<K, V> node) throws IOException {

        if (node instanceof NodeProxy) {
            return node;
        }

        long position = this.writer.getPosition();

        int subTreeSize = this.writer.writeNode(node);

        NodeProxy<K, V> proxy = new NodeProxy<K, V>(node.getBTree(), position, subTreeSize);

        this.cache.put(proxy, node);

        return proxy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<K, V> unwrapNode(Node<K, V> node) throws IOException {

        if (node instanceof NodeProxy) {

            return ((NodeProxy<K, V>) node).loadNode();
        }

        return node;
    }

    @Override
    @SafeVarargs
    public final Node<K, V>[] wrapNodes(Node<K, V>... nodes) throws IOException {

        @SuppressWarnings("unchecked")
        Node<K, V>[] newNodes = new Node[nodes.length];

        for (int i = 0; i < nodes.length; i++) {

            newNodes[i] = wrapNode(nodes[i]);
        }

        return newNodes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ValueWrapper<V> wrapValue(V value) throws IOException {

        long position = this.writer.getPosition();

        this.writer.writeData(value);

        int length = (int) (this.writer.getPosition() - position);

        return new ValueProxy<>(this, position, length);
    }

    /**
     * Loads the data corresponding to the specified proxy.
     * 
     * @param proxy the data proxy.
     * @return the data associated to the specified proxy.
     * @throws IOException if the data cannot be read.
     */
    public V loadValue(DataPointer<K, V> proxy) throws IOException {

        return this.reader.readData(proxy.getPosition());
    }

    /**
     * Loads the node corresponding to the specified proxy.
     * 
     * @param proxy the node proxy.
     * @return the node associated to the specified proxy.
     * @throws IOException if the node cannot be read.
     */
    public Node<K, V> loadNode(final NodeProxy<K, V> proxy) throws IOException {

        return this.cache.get(proxy, this.reader);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {

        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("file", this.file).toString();
    }
}
