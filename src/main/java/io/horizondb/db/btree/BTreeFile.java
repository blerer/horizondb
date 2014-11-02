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
import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.files.FileDataOutput;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

import com.codahale.metrics.MetricRegistry;

import static org.apache.commons.lang.Validate.notNull;

import static org.apache.commons.lang.Validate.notEmpty;

/**
 * @author Benjamin
 * 
 */
public final class BTreeFile<K extends Comparable<K> & Serializable, V extends Serializable> 
implements Monitorable, Closeable {

    /**
     * The B+Tree name.
     */
    private final String name;
    
    /**
     * The B+Tree in which are stored the entries.
     */
    private final BTree<K, V> btree;

    /**
     * The B+Tree node manager.
     */
    private final OnDiskNodeManager<K, V> nodeManager;

    public BTreeFile(String name,
                     Path file, 
                     int branchingFactor, 
                     Parser<K> keyParser, 
                     Parser<V> valueParser) 
                             throws IOException {
        
        notEmpty(name, "the name parameter must not be empty.");
        notNull(file, "the file parameter must not be null.");
        notNull(keyParser, "the keyParser parameter must not be null.");
        notNull(valueParser, "the valueParser parameter must not be null.");
        
        this.name = name;
        this.nodeManager = new OnDiskNodeManager<>(MetricRegistry.name(this.getClass(), "bTree"),
                                                   file,
                                                   new GenericNodeWriterFactory<K, V>(),
                                                   new GenericNodeReaderFactory<K, V>(keyParser, valueParser));

        this.btree = new BTree<>(this.nodeManager, branchingFactor);
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
        this.nodeManager.register(registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {
        this.nodeManager.unregister(registry);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

        this.nodeManager.close();
    }

    public boolean insertIfAbsent(K key, V value) throws IOException {

        return this.btree.insertIfAbsent(key, value);
    }

    public V get(K key) throws IOException {

        return this.btree.get(key);
    }

    /**
     * The <code>NodeWriter</code> factory for <code></code>.
     */
    public static final class GenericNodeWriterFactory<K extends Comparable<K> & Serializable, V extends Serializable> 
    implements NodeWriterFactory<K, V> {

        /**
         * {@inheritDoc}
         */
        @Override
        public GenericNodeWriter<K, V> newWriter(FileDataOutput output) throws IOException {
            return new GenericNodeWriter<K, V>(output);
        }
    };
    
    /**
     * Generic <code>NodeWriter</code> for the <code>Node</code>s with <code>Serializable</code> key and value.
     */
    private static final class GenericNodeWriter<K extends Comparable<K> & Serializable, V extends Serializable> 
    extends AbstractNodeWriter<K, V> {

        /**
         * Creates a new <code>GenericNodeWriter</code> that write to the specified output.
         * 
         * @param output the output used by the writer.
         * @throws IOException if an I/O problem occurs.
         */
        public GenericNodeWriter(FileDataOutput output) throws IOException {
            super(output);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected int computeKeySize(K key) throws IOException {
            return key.computeSerializedSize();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected int computeValueSize(V value) throws IOException {
            return value.computeSerializedSize();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void writeKey(ByteWriter writer, K key) throws IOException {
            writer.writeObject(key);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void writeValue(ByteWriter writer, V value) throws IOException {
            writer.writeObject(value);
        }
    }


    /**
     * The <code>NodeReader</code> factory.
     */
    public static final class GenericNodeReaderFactory<K extends Comparable<K> & Serializable, V extends Serializable>
    implements NodeReaderFactory<K, V> {

        /**
         * The key parser
         */
        private final Parser<K> keyParser;
        
        /**
         * The value parser
         */
        private final Parser<V> valueParser;
        
        /**
         * Creates a new <code>GenericNodeReaderFactory</code> that creates <code>GenericNodeReader</code>s instances.
         * 
         * @param keyParser the key parser
         * @param valueParser the value parser
         */
        public GenericNodeReaderFactory(Parser<K> keyParser, 
                                        Parser<V> valueParser) {
            this.keyParser = keyParser;
            this.valueParser = valueParser;
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public NodeReader<K, V> newReader(SeekableFileDataInput input) throws IOException {
            return new GenericNodeReader<K, V>(input, this.keyParser, this.valueParser);
        }
    };
    
    /**
     * Generic <code>NodeReader</code> for the <code>Node</code>s with <code>Serializable</code> key and value.
     * 
     */
    private static final class GenericNodeReader<K extends Comparable<K> & Serializable, V extends Serializable>  
    extends AbstractNodeReader<K, V>  {

        /**
         * The key parser
         */
        private final Parser<K> keyParser;
        
        /**
         * The value parser
         */
        private final Parser<V> valueParser;
        
        /**
         * Creates a new <code>GenericNodeReader</code> that read from the specified input.
         * 
         * @param input the input used by the reader.
         * @param keyParser the key parser
         * @param valueParser the value parser
         * @throws IOException if an I/O problem occurs.
         */
        public GenericNodeReader(SeekableFileDataInput input, 
                                 Parser<K> keyParser, 
                                 Parser<V> valueParser) 
                                         throws IOException {
            super(input);
            this.keyParser = keyParser;
            this.valueParser = valueParser;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected V readValue(ByteReader reader) throws IOException {
            return this.valueParser.parseFrom(reader);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected K readKey(ByteReader reader) throws IOException {
            return this.keyParser.parseFrom(reader);
        }
    }
}
