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
package io.horizondb.db.databases;

import io.horizondb.db.AbstractComponent;
import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.Names;
import io.horizondb.db.btree.AbstractNodeReader;
import io.horizondb.db.btree.AbstractNodeWriter;
import io.horizondb.db.btree.BTree;
import io.horizondb.db.btree.NodeReader;
import io.horizondb.db.btree.NodeReaderFactory;
import io.horizondb.db.btree.NodeWriter;
import io.horizondb.db.btree.NodeWriterFactory;
import io.horizondb.db.btree.OnDiskNodeManager;
import io.horizondb.db.series.TimeSeriesManager;
import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.files.FileDataOutput;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.model.DatabaseDefinition;
import io.horizondb.model.ErrorCodes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.codahale.metrics.MetricRegistry;

import static org.apache.commons.lang.Validate.notNull;

/**
 * The default database manager.
 * 
 * @author Benjamin
 * 
 */
public final class DefaultDatabaseManager extends AbstractComponent implements DatabaseManager {

    /**
     * The B+Tree branching factor.
     */
    private static final int BRANCHING_FACTOR = 10;

    /**
     * The name of the databases file.
     */
    private static final String DATABASES_FILENAME = "databases.b3";

    /**
     * The database server configuration.
     */
    private final Configuration configuration;

    /**
     * The time series manager.
     */
    private final TimeSeriesManager timeSeriesManager;

    /**
     * The B+Tree in which are stored the databases meta data.
     */
    private BTree<String, DatabaseDefinition> btree;

    /**
     * The B+Tree node manager.
     */
    private OnDiskNodeManager<String, DatabaseDefinition> nodeManager;

    /**
     * Creates a new <code>DefaultDatabaseManager</code> that will used the specified configuration.
     * 
     * @param configuration the database configuration
     */
    public DefaultDatabaseManager(Configuration configuration, TimeSeriesManager timeSeriesManager) {

        notNull(configuration, "the configuration parameter must not be null.");

        this.configuration = configuration;
        this.timeSeriesManager = timeSeriesManager;
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
    protected void doStart() throws IOException, InterruptedException {

        Path dataDirectory = this.configuration.getDataDirectory();
        Path systemDirectory = dataDirectory.resolve("system");

        if (!Files.exists(systemDirectory)) {
            Files.createDirectories(systemDirectory);
        }

        Path databasesFile = systemDirectory.resolve(DATABASES_FILENAME);

        this.nodeManager = OnDiskNodeManager.newBuilder(MetricRegistry.name(getName(), "bTree"),
                                                        databasesFile,
                                                        DatabaseDefinitionNodeWriter.FACTORY,
                                                        DatabaseDefinitionNodeReader.FACTORY).build();

        this.btree = new BTree<>(this.nodeManager, BRANCHING_FACTOR);

        this.timeSeriesManager.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

        this.timeSeriesManager.shutdown();

        this.nodeManager.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createDatabase(DatabaseDefinition definition, boolean throwExceptionIfExists) throws IOException, HorizonDBException {

        String name = definition.getName();
        String lowerCaseName = name.toLowerCase();

        Names.checkDatabaseName(name);

        if (!this.btree.insertIfAbsent(lowerCaseName, definition) && throwExceptionIfExists) {

            throw new HorizonDBException(ErrorCodes.DUPLICATE_DATABASE, "Duplicate database name " + name);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Database getDatabase(String name) throws IOException, HorizonDBException {

        String lowerCaseName = name.toLowerCase();

        DatabaseDefinition definition = this.btree.get(lowerCaseName);

        if (definition == null) {
            throw new HorizonDBException(ErrorCodes.UNKNOWN_DATABASE, "The database " + name + " does not exists.");
        }

        return new Database(this.configuration, definition, this.timeSeriesManager);
    }

    /**
     * <code>NodeWriter</code> for the <code>DatabaseDefinition</code>.
     * 
     */
    private static class DatabaseDefinitionNodeWriter extends AbstractNodeWriter<String, DatabaseDefinition> {

        /**
         * The <code>NodeWriter</code> factory.
         */
        public static final NodeWriterFactory<String, DatabaseDefinition> FACTORY = new NodeWriterFactory<String, DatabaseDefinition>() {

            /**
             * {@inheritDoc}
             */
            @Override
            public NodeWriter<String, DatabaseDefinition> newWriter(FileDataOutput output) throws IOException {
                return new DatabaseDefinitionNodeWriter(output);
            }
        };

        /**
         * Creates a new <code>DatabaseDefinitionNodeWriter</code> that write to the specified output.
         * 
         * @param output the output used by the writer.
         * @throws IOException if an I/O problem occurs.
         */
        public DatabaseDefinitionNodeWriter(FileDataOutput output) throws IOException {
            super(output);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected int computeKeySize(String name) {
            return VarInts.computeStringSize(name);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected int computeValueSize(DatabaseDefinition definition) {
            return definition.computeSerializedSize();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void writeKey(ByteWriter writer, String name) throws IOException {
            VarInts.writeString(writer, name);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void writeValue(ByteWriter writer, DatabaseDefinition definition) throws IOException {
            writer.writeObject(definition);
        }
    }

    /**
     * <code>NodeReader</code> for the <code>DatabaseDefinition</code>.
     * 
     */
    private static final class DatabaseDefinitionNodeReader extends AbstractNodeReader<String, DatabaseDefinition> {

        /**
         * The <code>NodeReader</code> factory.
         */
        public static final NodeReaderFactory<String, DatabaseDefinition> FACTORY = new NodeReaderFactory<String, DatabaseDefinition>() {

            /**
             * {@inheritDoc}
             */
            @Override
            public NodeReader<String, DatabaseDefinition> newReader(SeekableFileDataInput input) throws IOException {
                return new DatabaseDefinitionNodeReader(input);
            }
        };

        /**
         * Creates a new <code>DatabaseDefinitionNodeReader</code> that read from the specified input.
         * 
         * @param input the input used by the reader.
         * @throws IOException if an I/O problem occurs.
         */
        public DatabaseDefinitionNodeReader(SeekableFileDataInput input) throws IOException {
            super(input);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected DatabaseDefinition readValue(ByteReader reader) throws IOException {
            return DatabaseDefinition.parseFrom(reader);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected String readKey(ByteReader reader) throws IOException {
            return VarInts.readString(reader);
        }
    }
}
