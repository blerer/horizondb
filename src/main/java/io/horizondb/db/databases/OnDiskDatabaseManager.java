/**
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

import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBFiles;
import io.horizondb.db.btree.AbstractNodeReader;
import io.horizondb.db.btree.AbstractNodeWriter;
import io.horizondb.db.btree.NodeManager;
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
import io.horizondb.io.files.FileUtils;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.model.schema.DatabaseDefinition;

import java.io.IOException;
import java.nio.file.Path;

import com.codahale.metrics.MetricRegistry;

import static io.horizondb.io.files.FileUtils.createDirectoriesIfNeeded;

/**
 * <code>DatabaseManager</code> that store all data on disk.
 */
public final class OnDiskDatabaseManager extends AbstractDatabaseManager {

    /**
     * Creates a new <code>InMemoryDatabaseManager</code> that will used the specified configuration.
     * 
     * @param configuration the database configuration
     * @param timeSeriesManager the time series manager
     */
    public OnDiskDatabaseManager(Configuration configuration, TimeSeriesManager timeSeriesManager) {
        super(configuration, timeSeriesManager);
    }
    
    /**
     * {@inheritDoc} 
     */
    @Override
    protected NodeManager<String, DatabaseDefinition> createNodeManager(Configuration configuration, 
                                                                        String name) 
                                                                        throws IOException {

        createDirectoriesIfNeeded(HorizonDBFiles.getSystemDirectory(configuration));

        Path databasesFile = HorizonDBFiles.getDatabasesFile(configuration);

        return new OnDiskNodeManager<>(MetricRegistry.name(name, "bTree"),
                                       databasesFile,
                                       DatabaseDefinitionNodeWriter.FACTORY,
                                       DatabaseDefinitionNodeReader.FACTORY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterCreate(DatabaseDefinition definition) throws IOException {

        Path directory = HorizonDBFiles.getDatabaseDirectory(this.configuration, definition);
        FileUtils.createDirectoriesIfNeeded(directory);
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
