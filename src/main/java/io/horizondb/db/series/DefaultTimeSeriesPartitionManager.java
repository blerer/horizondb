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
package io.horizondb.db.series;

import io.horizondb.db.AbstractComponent;
import io.horizondb.db.Configuration;
import io.horizondb.db.btree.AbstractNodeReader;
import io.horizondb.db.btree.AbstractNodeWriter;
import io.horizondb.db.btree.BTree;
import io.horizondb.db.btree.NodeReader;
import io.horizondb.db.btree.NodeReaderFactory;
import io.horizondb.db.btree.NodeWriter;
import io.horizondb.db.btree.NodeWriterFactory;
import io.horizondb.db.btree.OnDiskNodeManager;
import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.files.FileDataOutput;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.model.PartitionId;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.codahale.metrics.MetricRegistry;

import static org.apache.commons.lang.Validate.notNull;

/**
 * @author Benjamin
 * 
 */
public final class DefaultTimeSeriesPartitionManager extends AbstractComponent implements TimeSeriesPartitionManager {

    /**
     * The B+Tree branching factor.
     */
    private static final int BRANCHING_FACTOR = 100;

    /**
     * The name of the partition file.
     */
    private static final String PARTITIONS_FILENAME = "partitions.b3";

    /**
     * The Database server configuration.
     */
    private final Configuration configuration;

    /**
     * The B+Tree in which are stored the partition meta data.
     */
    private BTree<PartitionId, TimeSeriesPartitionMetaData> btree;

    /**
     * The B+Tree node manager.
     */
    private OnDiskNodeManager<PartitionId, TimeSeriesPartitionMetaData> nodeManager;

    /**
     * The flush manager
     */
    private final FlushManager flushManager;

    /**
     * Creates a new <code>DefaultTimeSeriesPartitionManager</code> that will used the specified configuration.
     * 
     * @param configuration the database configuration
     */
    public DefaultTimeSeriesPartitionManager(Configuration configuration) {

        notNull(configuration, "the configuration parameter must not be null.");

        this.configuration = configuration;
        this.flushManager = new FlushManager(configuration);
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

        Path partitionFile = systemDirectory.resolve(PARTITIONS_FILENAME);
        
        this.nodeManager = new OnDiskNodeManager<>(MetricRegistry.name(getName(), "bTree"),
                                                   partitionFile,
                                                   PartitionMetaDataNodeWriter.FACTORY,
                                                   PartitionMetaDataNodeReader.FACTORY);

        this.btree = new BTree<>(this.nodeManager, BRANCHING_FACTOR);

        this.flushManager.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {

        this.nodeManager.register(registry);
        this.flushManager.register(registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {

        this.flushManager.unregister(registry);
        this.nodeManager.unregister(registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void save(TimeSeriesPartition partition) throws IOException {

        this.flushManager.savePartition(partition, this.btree);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesPartition getPartitionForRead(PartitionId partitionId, TimeSeriesDefinition definition) throws IOException {

        return getPartition(partitionId, definition);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesPartition getPartitionForWrite(PartitionId partitionId, 
                                                    TimeSeriesDefinition definition)
                                                    throws IOException {
        
        return getPartition(partitionId, definition);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush(TimeSeriesPartition timeSeriesPartition) {

        checkRunning();
        this.flushManager.flush(timeSeriesPartition);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forceFlush(TimeSeriesPartition timeSeriesPartition) {

        checkRunning();
        this.flushManager.forceFlush(timeSeriesPartition);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

        this.flushManager.shutdown();

        this.nodeManager.close();
    }

    /**
     * Blocks until all the flush tasks previously submitted have been completed.
     * <p>
     * This method is implemented for testing purpose.
     * </p>
     * 
     * @throws Exception if a problem occurs while synchronizing.
     */
    void sync() throws Exception {

        this.flushManager.sync();
    }

    private TimeSeriesPartition
            getPartition(PartitionId partitionId, TimeSeriesDefinition definition) throws IOException {

        TimeSeriesPartitionMetaData metadata = this.btree.get(partitionId);

        if (metadata == null) {

            metadata = TimeSeriesPartitionMetaData.newBuilder(definition.getPartitionTimeRange(partitionId.getId()))
                                                  .build();
        }

        return new TimeSeriesPartition(this, this.configuration, partitionId.getDatabaseName(), definition, metadata);
    }

    /**
     * <code>NodeWriter</code> for the <code>TimeSeriesPartitionMetaData</code>.
     * 
     */
    private static class PartitionMetaDataNodeWriter extends
            AbstractNodeWriter<PartitionId, TimeSeriesPartitionMetaData> {

        /**
         * The <code>NodeWriter</code> factory.
         */
        public static final NodeWriterFactory<PartitionId, TimeSeriesPartitionMetaData> FACTORY = new NodeWriterFactory<PartitionId, TimeSeriesPartitionMetaData>() {

            /**
             * {@inheritDoc}
             */
            @Override
            public NodeWriter<PartitionId, TimeSeriesPartitionMetaData>
                    newWriter(FileDataOutput output) throws IOException {
                return new PartitionMetaDataNodeWriter(output);
            }
        };

        /**
         * Creates a new <code>PartitionMetaDataNodeWriter</code> that write to the specified output.
         * 
         * @param output the output used by the writer.
         * @throws IOException if an I/O problem occurs.
         */
        public PartitionMetaDataNodeWriter(FileDataOutput output) throws IOException {
            super(output);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected int computeKeySize(PartitionId id) {
            return id.computeSerializedSize();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected int computeValueSize(TimeSeriesPartitionMetaData metadata) {
            return metadata.computeSerializedSize();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void writeKey(ByteWriter writer, PartitionId id) throws IOException {
            writer.writeObject(id);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void writeValue(ByteWriter writer, TimeSeriesPartitionMetaData metadata) throws IOException {
            writer.writeObject(metadata);
        }
    }

    /**
     * <code>NodeReader</code> for the <code>TimeSeriesPartitionMetaData</code>.
     * 
     */
    private static final class PartitionMetaDataNodeReader extends
            AbstractNodeReader<PartitionId, TimeSeriesPartitionMetaData> {

        /**
         * The <code>NodeReader</code> factory.
         */
        public static final NodeReaderFactory<PartitionId, TimeSeriesPartitionMetaData> FACTORY = new NodeReaderFactory<PartitionId, TimeSeriesPartitionMetaData>() {

            /**
             * {@inheritDoc}
             */
            @Override
            public NodeReader<PartitionId, TimeSeriesPartitionMetaData>
                    newReader(SeekableFileDataInput input) throws IOException {
                return new PartitionMetaDataNodeReader(input);
            }
        };

        /**
         * Creates a new <code>PartitionMetaDataNodeReader</code> that read from the specified input.
         * 
         * @param input the input used by the reader.
         * @throws IOException if an I/O problem occurs.
         */
        public PartitionMetaDataNodeReader(SeekableFileDataInput input) throws IOException {
            super(input);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected TimeSeriesPartitionMetaData readValue(ByteReader reader) throws IOException {
            return TimeSeriesPartitionMetaData.parseFrom(reader);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected PartitionId readKey(ByteReader reader) throws IOException {
            return PartitionId.parseFrom(reader);
        }
    }
}
