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
import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.files.FileDataOutput;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.model.ErrorCodes;
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
public final class DefaultTimeSeriesManager extends AbstractComponent implements TimeSeriesManager {

    /**
     * The B+Tree branching factor.
     */
    private static final int BRANCHING_FACTOR = 100;

    /**
     * The name of the time series file.
     */
    private static final String TIMESERIES_FILENAME = "timeseries.b3";

    /**
     * The Database server configuration.
     */
    private final Configuration configuration;

    /**
     * The time series partition manager
     */
    private final TimeSeriesPartitionManager partitionManager;

    /**
     * The B+Tree in which are stored the time series meta data.
     */
    private BTree<TimeSeriesId, TimeSeriesDefinition> btree;

    /**
     * The B+Tree node manager.
     */
    private OnDiskNodeManager<TimeSeriesId, TimeSeriesDefinition> nodeManager;

    /**
     * Creates a new <code>DefaultTimeSeriesManager</code> that will used the specified configuration.
     * 
     * @param partitionManager the partition manager
     * @param configuration the database configuration
     */
    public DefaultTimeSeriesManager(TimeSeriesPartitionManager partitionManager, Configuration configuration) {

        notNull(partitionManager, "the partitionManager parameter must not be null.");
        notNull(configuration, "the configuration parameter must not be null.");

        this.partitionManager = partitionManager;
        this.configuration = configuration;
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

        Path timeSeriesFile = systemDirectory.resolve(TIMESERIES_FILENAME);

        this.nodeManager = OnDiskNodeManager.newBuilder(MetricRegistry.name(getName(), "bTree"),
                                                        timeSeriesFile,
                                                        TimeSeriesDefinitionNodeWriter.FACTORY,
                                                        TimeSeriesDefinitionNodeReader.FACTORY).build();

        this.btree = new BTree<>(this.nodeManager, BRANCHING_FACTOR);

        this.partitionManager.start();
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
    protected void doShutdown() throws InterruptedException {

        this.partitionManager.shutdown();
        this.nodeManager.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createTimeSeries(TimeSeriesDefinition definition, boolean throwExceptionIfExists) throws IOException,
                                                                                                 HorizonDBException {

        TimeSeriesId id = new TimeSeriesId(definition.getDatabaseName(), definition.getSeriesName());

        Names.checkTimeSeriesName(id.getSeriesName());

        if (!this.btree.insertIfAbsent(id, definition) && throwExceptionIfExists) {

            throw new HorizonDBException(ErrorCodes.DUPLICATE_TIMESERIES, "Duplicate time series name "
                    + definition.getSeriesName() + " in database " + definition.getDatabaseName());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeries getTimeSeries(String databaseName, String seriesName) throws IOException, HorizonDBException {

        return getTimeSeries(new TimeSeriesId(databaseName, seriesName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeries getTimeSeries(TimeSeriesId id) throws IOException, HorizonDBException {

        TimeSeriesDefinition definition = this.btree.get(id);

        if (definition == null) {
            throw new HorizonDBException(ErrorCodes.UNKNOWN_TIMESERIES, "The time series " + id.getSeriesName()
                    + " does not exists within the database " + id.getDatabaseName() + ".");
        }

        return new TimeSeries(this.partitionManager, definition);
    }

    /**
     * <code>NodeWriter</code> for the <code>TimeSeriesDefinition</code>.
     * 
     */
    private static class TimeSeriesDefinitionNodeWriter extends AbstractNodeWriter<TimeSeriesId, TimeSeriesDefinition> {

        /**
         * The <code>NodeWriter</code> factory.
         */
        public static final NodeWriterFactory<TimeSeriesId, TimeSeriesDefinition> FACTORY = new NodeWriterFactory<TimeSeriesId, TimeSeriesDefinition>() {

            /**
             * {@inheritDoc}
             */
            @Override
            public NodeWriter<TimeSeriesId, TimeSeriesDefinition> newWriter(FileDataOutput output) throws IOException {
                return new TimeSeriesDefinitionNodeWriter(output);
            }
        };

        /**
         * Creates a new <code>DatabaseMetaDataNodeWriter</code> that write to the specified output.
         * 
         * @param output the output used by the writer.
         * @throws IOException if an I/O problem occurs.
         */
        public TimeSeriesDefinitionNodeWriter(FileDataOutput output) throws IOException {
            super(output);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected int computeKeySize(TimeSeriesId id) {
            return id.computeSerializedSize();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected int computeValueSize(TimeSeriesDefinition metadata) {
            return metadata.computeSerializedSize();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void writeKey(ByteWriter writer, TimeSeriesId id) throws IOException {
            writer.writeObject(id);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void writeValue(ByteWriter writer, TimeSeriesDefinition metadata) throws IOException {
            writer.writeObject(metadata);
        }
    }

    /**
     * <code>NodeReader</code> for the <code>TimeSeriesDefinition</code>.
     * 
     */
    private static final class TimeSeriesDefinitionNodeReader extends
            AbstractNodeReader<TimeSeriesId, TimeSeriesDefinition> {

        /**
         * The <code>NodeReader</code> factory.
         */
        public static final NodeReaderFactory<TimeSeriesId, TimeSeriesDefinition> FACTORY = new NodeReaderFactory<TimeSeriesId, TimeSeriesDefinition>() {

            /**
             * {@inheritDoc}
             */
            @Override
            public NodeReader<TimeSeriesId, TimeSeriesDefinition>
                    newReader(SeekableFileDataInput input) throws IOException {
                return new TimeSeriesDefinitionNodeReader(input);
            }
        };

        /**
         * Creates a new <code>TimeSeriesDefinitionNodeReader</code> that read from the specified input.
         * 
         * @param input the input used by the reader.
         * @throws IOException if an I/O problem occurs.
         */
        public TimeSeriesDefinitionNodeReader(SeekableFileDataInput input) throws IOException {
            super(input);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected TimeSeriesDefinition readValue(ByteReader reader) throws IOException {
            return TimeSeriesDefinition.parseFrom(reader);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected TimeSeriesId readKey(ByteReader reader) throws IOException {
            return TimeSeriesId.parseFrom(reader);
        }
    }
}
