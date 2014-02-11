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
import io.horizondb.db.btree.BTreeFile;
import io.horizondb.db.commitlog.CommitLog;
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ListenableFuture;

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
    private BTreeFile<TimeSeriesId, TimeSeriesDefinition> btree;

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

        this.btree = new BTreeFile<>(MetricRegistry.name(getClass(), "BTree"),
                                     timeSeriesFile, 
                                     BRANCHING_FACTOR, 
                                     TimeSeriesId.getParser(), 
                                     TimeSeriesDefinition.getParser());

        this.partitionManager.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {
        register(registry, this.btree, this.partitionManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {
        unregister(registry, this.partitionManager, this.btree);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

        this.partitionManager.shutdown();
        this.btree.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createTimeSeries(String databaseName,
                                 TimeSeriesDefinition definition, 
                                 ListenableFuture<ReplayPosition> future, 
                                 boolean throwExceptionIfExists) 
                                         throws IOException, 
                                                HorizonDBException {

        TimeSeriesId id = new TimeSeriesId(databaseName, definition.getName());

        Names.checkTimeSeriesName(id.getSeriesName());

        if (!this.btree.insertIfAbsent(id, definition) && throwExceptionIfExists) {

            throw new HorizonDBException(ErrorCodes.DUPLICATE_TIMESERIES, "Duplicate time series name "
                    + definition.getName() + " in database " + databaseName);
        }
        
        CommitLog.waitForCommitLogWriteIfNeeded(this.configuration, future);
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

        return new TimeSeries(id.getDatabaseName(), this.partitionManager, definition);
    }
}
