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
import io.horizondb.db.btree.BTreeStore;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import com.codahale.metrics.MetricRegistry;

import static org.apache.commons.lang.Validate.notNull;

/**
 */
abstract class AbstractTimeSeriesManager extends AbstractComponent implements TimeSeriesManager {

    /**
     * The B+Tree branching factor.
     */
    private static final int BRANCHING_FACTOR = 100;

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
    private BTreeStore<TimeSeriesId, TimeSeriesDefinition> btree;

    /**
     * Creates a new <code>AbstractTimeSeriesManager</code> that will used the specified configuration.
     * 
     * @param partitionManager the partition manager
     * @param configuration the database configuration
     */
    public AbstractTimeSeriesManager(TimeSeriesPartitionManager partitionManager, Configuration configuration) {

        notNull(partitionManager, "the partitionManager parameter must not be null.");
        notNull(configuration, "the configuration parameter must not be null.");

        this.partitionManager = partitionManager;
        this.configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void doStart() throws IOException, InterruptedException {

        this.btree = createBTreeStore(this.configuration, BRANCHING_FACTOR);
        this.partitionManager.start();
    }

    /**
     * Creates the B+Tree used to store the time series definitions.
     * 
     * @param configuration the database configuration
     * @param branchingFactor the B+Tree branching factor
     * @throws IOException if an I/O problem occurs while creating the B+Tree
     */
    protected abstract BTreeStore<TimeSeriesId, TimeSeriesDefinition> createBTreeStore(Configuration configuration,
                                                                                       int branchingFactor) 
                                                                                       throws IOException;

    /**
     * {@inheritDoc}
     */
    @Override
    public final void register(MetricRegistry registry) {
        register(registry, this.btree, this.partitionManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void unregister(MetricRegistry registry) {
        unregister(registry, this.partitionManager, this.btree);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void doShutdown() throws InterruptedException {

        this.partitionManager.shutdown();
        this.btree.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void createTimeSeries(String databaseName,
                                       TimeSeriesDefinition definition, 
                                       boolean throwExceptionIfExists) 
                                       throws IOException, HorizonDBException {

        TimeSeriesId id = new TimeSeriesId(databaseName, definition.getName());

        Names.checkTimeSeriesName(id.getSeriesName());

        if (!this.btree.insertIfAbsent(id, definition) && throwExceptionIfExists) {

            throw new HorizonDBException(ErrorCodes.DUPLICATE_TIMESERIES, "Duplicate time series name "
                    + definition.getName() + " in database " + databaseName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final TimeSeries getTimeSeries(String databaseName, 
                                          String seriesName) 
                                          throws IOException, HorizonDBException {

        return getTimeSeries(new TimeSeriesId(databaseName, seriesName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final TimeSeries getTimeSeries(TimeSeriesId id) throws IOException, HorizonDBException {

        TimeSeriesDefinition definition = this.btree.get(id);

        if (definition == null) {
            throw new HorizonDBException(ErrorCodes.UNKNOWN_TIMESERIES, "The time series " + id.getSeriesName()
                    + " does not exists within the database " + id.getDatabaseName() + ".");
        }

        return new TimeSeries(id.getDatabaseName(), definition, this.partitionManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final TimeSeriesPartitionManager getPartitionManager() {
        return this.partitionManager;
    }
}
