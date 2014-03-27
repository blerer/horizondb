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
import io.horizondb.db.cache.ValueLoader;
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheStats;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Decorator that add caching functionalities to a <code>TimeSeriesManager</code>
 * 
 * @author Benjamin
 * 
 */
public final class TimeSeriesManagerCache extends AbstractComponent implements TimeSeriesManager {

    /**
     * The decorated time series manager.
     */
    private final TimeSeriesManager manager;

    /**
     * The time series cache.
     */
    private TimeSeriesCache cache;

    /**
     * Creates a <code>TimeSeriesManagerCache</code> to cache the time series returned by the specified manager.
     * 
     * @param configuration the database configuration.
     * @param manager the manager to decorate.
     */
    public TimeSeriesManagerCache(Configuration configuration, TimeSeriesManager manager) {

        this.manager = manager;
        this.cache = new TimeSeriesCache(configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        start(this.manager, this.cache);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {
        
        register(registry, this.manager, this.cache);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {
        
        unregister(registry, this.cache, this.manager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {
        shutdown(this.cache, this.manager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createTimeSeries(String databaseName, 
                                 TimeSeriesDefinition definition, 
                                 ListenableFuture<ReplayPosition> future, 
                                 boolean throwExceptionIfExists) 
                                         throws IOException, HorizonDBException {
        
        this.manager.createTimeSeries(databaseName, definition, future, throwExceptionIfExists);
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

        return this.cache.get(id, new ValueLoader<TimeSeriesId, TimeSeries>() {

            @Override
            public TimeSeries loadValue(TimeSeriesId key) throws IOException, HorizonDBException {
                return TimeSeriesManagerCache.this.manager.getTimeSeries(key);
            }
        });
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesPartitionManager getPartitionManager() {
        return this.manager.getPartitionManager();
    }

    /**
     * Returns the cache statistics.
     * 
     * @return the cache statistics.
     */
    CacheStats stats() {

        return this.cache.stats();
    }

    /**
     * Returns the cache size.
     * 
     * @return the cache size.
     */
    long size() {
        return this.cache.size();
    }
}
