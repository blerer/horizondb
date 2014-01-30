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
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.db.metrics.CacheMetrics;
import io.horizondb.db.metrics.PrefixFilter;
import io.horizondb.model.schema.DatabaseDefinition;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Decorator that add caching functionalities to a <code>DatabaseManager</code>
 * 
 * @author Benjamin
 * 
 */
public final class DatabaseManagerCache extends AbstractComponent implements DatabaseManager {

    /**
     * The decorated database manager.
     */
    private final DatabaseManager manager;

    /**
     * The database configuration.
     */
    private final Configuration configuration;

    /**
     * The database cache.
     */
    private LoadingCache<String, Database> cache;

    /**
     * Creates a <code>TimeSeriesManagerCache</code> to cache the databases returned by the specified manager.
     * 
     * @param configuration the database configuration.
     * @param manager the manager to decorate.
     */
    public DatabaseManagerCache(Configuration configuration, DatabaseManager manager) {

        this.configuration = configuration;
        this.manager = manager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        this.manager.start();

        this.cache = CacheBuilder.newBuilder()
                                 .maximumSize(this.configuration.getDatabaseCacheMaximumSize())
                                 .recordStats()
                                 .build(new CacheLoader<String, Database>() {

                                     @Override
                                     public Database load(String name) throws Exception {

                                         return DatabaseManagerCache.this.manager.getDatabase(name);
                                     }
                                 });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {
        this.manager.register(registry);
        registry.registerAll(new CacheMetrics(getName(), this.cache));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {
        this.manager.unregister(registry);
        registry.removeMatching(new PrefixFilter(getName()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {
        this.manager.shutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createDatabase(DatabaseDefinition definition, 
                               ListenableFuture<ReplayPosition> future, 
                               boolean throwExceptionIfExists) 
                                       throws IOException, HorizonDBException {
        
        this.manager.createDatabase(definition, future, throwExceptionIfExists);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Database getDatabase(String name) throws IOException, HorizonDBException {

        String lowerCaseName = name.toLowerCase();

        try {

            return this.cache.get(lowerCaseName);

        } catch (ExecutionException e) {

            Throwable cause = e.getCause();

            if (cause instanceof HorizonDBException) {

                throw (HorizonDBException) cause;
            }

            throw new IOException(cause);
        }
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
