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
package io.horizondb.db.cache;

import io.horizondb.db.AbstractComponent;
import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.metrics.CacheMetrics;
import io.horizondb.db.metrics.PrefixFilter;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;

/**
 * Base class for <code>Cache</code> implementation with multilevel.
 * 
 * @author Benjamin
 * 
 */
public abstract class AbstractMultilevelCache<K, V> 
extends AbstractComponent 
implements io.horizondb.db.cache.Cache<K, V> {

    /**
     * The database configuration.
     */
    private final Configuration configuration;

    /**
     * The second level cache.
     */
    private final io.horizondb.db.cache.Cache<K, V> secondLevelCache;
    
    /**
     * The actual cache.
     */
    private Cache<K, V> cache;

    /**
     * Creates a <code>AbstractCache</code>.
     * 
     * @param configuration the database configuration.
     * @param secondLevelCache the second level cache.
     */
    public AbstractMultilevelCache(Configuration configuration, 
                                   io.horizondb.db.cache.Cache<K, V> secondLevelCache) {

        this.configuration = configuration;
        this.secondLevelCache = secondLevelCache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        this.cache = newBuilder(this.configuration).build();
    }

    /**
     * Creates a new builder for 
     * @return
     */
    protected abstract CacheBuilder<K, V> newBuilder(Configuration configuration);

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {
        registry.registerAll(new CacheMetrics(getName(), this.cache));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {
        registry.removeMatching(new PrefixFilter(getName()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(final K key, final ValueLoader<K, V> loader) throws IOException, HorizonDBException {

        checkRunning();
        
        try {

            return this.cache.get(key, new Callable<V>() {
                
                /**
                 * {@inheritDoc}
                 */
                @Override
                public V call() throws Exception {
                    return AbstractMultilevelCache.this.secondLevelCache.get(key, loader);
                }
            });

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
