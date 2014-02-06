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
import java.util.concurrent.ExecutionException;

import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;

/**
 * Base class for <code>Cache</code> implementation.
 * 
 * @author Benjamin
 * 
 */
public abstract class AbstractCache<K, V> extends AbstractComponent implements io.horizondb.db.cache.Cache<K, V> {

    /**
     * The database configuration.
     */
    private final Configuration configuration;

    /**
     * The actual cache.
     */
    private Cache<K, V> cache;

    /**
     * Creates a <code>AbstractCache</code>.
     * 
     * @param configuration the database configuration.
     */
    public AbstractCache(Configuration configuration) {

        this.configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        this.cache = newBuilder(this.configuration).build();
    }

    /**
     * Creates a new builder for the Guava Cache used internally.
     * @return a new builder for the Guava Cache used internally.
     */
    protected abstract CacheBuilder<Object, Object> newBuilder(Configuration configuration);

    /**
     * {@inheritDoc}
     */
    @Override
    public final void register(MetricRegistry registry) {
        registry.registerAll(new CacheMetrics(getName(), this.cache));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void unregister(MetricRegistry registry) {
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
    public final V get(K key, ValueLoader<K, V> loader) throws IOException, HorizonDBException {

        checkRunning();
        
        try {

            return this.cache.get(key, new CallableAdaptor<>(key, loader));

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
    public final CacheStats stats() {

        return this.cache.stats();
    }

    /**
     * Returns the cache size.
     * 
     * @return the cache size.
     */
    public final long size() {
        return this.cache.size();
    }
}
