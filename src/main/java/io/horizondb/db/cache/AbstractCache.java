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
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

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
    public final void register(MetricRegistry registry) {
        registry.registerAll(new CacheMetrics(getName(), this.cache));
        onRegister(registry);
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
    public void put(K key, V value) {
        this.cache.put(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final V get(K key, ValueLoader<K, V> loader) throws IOException, HorizonDBException {

        checkRunning();
        
        try {

            final V value = doGet(key, loader);
            
            afterLoad(value);
            
            return value;

        } catch (ExecutionException e) {

            Throwable cause = e.getCause();

            if (cause instanceof HorizonDBException) {

                throw (HorizonDBException) cause;
            }

            throw new IOException(cause);
        }
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public V getIfPresent(K key) {
        return this.cache.getIfPresent(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void invalidate(K key) {
        this.cache.invalidate(key);
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
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("cache", this.cache.asMap())
                                                                          .toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        this.cache = newBuilder(this.configuration).build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() {
        this.cache.invalidateAll();
    }
    
    /**
     * Creates a new builder for the Guava Cache used internally.
     * @return a new builder for the Guava Cache used internally.
     */
    protected abstract CacheBuilder<? super K, ? super V> newBuilder(Configuration configuration);
    
    /**
     * Performs the get operation.
     * 
     * @param key the value key
     * @param loader the loader that will be used to load the value if the key is not present within the cache
     * @return the value associated to the specified key
     * @throws ExecutionException if a problem occurs while retrieving the value
     */
    protected V doGet(K key, ValueLoader<K, V> loader) throws ExecutionException {
        
        return doGet(key, new CallableAdaptor<>(key, loader));
    }
    
    /**
     * Performs the get operation.
     * 
     * @param key the value key
     * @param callable the <code>Callable</code> that will be used to load the value if the key is not present within the cache
     * @return the value associated to the specified key
     * @throws ExecutionException if a problem occurs while retrieving the value
     */
    protected final V doGet(K key, Callable<V> callable) throws ExecutionException {
        
        return this.cache.get(key, callable);
    }  

    /**
     * Allows a sub-classes to perform operations on a loaded value before returning it.
     *  
     * @param value the loaded value
     */
    protected void afterLoad(V value) {

    }
    
    /**
     * Allows sub-classes to register more meters if they need to.
     * @param registry the <code>MetricRegistry</code>
     */
    protected void onRegister(MetricRegistry registry) {

    }
}
