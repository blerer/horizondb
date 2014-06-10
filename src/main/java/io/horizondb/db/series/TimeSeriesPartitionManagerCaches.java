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
import io.horizondb.db.btree.KeyValueIterator;
import io.horizondb.db.cache.ValueLoader;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.annotation.concurrent.ThreadSafe;

import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheStats;

/**
 * Decorator that add caching functionalities to a <code>TimeSeriesPartitionManager</code>
 * 
 * @author Benjamin
 */
@ThreadSafe
public final class TimeSeriesPartitionManagerCaches extends AbstractComponent implements TimeSeriesPartitionManager {

    /**
     * The decorated partition manager.
     */
    private final TimeSeriesPartitionManager manager;

    /**
     * The cache used when performing reads.
     */
    private final TimeSeriesPartitionReadCache readCache;

    /**
     * The cache used when performing writes.
     */
    private final TimeSeriesPartitionWriteCache writeCache;

    /**
     * The global cache.
     */
    private final TimeSeriesPartitionSecondLevelCache globalCache;

    /**
     * Creates a <code>TimeSeriesPartitionManagerCachesTest</code> to globalCache the time series partition returned by
     * the specified manager.
     * 
     * @param configuration the database configuration.
     * @param manager the manager to decorate.
     */
    public TimeSeriesPartitionManagerCaches(Configuration configuration, TimeSeriesPartitionManager manager) {

        this.manager = manager;

        this.globalCache = new TimeSeriesPartitionSecondLevelCache(configuration);
        this.readCache = new TimeSeriesPartitionReadCache(configuration, this.globalCache);
        this.writeCache = new TimeSeriesPartitionWriteCache(configuration, this.globalCache);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        start(this.manager, this.globalCache, this.readCache, this.writeCache);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {

        register(registry, this.manager, this.globalCache, this.readCache, this.writeCache);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {
        
        unregister(registry, this.writeCache, this.readCache, this.globalCache, this.manager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

        shutdown(this.writeCache, this.readCache, this.globalCache, this.manager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void save(TimeSeriesPartition partition) throws IOException {
        this.manager.save(partition);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesPartition getPartitionForWrite(final PartitionId partitionId,
                                                    final TimeSeriesDefinition seriesDefinition) throws IOException, HorizonDBException {

        return this.writeCache.get(partitionId, new ValueLoader<PartitionId, TimeSeriesPartition>() {

            @Override
            public TimeSeriesPartition loadValue(PartitionId key) throws IOException, HorizonDBException {
                // Calling get partition for write or read does not really change anything has the real
                // difference between the 2 methods is only in caching.
                return TimeSeriesPartitionManagerCaches.this.manager.getPartitionForWrite(partitionId, seriesDefinition);
            }
        });
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void forceFlush(long id) throws InterruptedException {
        
        this.logger.info("trying to flush all the partitions that have non persisted data within commit log segment: {}",
                Long.valueOf(id));
        
        List<TimeSeriesPartition> partitions = this.writeCache.getPartitionsWithNonPersistedDataWithin(id);
        
        if (partitions.isEmpty()) {
            
            this.logger.info("no partitions have non persisted data within commit log segment: {}", Long.valueOf(id));
            return;
        }
        
        final CountDownLatch latch = new CountDownLatch(partitions.size());
        
        FlushListener listener = new FlushListener() {
            
            @Override
            public void afterFlush() {
                latch.countDown();                    
            }
        };
        
        for (TimeSeriesPartition partition : partitions) {
            
            forceFlush(id, partition, listener);
        }
        
        latch.await();
                
        this.logger.info("the partitions: {}  had non persisted data within commit log segment {}" 
                         + " and have been flushed to disk", partitions, Long.valueOf(id));
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void forceFlush(long id, TimeSeriesPartition timeSeriesPartition, FlushListener... listeners) {
        this.manager.forceFlush(id, timeSeriesPartition, listeners);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public KeyValueIterator<PartitionId, TimeSeriesPartition> getRangeForRead(PartitionId fromId,
                                                                              PartitionId toId,
                                                                              TimeSeriesDefinition definition) 
                                                                                      throws IOException {
        
        return new TimeSeriesPartitionCacheIterator(this.readCache, 
                                                    this.manager.getRangeForRead(fromId, toId, definition));
    }

    /**
     * Returns the global cache size.
     * 
     * @return the global cache size.
     */
    long globalCacheSize() {

        return this.globalCache.size();
    }

    /**
     * Returns the read cache size.
     * 
     * @return the read cache size.
     */
    long readCacheSize() {
        return this.readCache.size();
    }

    /**
     * Returns the write cache size.
     * 
     * @return the write cache size.
     */
    long writeCacheSize() {
        return this.writeCache.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush(TimeSeriesPartition timeSeriesPartition, FlushListener... listeners) {

        this.manager.flush(timeSeriesPartition, listeners);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forceFlush(TimeSeriesPartition timeSeriesPartition, FlushListener... listeners) {

        this.manager.forceFlush(timeSeriesPartition, listeners);
    }

    /**
     * Returns the statistics of the global cache.
     * 
     * @return the statistics of the global cache.
     */
    CacheStats globalCacheStats() {

        return this.globalCache.stats();
    }

    /**
     * Returns the statistics of the read cache.
     * 
     * @return the statistics of the read cache.
     */
    CacheStats readCacheStats() {

        return this.readCache.stats();
    }

    /**
     * Returns the statistics of the write cache.
     * 
     * @return the statistics of the write cache.
     */
    CacheStats writeCacheStats() {

        return this.writeCache.stats();
    }

    void evictFromReadCache(PartitionId id) {

        this.readCache.invalidate(id);
    }
    
    /**
     * <code>KeyValueIterator</code> used to iterate over a range of partitions.
     */
    private static final class TimeSeriesPartitionCacheIterator implements KeyValueIterator<PartitionId, TimeSeriesPartition> {
        
        /**
         * The meta data iterator.
         */
        private final KeyValueIterator<PartitionId, TimeSeriesPartition> iterator;

        /**
         * The cache used when performing reads.
         */
        private final TimeSeriesPartitionReadCache readCache;
        
        /**
         * Creates a <code>TimeSeriesPartitionCacheIterator</code>.
         * 
         * @param readCache the read cache
         * @param iterator the partition iterator
         */
        public TimeSeriesPartitionCacheIterator(TimeSeriesPartitionReadCache readCache, 
                                                KeyValueIterator<PartitionId, TimeSeriesPartition> iterator) {
            
            this.readCache = readCache;
            this.iterator = iterator;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean next() throws IOException {
            return this.iterator.next();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public PartitionId getKey() {
            return this.iterator.getKey();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TimeSeriesPartition getValue() throws IOException {
            
            PartitionId id = getKey();
            TimeSeriesPartition partition = this.readCache.getIfPresent(id);
            
            if (partition != null) {
                return partition;
            }
            
            partition = this.iterator.getValue();
            this.readCache.put(id, partition);
            
            return partition;
        }        
    }
}
