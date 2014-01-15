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
import io.horizondb.db.metrics.CacheMetrics;
import io.horizondb.db.metrics.PrefixFilter;
import io.horizondb.model.PartitionId;
import io.horizondb.model.TimeSeriesDefinition;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

/**
 * Decorator that add caching functionalities to a <code>PartitionManager</code>
 * 
 * @author Benjamin
 * 
 */
@ThreadSafe
public final class TimeSeriesPartitionManagerCaches extends AbstractComponent implements TimeSeriesPartitionManager {

    /**
     * The decorated partition manager.
     */
    private final TimeSeriesPartitionManager manager;

    /**
     * The database configuration.
     */
    private final Configuration configuration;

    /**
     * The cache used when performing reads.
     */
    private final Cache<PartitionId, TimeSeriesPartition> readCache;

    /**
     * The cache used when performing writes.
     */
    private final Cache<PartitionId, TimeSeriesPartition> writeCache;

    /**
     * The global cache.
     */
    private final Cache<PartitionId, TimeSeriesPartition> globalCache;

    /**
     * The listener used to track the memory usage change.
     */
    private final MemoryUsageListener listener;

    /**
     * The counter used to keep track of the memory usage.
     */
    private final AtomicLong memTimeSeriesMemoryUsage = new AtomicLong();

    /**
     * Creates a <code>TimeSeriesPartitionManagerCachesTest</code> to globalCache the time series partition returned by
     * the specified manager.
     * 
     * @param configuration the database configuration.
     * @param manager the manager to decorate.
     */
    public TimeSeriesPartitionManagerCaches(Configuration configuration, TimeSeriesPartitionManager manager) {

        this.configuration = configuration;
        this.manager = manager;

        this.globalCache = CacheBuilder.newBuilder()
                                       .concurrencyLevel(this.configuration.getCachesConcurrencyLevel())
                                       .weakValues()
                                       .recordStats()
                                       .build();

        this.readCache = CacheBuilder.newBuilder()
                                     .concurrencyLevel(this.configuration.getCachesConcurrencyLevel())
                                     .maximumSize(this.configuration.getDatabaseCacheMaximumSize())
                                     .recordStats()
                                     .build();

        this.writeCache = CacheBuilder.newBuilder()
                                      .concurrencyLevel(this.configuration.getCachesConcurrencyLevel())
                                      .maximumWeight(this.configuration.getMaximumMemoryUsageByMemTimeSeries())
                                      .expireAfterAccess(this.configuration.getMemTimeSeriesLifeTime(),
                                                         TimeUnit.SECONDS)
                                      .recordStats()
                                      .weigher(new Weigher<PartitionId, TimeSeriesPartition>() {

                                          /**
                                           * {@inheritDoc}
                                           */
                                          @Override
                                          public int weigh(PartitionId id, TimeSeriesPartition partition) {

                                              return partition.getMemoryUsage();
                                          }
                                      })
                                      .removalListener(new RemovalListener<PartitionId, TimeSeriesPartition>() {

                                          /**
                                           * {@inheritDoc}
                                           */
                                          @SuppressWarnings("boxing")
                                          @Override
                                          public void
                                                  onRemoval(RemovalNotification<PartitionId, TimeSeriesPartition> notification) {

                                              if (RemovalCause.REPLACED.equals(notification.getCause())) {
                                                  return;
                                              }

                                              TimeSeriesPartition partition = notification.getValue();

                                              long newMemoryUsage = TimeSeriesPartitionManagerCaches.this.memTimeSeriesMemoryUsage.addAndGet(-partition.getMemoryUsage());

                                              TimeSeriesPartitionManagerCaches.this.logger.debug("the partition {} is being removed ({}) (new memory usage after flush {})",
                                                                                                 new Object[] {
                                                                                                         partition.getId(),
                                                                                                         notification.getCause(),
                                                                                                         newMemoryUsage });

                                              partition.removeMemoryUsageListener(TimeSeriesPartitionManagerCaches.this.listener);

                                              TimeSeriesPartitionManagerCaches.this.forceFlush(partition);
                                          }

                                      })
                                      .build();

        this.listener = new MemoryUsageListener() {

            @SuppressWarnings("boxing")
            @Override
            public void memoryUsageChanged(TimeSeriesPartition partition, int previousMemoryUsage, int newMemoryUsage) {

                int delta = newMemoryUsage - previousMemoryUsage;

                long newMemTimeSeriesMemoryUsage = TimeSeriesPartitionManagerCaches.this.memTimeSeriesMemoryUsage.addAndGet(delta);

                TimeSeriesPartitionManagerCaches.this.logger.debug("memory usage by all memTimeSeries: {}",
                                                                   newMemTimeSeriesMemoryUsage);

                TimeSeriesPartitionManagerCaches.this.writeCache.put(partition.getId(), partition);
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        this.manager.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {

        this.manager.register(registry);
        registry.register(MetricRegistry.name(getName(), "memTimeSeriesMemoryUsage"), new Gauge<Long>() {

            /**
             * {@inheritDoc}
             */
            @Override
            public Long getValue() {
                return Long.valueOf(TimeSeriesPartitionManagerCaches.this.memTimeSeriesMemoryUsage.get());
            }
        });
        registry.registerAll(new CacheMetrics(getName(), this.globalCache));
        registry.registerAll(new CacheMetrics(getName(), this.readCache));
        registry.registerAll(new CacheMetrics(getName(), this.writeCache));
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

        flushAllMemTimeSeries();

        this.manager.shutdown();
    }

    /**
     * Flushes all memTimeSeries to disk.
     */
    private void flushAllMemTimeSeries() {
        this.writeCache.invalidateAll();
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
                                                    final TimeSeriesDefinition seriesDefinition) throws IOException {

        try {

            return this.writeCache.get(partitionId, new Callable<TimeSeriesPartition>() {

                @Override
                public TimeSeriesPartition call() throws Exception {

                    TimeSeriesPartition partition = TimeSeriesPartitionManagerCaches.this.getPartitionFromGlobalCache(partitionId,
                                                                                                                      seriesDefinition);

                    partition.addMemoryUsageListener(TimeSeriesPartitionManagerCaches.this.listener);

                    return partition;
                }
            });

        } catch (ExecutionException e) {

            Throwable cause = e.getCause();

            throw new IOException(cause);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesPartition getPartitionForRead(final PartitionId partitionId,
                                                   final TimeSeriesDefinition seriesDefinition) throws IOException {

        try {

            return this.readCache.get(partitionId, new Callable<TimeSeriesPartition>() {

                @Override
                public TimeSeriesPartition call() throws Exception {

                    return TimeSeriesPartitionManagerCaches.this.getPartitionFromGlobalCache(partitionId,
                                                                                             seriesDefinition);
                }
            });

        } catch (ExecutionException e) {

            Throwable cause = e.getCause();

            throw new IOException(cause);
        }
    }

    TimeSeriesPartition getPartitionFromGlobalCache(final PartitionId partitionId,
                                                    final TimeSeriesDefinition seriesDefinition) throws IOException {
        try {

            return this.globalCache.get(partitionId, new Callable<TimeSeriesPartition>() {

                @Override
                public TimeSeriesPartition call() throws Exception {

                    // Calling get partition for write or read does not really change anything has the real
                    // difference between the 2 methods is only in caching.
                    return TimeSeriesPartitionManagerCaches.this.manager.getPartitionForWrite(partitionId,
                                                                                              seriesDefinition);
                }
            });

        } catch (ExecutionException e) {

            Throwable cause = e.getCause();

            throw new IOException(cause);
        }
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
    public void flush(TimeSeriesPartition timeSeriesPartition) {

        this.manager.flush(timeSeriesPartition);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forceFlush(TimeSeriesPartition timeSeriesPartition) {

        this.manager.forceFlush(timeSeriesPartition);
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
}
