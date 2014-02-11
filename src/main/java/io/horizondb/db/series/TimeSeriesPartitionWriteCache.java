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

import io.horizondb.db.Configuration;
import io.horizondb.db.cache.AbstractMultilevelCache;
import io.horizondb.model.PartitionId;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheBuilder;
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
final class TimeSeriesPartitionWriteCache extends AbstractMultilevelCache<PartitionId, TimeSeriesPartition> {

    /**
     * The listener used to track the memory usage change.
     */
    private final MemoryUsageListener listener;

    /**
     * The counter used to keep track of the memory usage.
     */
    private final AtomicLong memTimeSeriesMemoryUsage = new AtomicLong();


    public TimeSeriesPartitionWriteCache(Configuration configuration, TimeSeriesPartitionSecondLevelCache cache) {

        super(configuration, cache);

        this.listener = new MemoryUsageListener() {

            @SuppressWarnings("boxing")
            @Override
            public void memoryUsageChanged(TimeSeriesPartition partition, int previousMemoryUsage, int newMemoryUsage) {

                int delta = newMemoryUsage - previousMemoryUsage;

                long newMemTimeSeriesMemoryUsage = TimeSeriesPartitionWriteCache.this.memTimeSeriesMemoryUsage.addAndGet(delta);

                TimeSeriesPartitionWriteCache.this.logger.debug("memory usage by all memTimeSeries: {}",
                                                                   newMemTimeSeriesMemoryUsage);

                TimeSeriesPartitionWriteCache.this.put(partition.getId(), partition);
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CacheBuilder<PartitionId, TimeSeriesPartition> newBuilder(Configuration configuration) {

        return CacheBuilder.newBuilder()
                
                .maximumWeight(configuration.getMaximumMemoryUsageByMemTimeSeries())
                .expireAfterAccess(configuration.getMemTimeSeriesLifeTime(),
                                   TimeUnit.SECONDS)
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

                        long newMemoryUsage = TimeSeriesPartitionWriteCache.this.memTimeSeriesMemoryUsage.addAndGet(-partition.getMemoryUsage());

                        TimeSeriesPartitionWriteCache.this.logger.debug("the partition {} is being removed ({}) (new memory usage after flush {})",
                                                                           new Object[] {
                                                                                   partition.getId(),
                                                                                   notification.getCause(),
                                                                                   newMemoryUsage });

                        partition.removeMemoryUsageListener(TimeSeriesPartitionWriteCache.this.listener);

                        partition.scheduleForceFlush();
                    }

                })
                .recordStats().concurrencyLevel(configuration.getCachesConcurrencyLevel());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterLoad(TimeSeriesPartition partition) {
        
        partition.addMemoryUsageListener(TimeSeriesPartitionWriteCache.this.listener);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onRegister(MetricRegistry registry) {
        
        registry.register(MetricRegistry.name(getName(), "memTimeSeriesMemoryUsage"), new Gauge<Long>() {

            /**
             * {@inheritDoc}
             */
            @Override
            public Long getValue() {
                return Long.valueOf(TimeSeriesPartitionWriteCache.this.memTimeSeriesMemoryUsage.get());
            }
        });
    }
}
