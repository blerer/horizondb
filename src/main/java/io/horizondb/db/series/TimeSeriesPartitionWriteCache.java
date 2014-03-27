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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.collect.TreeMultimap;

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
    private final TimeSeriesPartitionListener listener;

    /**
     * The counter used to keep track of the memory usage.
     */
    private final AtomicLong memTimeSeriesMemoryUsage = new AtomicLong();
    
    /**
     * The partitions per first segment containing non persisted data.
     */
    @GuardedBy("partitionsPerSegment")
    private final TreeMultimap<Long, TimeSeriesPartition> partitionsPerSegment = TreeMultimap.create();

    public TimeSeriesPartitionWriteCache(Configuration configuration, TimeSeriesPartitionSecondLevelCache cache) {

        super(configuration, cache);

        this.listener = new TimeSeriesPartitionListener() {

            @Override
            public void memoryUsageChanged(TimeSeriesPartition partition, int previousMemoryUsage, int newMemoryUsage) {

                updateMemoryUsage(previousMemoryUsage, newMemoryUsage);

                if (newMemoryUsage == 0 
                        && TimeSeriesPartitionWriteCache.this.getIfPresent(partition.getId()) == null) {
                    return;
                }
                
                TimeSeriesPartitionWriteCache.this.put(partition.getId(), partition);
            }

            @Override
            public void firstSegmentContainingNonPersistedDataChanged(TimeSeriesPartition partition,
                                                                      Long previousSegment,
                                                                      Long newSegment) {
                
                
                updatePartitionsPerSegment(partition, previousSegment, newSegment);
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
                .expireAfterAccess(configuration.getMemTimeSeriesIdleTimeInSeconds(),
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
                    @Override
                    public void
                            onRemoval(RemovalNotification<PartitionId, TimeSeriesPartition> notification) {

                        if (RemovalCause.REPLACED.equals(notification.getCause())) {
                            return;
                        }

                        final TimeSeriesPartition partition = notification.getValue();

                        partition.scheduleForceFlush(new FlushListener() {
                            
                            @Override
                            public void afterFlush() {
                                partition.removeListener(TimeSeriesPartitionWriteCache.this.listener);
                            }
                        });
                    }

                })
                .recordStats().concurrencyLevel(configuration.getCachesConcurrencyLevel());
    }


    /**
     * @param id
     * @return
     */
    public List<TimeSeriesPartition> getPartitionsWithNonPersistedDataWithin(long id) {
        
        synchronized(this.partitionsPerSegment)  {
            
            List<TimeSeriesPartition> partitions = new ArrayList<>(); 
            
            for (Long segment : this.partitionsPerSegment.keySet()) {
                
                if (id < segment.longValue()){
                    break;
                }
                
                partitions.addAll(this.partitionsPerSegment.get(segment));
            }
            
            return partitions;
        } 
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterLoad(TimeSeriesPartition partition) {
        
        partition.addListener(TimeSeriesPartitionWriteCache.this.listener);
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
    

    /**
     * Updates the total memory usage by the time series.
     * 
     * @param previousMemoryUsage the previous amount of memory being used by the partition
     * @param newMemoryUsage the new amount of memory being used by the partition
     * @return the new total memory usage 
     */
    private long updateMemoryUsage(int previousMemoryUsage, int newMemoryUsage) {
        
        int delta = newMemoryUsage - previousMemoryUsage;

        long newMemTimeSeriesMemoryUsage = this.memTimeSeriesMemoryUsage.addAndGet(delta);

        this.logger.debug("memory usage by all memTimeSeries: {}", Long.valueOf(newMemTimeSeriesMemoryUsage));
        
        return newMemTimeSeriesMemoryUsage;
    }
    
    /**
     * Updates the mapping between partitions an the first segment that contains non persisted data.
     * 
     * @param partition the partition
     * @param previousSegment the previous first segment for which the partition was containing non persisted data
     * @param newSegment the new first segment for which the partition contains non persisted data 
     */
    private void updatePartitionsPerSegment(TimeSeriesPartition partition,
                                            Long previousSegment,
                                            Long newSegment) {

        synchronized(this.partitionsPerSegment) {
            
            if (previousSegment != null) {
                
                this.partitionsPerSegment.remove(previousSegment, partition);
            } 

            if (newSegment != null) {
                    
                this.partitionsPerSegment.put(newSegment, partition);
                this.logger.debug("first segment containing non persisted data for partition {}: {}", 
                                  partition.getId(),
                                  newSegment);
            
            } else {
                
                this.logger.debug("partition {} does not contains anymore non persisted data", partition.getId());
            }
        } 
    }
}
