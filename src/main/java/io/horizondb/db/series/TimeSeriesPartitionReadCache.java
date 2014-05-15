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

import java.io.IOException;

import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.cache.AbstractMultilevelCache;
import io.horizondb.db.cache.ValueLoader;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.cache.CacheBuilder;

/**
 * The time series partition read cache.
 * 
 * @author Benjamin
 * 
 */
@ThreadSafe
final class TimeSeriesPartitionReadCache extends AbstractMultilevelCache<PartitionId, TimeSeriesPartition> {

    /**
     * Creates a <code>TimeSeriesPartitionReadCache</code> that get values from the specified cache if it does not 
     * have them.
     * 
     * @param configuration the database configuration
     * @param cache the second level cache
     */
    public TimeSeriesPartitionReadCache(Configuration configuration, TimeSeriesPartitionSecondLevelCache cache) {

        super(configuration, cache);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void put(final PartitionId key, final TimeSeriesPartition value) {
        try {
            get(key, new ValueLoader<PartitionId, TimeSeriesPartition>() {
                
                /**    
                 * {@inheritDoc}
                 */
                @Override
                public TimeSeriesPartition loadValue(PartitionId key) throws IOException, HorizonDBException {
                    return value;
                }
            });
            
        } catch (IOException | HorizonDBException e) {

            // Do nothing as no exception can occurs.
        } 
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesPartition getIfPresent(PartitionId id) {
        
        TimeSeriesPartition partition = super.getIfPresent(id);
        
        if (partition != null) {
            return partition;
        }
        
        partition = getSecondLevelCache().getIfPresent(id);
        
        if (partition != null) {
            put(id, partition);
        }
        return partition;
    }



    /**
     * {@inheritDoc}
     */
    @Override
    protected CacheBuilder<Object, Object> newBuilder(Configuration configuration) {

        return CacheBuilder.newBuilder()
                           .concurrencyLevel(configuration.getCachesConcurrencyLevel())
                           .maximumSize(configuration.getDatabaseCacheMaximumSize())
                           .recordStats();
    }
}
