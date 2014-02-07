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
import io.horizondb.db.cache.AbstractCache;
import io.horizondb.model.PartitionId;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.cache.CacheBuilder;

/**
 * The second level cache for the time series partitions. The cache is using weak values by consequence entries will 
 * be automatically removed on garbage collection when they are not present anymore within the read or write cache.  
 * 
 * @author Benjamin
 * 
 */
@ThreadSafe
public final class TimeSeriesPartitionSecondLevelCache extends AbstractCache<PartitionId, TimeSeriesPartition> {

    /**
     * Creates a second level cache for the time series partition.
     * 
     * @param configuration the database configuration.
     */
    public TimeSeriesPartitionSecondLevelCache(Configuration configuration) {

        super(configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CacheBuilder<Object, Object> newBuilder(Configuration configuration) {
        
        return CacheBuilder.newBuilder()
                           .concurrencyLevel(configuration.getCachesConcurrencyLevel())
                           .weakValues()
                           .recordStats();
    }
}
