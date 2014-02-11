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

import com.google.common.cache.CacheBuilder;

/**
 * <code>Cache</code> for time series.
 * 
 * @author Benjamin
 * 
 */
final class TimeSeriesCache extends AbstractCache<TimeSeriesId, TimeSeries> {

    /**
     * Creates a <code>TimeSeriesCache</code> to cache the time series.
     * 
     * @param configuration the database configuration.
     */
    public TimeSeriesCache(Configuration configuration) {
        super(configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CacheBuilder<Object, Object> newBuilder(Configuration configuration) {
        return CacheBuilder.newBuilder().maximumSize(configuration.getTimeSeriesCacheMaximumSize()).recordStats();
    }
}
