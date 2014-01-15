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
package io.horizondb.db.metrics;

import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.common.cache.Cache;

/**
 * The metrics for a cache instance.
 * 
 * @author Benjamin
 * 
 */
public final class CacheMetrics implements MetricSet {

    /**
     * The name of the loading cache.
     */
    private final String name;

    /**
     * The cache.
     */
    private final Cache<?, ?> cache;

    public CacheMetrics(String name, Cache<?, ?> cache) {

        this.name = name;
        this.cache = cache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Metric> getMetrics() {

        Map<String, Metric> map = new HashMap<String, Metric>();

        map.put(MetricRegistry.name(this.name, "size"), new Gauge<Long>() {

            /**
             * {@inheritDoc}
             */
            @SuppressWarnings("boxing")
            @Override
            public Long getValue() {
                return CacheMetrics.this.cache.size();
            }
        });

        map.put(MetricRegistry.name(this.name, "requestCount"), new Gauge<Long>() {

            /**
             * {@inheritDoc}
             */
            @SuppressWarnings("boxing")
            @Override
            public Long getValue() {
                return CacheMetrics.this.cache.stats().requestCount();
            }
        });

        map.put(MetricRegistry.name(this.name, "hitRate"), new Gauge<Double>() {

            /**
             * {@inheritDoc}
             */
            @SuppressWarnings("boxing")
            @Override
            public Double getValue() {
                return CacheMetrics.this.cache.stats().hitRate();
            }
        });

        map.put(MetricRegistry.name(this.name, "evictionCount"), new Gauge<Long>() {

            /**
             * {@inheritDoc}
             */
            @SuppressWarnings("boxing")
            @Override
            public Long getValue() {
                return CacheMetrics.this.cache.stats().evictionCount();
            }
        });

        return map;
    }
}
