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
import java.util.concurrent.ThreadPoolExecutor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;

/**
 * The metrics for a <code>ThreadPoolExecutor</code>.
 * 
 * @author Benjamin
 * 
 */
public final class ThreadPoolExecutorMetrics implements MetricSet {

    /**
     * The name of the thread pool.
     */
    private final String name;

    /**
     * The thread pool.
     */
    private final ThreadPoolExecutor threadPool;

    public ThreadPoolExecutorMetrics(String name, ThreadPoolExecutor threadPool) {

        this.name = name;
        this.threadPool = threadPool;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Metric> getMetrics() {

        Map<String, Metric> map = new HashMap<String, Metric>();

        map.put(MetricRegistry.name(this.name, "poolSize"), new Gauge<Integer>() {

            /**
             * {@inheritDoc}
             */
            @SuppressWarnings("boxing")
            @Override
            public Integer getValue() {
                return ThreadPoolExecutorMetrics.this.threadPool.getPoolSize();
            }
        });

        map.put(MetricRegistry.name(this.name, "activeCount"), new Gauge<Integer>() {

            /**
             * {@inheritDoc}
             */
            @SuppressWarnings("boxing")
            @Override
            public Integer getValue() {
                return ThreadPoolExecutorMetrics.this.threadPool.getActiveCount();
            }
        });

        map.put(MetricRegistry.name(this.name, "completedTaskCount"), new Gauge<Long>() {

            /**
             * {@inheritDoc}
             */
            @SuppressWarnings("boxing")
            @Override
            public Long getValue() {
                return ThreadPoolExecutorMetrics.this.threadPool.getCompletedTaskCount();
            }
        });

        map.put(MetricRegistry.name(this.name, "pendingTaskCount"), new Gauge<Long>() {

            /**
             * {@inheritDoc}
             */
            @SuppressWarnings("boxing")
            @Override
            public Long getValue() {

                ThreadPoolExecutor pool = ThreadPoolExecutorMetrics.this.threadPool;

                return pool.getTaskCount() - pool.getActiveCount() - pool.getCompletedTaskCount();
            }
        });

        return map;
    }
}
