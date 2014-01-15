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

import io.horizondb.db.metrics.ThreadPoolExecutorMetrics;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

import com.codahale.metrics.Gauge;

import static org.junit.Assert.assertEquals;

/**
 * @author Benjamin
 * 
 */
public class ThreadPoolMetricsTest {

    @Test
    public void testWithFixedThreadPool() throws InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(2);

        ThreadPoolExecutorMetrics metrics = new ThreadPoolExecutorMetrics("test", (ThreadPoolExecutor) executor);

        NonBlockingRunnable firstRunnable = new NonBlockingRunnable();
        BlockingRunnable secondRunnable = new BlockingRunnable();
        BlockingRunnable thirdRunnable = new BlockingRunnable();
        NonBlockingRunnable fourthRunnable = new NonBlockingRunnable();

        try {

            executor.submit(firstRunnable);
            executor.submit(secondRunnable);
            executor.submit(thirdRunnable);
            executor.submit(fourthRunnable);

            Thread.sleep(100L);

            assertEquals(2, getIntValue(metrics, "test.activeCount"));
            assertEquals(1, getLongValue(metrics, "test.completedTaskCount"));
            assertEquals(1, getLongValue(metrics, "test.pendingTaskCount"));
            assertEquals(2, getIntValue(metrics, "test.poolSize"));

        } finally {

            secondRunnable.unblock();
            thirdRunnable.unblock();
        }
    }

    private static int getIntValue(ThreadPoolExecutorMetrics metrics, String name) {

        Gauge<Integer> gauge = (Gauge<Integer>) metrics.getMetrics().get(name);
        return gauge.getValue().intValue();
    }

    private static long getLongValue(ThreadPoolExecutorMetrics metrics, String name) {

        Gauge<Long> gauge = (Gauge<Long>) metrics.getMetrics().get(name);
        return gauge.getValue().longValue();
    }

    /**
     * Non blocking runnable used during the tests.
     * 
     */
    private static class NonBlockingRunnable implements Runnable {

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {

            // Do nothing.
        }
    }

    /**
     * Blocking runnable used during the tests.
     * 
     */
    private static class BlockingRunnable implements Runnable {

        /**
         * The lock used to block the task.
         */
        private ReentrantLock lock = new ReentrantLock();

        /**
         * Creates a new <code>BlockingRunnable</code>.
         */
        public BlockingRunnable() {

            this.lock.lock();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {

            this.lock.lock();
            this.lock.unlock();
        }

        /**
         * Unblock this runnable.
         */
        public void unblock() {
            this.lock.unlock();
        }
    }

}
