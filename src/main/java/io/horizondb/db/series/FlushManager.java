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
import io.horizondb.db.metrics.PrefixFilter;
import io.horizondb.db.metrics.ThreadPoolExecutorMetrics;
import io.horizondb.db.util.concurrent.NamedThreadFactory;
import io.horizondb.db.util.concurrent.SyncTask;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

import static com.codahale.metrics.MetricRegistry.name;
import static io.horizondb.db.util.concurrent.ExecutorsUtils.shutdownAndAwaitForTermination;
import static org.apache.commons.lang.Validate.notNull;

/**
 * Manages the flush of data to the disk.
 * 
 * @author Benjamin
 * 
 */
@ThreadSafe
final class FlushManager extends AbstractComponent {

    /**
     * The database configuration.
     */
    private final Configuration configuration;

    /**
     * The executor service used
     */
    private ExecutorService executor;

    /**
     * Creates a new <code>FlushManager</code> instance.
     * 
     * @param configuration the database configuration
     */
    public FlushManager(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {
        registry.registerAll(new ThreadPoolExecutorMetrics(name(getName(), "executor"),
                                                           (ThreadPoolExecutor) this.executor));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {
        registry.removeMatching(new PrefixFilter(getName()));
    }

    /**
     * Flush the pending in memory data of the specified partition.
     * 
     * @param partition the partition that must be flushed.
     * @param listeners the <code>FlushListener</code> that need to be notified from the flush
     */
    public void flush(TimeSeriesPartition partition, FlushListener... listeners) {

        checkRunning();
        this.executor.execute(new FlushTask(partition, listeners) {

            /**
             * {@inheritDoc}
             */
            @Override
            public void doFlush(TimeSeriesPartition partition) throws InterruptedException,
                                                              IOException,
                                                              ExecutionException {

                partition.flush();
            }
        });
    }

    /**
     * Flush all the in memory data of the specified partition.
     * 
     * @param partition the partition that must be flushed
     * @param listeners the <code>FlushListener</code> that need to be notified from the flush
     */
    public void forceFlush(TimeSeriesPartition partition, FlushListener... listeners) {

        checkRunning();
        this.executor.execute(new FlushTask(partition, listeners) {

            /**
             * {@inheritDoc}
             */
            @Override
            public void doFlush(TimeSeriesPartition partition) throws InterruptedException, IOException, ExecutionException {

                partition.forceFlush();
            }
        });
    }

    /**
     * Flush all the in memory data of the specified partition if the non persisted data are within or in a 
     * segment before the specified one.
     * 
     * @param segment the commit log segment id
     * @param partition the partition that must be flushed
     * @param listeners the <code>FlushListener</code> that need to be notified from the flush
     */
    public void forceFlush(final long segment, final TimeSeriesPartition partition, final FlushListener... listeners) {

        checkRunning();
        this.executor.execute(new FlushTask(partition, listeners) {

            /**
             * {@inheritDoc}
             */
            @Override
            public void doFlush(TimeSeriesPartition partition) throws InterruptedException, IOException, ExecutionException {

                Long firstSegment = partition.getFirstSegmentContainingNonPersistedData();
                if (firstSegment != null && Long.valueOf(segment).compareTo(firstSegment) >= 0) {
                    partition.forceFlush();
                }    
            }
        });
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        ThreadFactory factory = new NamedThreadFactory(getName());
        this.executor = Executors.newFixedThreadPool(1, factory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {
        
        sync(); // We need to sync first because flush tasks will cause the submit of new runnable through the 
                // savePartition method. Without sync those new task will be rejected by the executor which is
                // in terminating mode.
        shutdownAndAwaitForTermination(this.executor, this.configuration.getShutdownWaitingTimeInSeconds());
    }

    /**
     * Blocks until all the flush tasks previously submitted have been completed.
     * 
     * @throws InterruptedException if the thread has been interrupted.
     */
    void sync() throws InterruptedException {

        try {
            this.executor.submit(new SyncTask()).get();
        } catch (ExecutionException e) {
            // do nothing
        }
    }

    /**
     * A <code>Runnable</code> performing a flush.
     */
    private abstract static class FlushTask implements Runnable {

        /**
         * The logger.
         */
        private final Logger logger = LoggerFactory.getLogger(getClass());

        /**
         * The partition that need to have its in memory data flushed to the disk.
         */
        private final TimeSeriesPartition partition;
        
        /**
         * The listeners that need to be notified from the flush.
         */
        private final FlushListener[] listeners;

        /**
         * Creates a <code>FlushTask</code> that will flush the in memory data of the specified partition to the disk.
         * 
         * @param partition the partition that have some data that need to be flush to the disk.
         * @param listeners the listeners that need to be notified from the flush.
         */
        public FlushTask(TimeSeriesPartition partition, FlushListener... listeners) {

            notNull(partition, "the partition parameter must not be null.");

            this.partition = partition;
            this.listeners = listeners;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {

            try {

                doFlush(this.partition);
                notifyListeners();
                
            } catch (IOException | ExecutionException e) {

                this.logger.error("The flush of the partition " + this.partition.getId()
                        + " failed due to the following exception: ", e);

            } catch (InterruptedException e) {

                this.logger.error("The flush of the partition " + this.partition.getId() + " was interrupted.", e);

                Thread.currentThread().interrupt();
            }
        }

        /**
         * Perform the flush operation.
         * 
         * @throws InterruptedException if the thread is interrupted
         * @throws IOException if an I/O problem occurs.
         * @throws ExecutionException if a the commit log cannot persist some data
         */
        public abstract void doFlush(TimeSeriesPartition partition) throws InterruptedException, IOException, ExecutionException;
        
        /**
         * Notifies the flush listeners. 
         */
        private void notifyListeners() {
            
            for (FlushListener listener : this.listeners) {
                listener.afterFlush();
            }
        }
    }
}
