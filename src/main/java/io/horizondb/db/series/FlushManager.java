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
import io.horizondb.db.btree.BTree;
import io.horizondb.db.metrics.PrefixFilter;
import io.horizondb.db.metrics.ThreadPoolExecutorMetrics;
import io.horizondb.db.utils.concurrent.NamedThreadFactory;
import io.horizondb.db.utils.concurrent.SyncTask;
import io.horizondb.model.PartitionId;

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

import static io.horizondb.db.utils.concurrent.ExecutorsUtils.shutdownAndAwaitForTermination;

import static com.codahale.metrics.MetricRegistry.name;
import static org.apache.commons.lang.Validate.notNull;

/**
 * Manages the flush of the time series partitions.
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
     */
    public void flush(TimeSeriesPartition partition) {

        checkRunning();
        this.executor.execute(new FlushTask(partition) {

            /**
             * {@inheritDoc}
             */
            @Override
            public void doFlush(TimeSeriesPartition partition) throws InterruptedException, IOException {

                partition.flush();
            }
        });
    }

    /**
     * Flush all the in memory data of the specified partition.
     * 
     * @param partition the partition that must be flushed.
     */
    public void forceFlush(TimeSeriesPartition partition) {

        checkRunning();
        this.executor.execute(new FlushTask(partition) {

            /**
             * {@inheritDoc}
             */
            @Override
            public void doFlush(TimeSeriesPartition partition) throws InterruptedException, IOException {

                partition.forceFlush();
            }
        });
    }

    /**
     * Saves the partition meta data within the specified B+Tree.
     * 
     * @param partition the time series partition  
     * @param btree the B+Tree where the partition meta data must be saved
     */
    public void savePartition(final TimeSeriesPartition partition, 
                              final BTree<PartitionId, TimeSeriesPartitionMetaData> btree) {
        
        partition.getFuture().addListener(new Runnable() {
            
            /**
             * {@inheritDoc}
             */
            @Override
            public void run() {
                
                PartitionId id = partition.getId();
                
                try {
                    TimeSeriesPartitionMetaData metaData = partition.getMetaData();

                    FlushManager.this.logger.debug("saving partition {} with meta data: {}", id, metaData);

                    btree.insert(id, metaData);
                    
                } catch (IOException | InterruptedException | ExecutionException e) {
                    
                    FlushManager.this.logger.error("meta data for partition " + id + " could not be saved to disk");
                }
            }
            
        }, this.executor);
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
        shutdownAndAwaitForTermination(this.executor, this.configuration.getShutdownWaitingTimeInSeconds());
    }

    /**
     * Blocks until all the flush tasks previously submitted have been completed.
     * <p>
     * This method is implemented for testing purpose.
     * </p>
     * 
     * @throws Exception if a problem occurs while synchronizing.
     */
    void sync() throws Exception {

        this.executor.submit(new SyncTask()).get();
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
         * Creates a <code>FlushTask</code> that will flush the in memory data of the specified partition to the disk.
         * 
         * @param partition the partition that have some data that need to be flush to the disk.
         */
        public FlushTask(TimeSeriesPartition partition) {

            notNull(partition, "the partition parameter must not be null.");

            this.partition = partition;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {

            try {

                doFlush(this.partition);

            } catch (IOException e) {

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
         */
        public abstract void doFlush(TimeSeriesPartition partition) throws InterruptedException, IOException;
    }
}
