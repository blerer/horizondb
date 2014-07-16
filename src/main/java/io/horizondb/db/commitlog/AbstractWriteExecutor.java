/**
 * Copyright 2014 Benjamin Lerer
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
package io.horizondb.db.commitlog;

import io.horizondb.db.Configuration;
import io.horizondb.db.commitlog.CommitLog.FlushTask;
import io.horizondb.db.commitlog.CommitLog.WriteTask;
import io.horizondb.db.metrics.PrefixFilter;
import io.horizondb.db.metrics.ThreadPoolExecutorMetrics;
import io.horizondb.db.util.concurrent.ExecutorsUtils;
import io.horizondb.db.util.concurrent.ForwardingRunnableScheduledFuture;
import io.horizondb.db.util.concurrent.NamedThreadFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ListenableFuture;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Base class for the <code>WriteExecutor</code>s implementations.
 * 
 * @author Benjamin
 */
abstract class AbstractWriteExecutor implements WriteExecutor {

    /**
     * The database configuration.
     */
    private final Configuration configuration;
    
    /**
     * The executor used to flush the data changes.
     */
    private final WriteThreadPoolExecutor executor;

    /**
     * @param configuration
     */
    public AbstractWriteExecutor(Configuration configuration, FlushTask flushTask) {
        
        this.configuration = configuration;
        this.executor = createWriteThreadPoolExecutor(configuration, name(getName(), "executor"), flushTask);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void register(MetricRegistry registry) {
        registry.registerAll(new ThreadPoolExecutorMetrics(name(getName(), "executor"), this.executor));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void unregister(MetricRegistry registry) {
        registry.removeMatching(new PrefixFilter(getName()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final String getName() {
        return MetricRegistry.name(this.getClass());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final ListenableFuture<ReplayPosition> executeWrite(WriteTask writeTask) {
        
        CommitLogWriteFutureTask<ReplayPosition> futureTask =  new CommitLogWriteFutureTask<>(writeTask);
        this.executor.submit(futureTask);
        return futureTask; 
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void shutdown() throws InterruptedException {
        
        this.executor.flush();
        ExecutorsUtils.shutdownAndAwaitForTermination(this.executor,
                                                      this.configuration.getShutdownWaitingTimeInSeconds());
    }   
    
    /**
     * Creates the <code>WriteThreadPoolExecutor</code> that will be used by this <code>WriteExecutor</code> 
     * to perform the writes.
     * 
     * @param configuration the database configuration
     * @param flushTask the flush task
     * @return the <code>WriteThreadPoolExecutor</code> that will be used by <code>WriteExecutor</code> to perform 
     * the writes
     */
    protected abstract WriteThreadPoolExecutor createWriteThreadPoolExecutor(Configuration configuration, 
                                                                             String name, 
                                                                             FlushTask flushTask);
    
    /**
     *  <code>ScheduledThreadPoolExecutor</code> that flush the data to disk.
     *
     */
    protected static class WriteThreadPoolExecutor extends ScheduledThreadPoolExecutor {
        
        /**
         * The class logger.
         */
        private final Logger logger = LoggerFactory.getLogger(getClass());
        
        /**
         * The flush task.
         */
        private final FlushTask flushTask;
        
        /**
         * The <code>CommitLogWriteFutureTask</code>s waiting for the flush signal.
         */
        private final Queue<CommitLogWriteFutureTask<?>> waitingFutures = new LinkedList<>();


        public WriteThreadPoolExecutor(String name, FlushTask flushTask) {
            
            super(1, new NamedThreadFactory(name));
            this.flushTask = flushTask;
        }
        
        /**
         * Executes a flush task as soon as possible.
         */
        final void flush() {
            
            execute(this.flushTask);
        }

        
        
        @Override
        protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
            
            if (runnable instanceof CommitLogWriteFutureTask) {
                
                return new RunnableAwareTask<>(task, runnable);
            }
            
            return task;
        }

        /**
         * {@inheritDoc}
         */
        @SuppressWarnings("unchecked")
        @Override
        protected final void beforeExecute(Thread thread, Runnable runnable) {

            if (runnable instanceof RunnableAwareTask) {
                
                Runnable futureTask = ((RunnableAwareTask<?>) runnable).getRunnable();
                
                beforeWrite();

                addToWaitingFutures((CommitLogWriteFutureTask<ReplayPosition>) futureTask);
                
                this.logger.debug("Executing write with {}", futureTask);
            
            } else {
                this.logger.debug("Flushing writes to the disk.");
            }
        }

        /**        
         * {@inheritDoc}
         */
        @Override
        protected final void afterExecute(Runnable runnable, Throwable throwable) {
            
            if (runnable instanceof RunnableAwareTask<?> || throwable != null) {
                
                this.logger.debug("Write {} is now waiting for flush to disk.", 
                                  ((RunnableAwareTask<?>) runnable).getRunnable());
                return;
            }

            afterFlush();
            notifyWaitingFutures();
        }       

        /**
         * Returns the flush task.    
         *     
         * @return the flush task.
         */
        protected final FlushTask getFlushTask() {
            return this.flushTask;
        }

        /**
         * Extension point.
         */
        protected void beforeWrite() {

        }
        
        /**
         * Extension point.
         */
        protected void afterFlush() {
            
        }

        /**
         * Notifies all the <code>Future</code>s that were waiting for a flush that it has been done.
         */
        private void notifyWaitingFutures() {
            
            int count = 0;
            
            synchronized (this.waitingFutures) {
                
                CommitLogWriteFutureTask<?> future;
                               
                while ((future  = this.waitingFutures.poll()) != null) {
                    
                    future.flushed();
                    count++;
                }
            }
            
            this.logger.debug("Flushed {} writes to the disk.", Integer.valueOf(count));
        }

        /**
         * Adds the specified <code>Future</code> to the list of <code>Future</code> that will be waiting for the flush.
         * @param future the <code>Future</code> to add
         */
        private void addToWaitingFutures(CommitLogWriteFutureTask<ReplayPosition> future) {
            
            synchronized (this.waitingFutures) {
                
                this.waitingFutures.offer(future);
            }
        }
    }
    
    /**
     * <code>RunnableScheduledFuture</code> that provide access to the decorated <code>Runnable</code>.
     *
     * @param <V>
     */
    private static final class RunnableAwareTask<V> extends ForwardingRunnableScheduledFuture<V> {

        /**
         * The task.
         */
        private final RunnableScheduledFuture<V> task;
    
        /**
         * The original runnable.
         */
        private final Runnable runnable;        
        
        /**
         * Creates a task that is aware of the decorated <code>Runnable</code>.
         * 
         * @param task the task decorating the runnable
         * @param runnable the runnable
         */
        public RunnableAwareTask(RunnableScheduledFuture<V> task, Runnable runnable) {
            this.task = task;
            this.runnable = runnable;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected RunnableScheduledFuture<V> delegate() {
            return this.task;
        }
        
        /**
         * Returns the runnable.
         * 
         * @return the runnable
         * @return
         */
        public Runnable getRunnable() {
            return this.runnable;
        }
    }    
}
