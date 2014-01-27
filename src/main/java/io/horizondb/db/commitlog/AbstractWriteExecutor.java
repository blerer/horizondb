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
import io.horizondb.db.utils.concurrent.ExecutorsUtils;
import io.horizondb.db.utils.concurrent.NamedThreadFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;

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
        
        ListenableFutureTask<ReplayPosition> futureTask = ListenableFutureTask.create(writeTask);
        
        this.executor.execute(futureTask);
        
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
         * The flush task.
         */
        private final FlushTask flushTask;
        
        /**
         * The <code>CommitLogWriteFuture</code>s waiting for the flush signal.
         */
        private final Queue<CommitLogWriteFuture<?>> waitingFutures = new LinkedList<>();


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
        
        /**
         * {@inheritDoc}
         */
        @Override
        protected final <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
            
            RunnableScheduledFuture<V> decoratedTask =  super.decorateTask(runnable, task);
            
            if (runnable instanceof ListenableFutureTask) {
                
                return new CommitLogWriteFuture<V>(decoratedTask);
            }
            
            return decoratedTask;
        }
        
        /**
         * {@inheritDoc}
         */
        @SuppressWarnings("unchecked")
        @Override
        protected final void beforeExecute(Thread thread, Runnable runnable) {
            
            super.beforeExecute(thread, runnable);
            
            if (runnable instanceof CommitLogWriteFuture) {
                
                beforeWrite();
                
                addToWaitingFutures((CommitLogWriteFuture<ReplayPosition>) runnable);
            }
        }

        /**        
         * {@inheritDoc}
         */
        @Override
        protected final void afterExecute(Runnable runnable, Throwable throwable) {

            super.afterExecute(runnable, throwable);
            
            if (runnable instanceof CommitLogWriteFuture || throwable != null) {
                
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
            
            synchronized (this.waitingFutures) {
                
                CommitLogWriteFuture<?> future;
                
                while ((future  = this.waitingFutures.poll()) != null) {
                    
                    future.flushed();
                }
            }
        }

        /**
         * Adds the specified <code>Future</code> to the list of <code>Future</code> that will be waiting for the flush.
         * @param future the <code>Future</code> to add
         */
        private void addToWaitingFutures(CommitLogWriteFuture<ReplayPosition> future) {
            
            synchronized (this.waitingFutures) {
                
                this.waitingFutures.offer(future);
            }
        }
    }
}
