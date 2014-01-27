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

import java.util.concurrent.TimeUnit;

/**
 * A <code>WriteExecutor</code> that flush the data to disk after a specified amount of time.
 * 
 * @author Benjamin
 */
final class BatchWriteExecutor extends AbstractWriteExecutor {

    public BatchWriteExecutor(Configuration configuration, FlushTask flushTask) {
        
        super(configuration,  flushTask);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected WriteThreadPoolExecutor createWriteThreadPoolExecutor(final Configuration configuration,
                                                                    final String name,
                                                                    final FlushTask flushTask) {
        return new BatchExecutorService(configuration, name, flushTask);
    }
    
    /**
     *  <code>ScheduledThreadPoolExecutor</code> that flush the data to disk after a specified time.
     *
     */
    private static final class BatchExecutorService extends WriteThreadPoolExecutor {
        
        /**
         * The database configuration.
         */
        private final Configuration configuration;
        
        /**
         * <code>true</code> if a flush has been scheduled, <code>false</code> otherwise.
         */
        private volatile boolean hasScheduledFlush;

        public BatchExecutorService(Configuration configuration, String name, FlushTask flushTask) {
            super(name, flushTask);
            
            this.configuration = configuration;
        }

        /**        
         * {@inheritDoc}
         */
        @Override
        protected void beforeWrite() {
            
            if (!this.hasScheduledFlush && !isTerminating()) {
                
                schedule(getFlushTask(), 
                         this.configuration.getCommitLogBatchWindowInMillis(), 
                         TimeUnit.MILLISECONDS);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void afterFlush() {
            this.hasScheduledFlush = false;
        }
    }
}
