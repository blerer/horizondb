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
 * A <code>WriteExecutor</code> that flush the data to disk periodically.
 * The period is specified by the <code>commitLogFlushPeriodInMillis</code> from the database configuration.
 * 
 * @author Benjamin
 *
 */
final class PeriodicWriteExecutor extends AbstractWriteExecutor {


    public PeriodicWriteExecutor(Configuration configuration, FlushTask flushTask) {
        
        super(configuration, flushTask);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected WriteThreadPoolExecutor createWriteThreadPoolExecutor(Configuration configuration,
                                                                    String name,
                                                                    FlushTask flushTask) {
        return new PeriodicExecutorService(configuration, name, flushTask);
    }
    
    /**
     *  <code>ScheduledThreadPoolExecutor</code> that flush the data to disk periodically.
     *
     */
    private static final class PeriodicExecutorService extends WriteThreadPoolExecutor {
        
        public PeriodicExecutorService(Configuration configuration, String name, FlushTask flushTask) {
            
            super(name, flushTask);
            
            long flushPeriodInMillis = configuration.getCommitLogFlushPeriodInMillis();
            scheduleAtFixedRate(flushTask, flushPeriodInMillis, flushPeriodInMillis, TimeUnit.MILLISECONDS);
        }
    }
}
