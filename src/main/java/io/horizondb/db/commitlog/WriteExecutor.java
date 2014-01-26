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

import io.horizondb.db.metrics.Monitorable;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * An object that executes writes to the commit log.
 * 
 * @author Benjamin
 *
 */
interface WriteExecutor extends Monitorable {

    /**
     * Executes the specified write task.
     * 
     * @param writeTask the write task to execute.
     * @return a <code>Future</code> representing pending completion of the task
     */
    ListenableFuture<ReplayPosition> executeWrite(CommitLog.WriteTask writeTask);  

    /**
     * Shutdown this <code>WriteExecutor</code>
     * @throws InterruptedException if the thread is interrupted
     */
    void shutdown() throws InterruptedException;
}
