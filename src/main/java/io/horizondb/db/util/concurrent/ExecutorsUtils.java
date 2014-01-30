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
package io.horizondb.db.util.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for {@link ExecutorService}.
 * 
 * @author Benjamin
 * 
 */
public final class ExecutorsUtils {

    /**
     * The class logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorsUtils.class);

    /**
     * Returns a thread factory used to create new threads with name with the specified prefix.
     * 
     * @param prefix the prefix of the name of the threads that the factory will create.
     * @return a thread factory
     */
    public static ThreadFactory namedThreadFactory(String prefix) {
        return new NamedThreadFactory(prefix);
    }

    /**
     * Shutdown the specified executor and await for its termination. If after the specified amount of time the executor
     * did not shutdown the system will try to stop actively the executor.
     * 
     * @param executor the executor to shutdown
     * @param shutdownWaitingTimeInSeconds how many seconds the system will wait for the shutdown to happen before
     * trying to stop actively the executor.
     * @throws InterruptedException if interrupted while waiting for the executor to shutdown.
     */
    public static void
            shutdownAndAwaitForTermination(ExecutorService executor, int shutdownWaitingTimeInSeconds) throws InterruptedException {
        try {
            executor.shutdown();

            if (!executor.awaitTermination(shutdownWaitingTimeInSeconds, TimeUnit.SECONDS)) {

                executor.shutdownNow();

                if (!executor.awaitTermination(shutdownWaitingTimeInSeconds, TimeUnit.SECONDS)) {

                    LOG.error("The executor: {} could not be shutdown.", executor);
                }
            }

        } catch (InterruptedException e) {
            executor.shutdownNow();
            throw e;
        }
    }

    /**
     * Must not be instantiated as all methods are statics.
     */
    private ExecutorsUtils() {

    }
}
