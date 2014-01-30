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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.Validate;

/**
 * Thread factory that give to the thread it creates a meaningful name.
 * 
 * @author Benjamin
 * 
 */
public final class NamedThreadFactory implements ThreadFactory {

    /**
     * The uncaught exception handler.
     */
    private static final LoggingUncaughtExceptionHandler exceptionHandler = new LoggingUncaughtExceptionHandler();

    /**
     * The name separator.
     */
    private static final char NAME_SEPARATOR = '-';

    /**
     * The thread counter.
     */
    private final AtomicInteger counter = new AtomicInteger(1);

    /**
     * The prefix of the name of the thread.
     */
    private final String prefix;

    /**
     * Creates a new <code>ThreadFactory</code> that will used the specified prefix for the name for the threads that it
     * creates.
     * 
     * @param prefix the prefix to use to create the thread names.
     */
    public NamedThreadFactory(String prefix) {

        Validate.notEmpty(prefix, "The prefix must not be empty.");

        this.prefix = prefix;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Thread newThread(Runnable runnable) {

        Thread thread = new Thread(runnable, this.prefix + NAME_SEPARATOR + this.counter.getAndIncrement());
        thread.setUncaughtExceptionHandler(exceptionHandler);

        if (thread.isDaemon()) {
            thread.setDaemon(false);
        }

        if (thread.getPriority() != Thread.NORM_PRIORITY) {
            thread.setPriority(Thread.NORM_PRIORITY);
        }

        return thread;
    }
}
