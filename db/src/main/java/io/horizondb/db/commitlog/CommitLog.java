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
package io.horizondb.db.commitlog;

import io.horizondb.db.AbstractComponent;
import io.horizondb.db.Configuration;
import io.horizondb.db.DatabaseEngine;
import io.horizondb.db.metrics.PrefixFilter;
import io.horizondb.db.metrics.ThreadPoolExecutorMetrics;
import io.horizondb.db.utils.concurrent.ExecutorsUtils;
import io.horizondb.db.utils.concurrent.NamedThreadFactory;
import io.horizondb.io.ReadableBuffer;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.Validate;

import com.codahale.metrics.MetricRegistry;

import static com.codahale.metrics.MetricRegistry.name;

public final class CommitLog extends AbstractComponent {

    /**
     * The database configuration.
     */
    private final Configuration configuration;

    /**
     * Performs the pre-allocation in the background of the segments.
     */
    private final CommitLogAllocator allocator;

    /**
     * The executor service used to performs the writes.
     */
    private ScheduledExecutorService executor;

    /**
     * The segment on which the writes must be performed.
     */
    public CommitLogSegment activeSegment;

    public CommitLog(Configuration configuration, DatabaseEngine databaseEngine) {

        Validate.notNull(configuration, "the configuration parameter must be not null");
        Validate.notNull(databaseEngine, "the databaseEngine parameter must be not null");

        this.configuration = configuration;
        this.allocator = new CommitLogAllocator(configuration, databaseEngine);
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
     * Writes the operation represented by the specified bytes to the commit log.
     * 
     * @param bytes the bytes to add to the log
     * @return the future returning the <code>ReplayPosition</code> for the written bytes.
     */
    public Future<ReplayPosition> write(ReadableBuffer bytes) {

        checkRunning();
        return this.executor.submit(new WriteTask(bytes));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        this.allocator.start();

        ThreadFactory threadFactory = new NamedThreadFactory(getName() + "-Writer");
        this.executor = Executors.newScheduledThreadPool(1, threadFactory);

        long periodInMillis = this.configuration.getCommitLogFlushPeriodInMillis();
        this.executor.scheduleAtFixedRate(new FlushTask(), periodInMillis, periodInMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

        try {

            this.executor.execute(new FlushTask());
            ExecutorsUtils.shutdownAndAwaitForTermination(this.executor,
                                                          this.configuration.getShutdownWaitingTimeInSeconds());

        } finally {

            this.allocator.shutdown();
        }
    }

    /**
     * Fetches a new segment file from the allocator and activates it.
     * 
     * @return the newly activated segment
     * @throws InterruptedException
     */
    private void activateNextSegment() throws InterruptedException {

        this.activeSegment = this.allocator.fetchSegment();
        this.logger.debug("Active segment is now {}", this.activeSegment);
    }

    /**
     * Returns the active segment.
     * 
     * @return the active segment.
     */
    private CommitLogSegment getActiveSegment() {
        return this.activeSegment;
    }

    /**
     * Flushes the data of all the active commit log segments to disk.
     * 
     * @throws IOException if a problem occurs while flushing the data.
     */
    private void flush() throws IOException {

        for (CommitLogSegment segment : this.allocator.getActiveSegments()) {

            segment.flush();
        }
    }

    /**
     * <code>Callable</code> in charge of performing the write to the commit log segment.
     * 
     */
    private class WriteTask implements Callable<ReplayPosition> {

        /**
         * The bytes to write to the commit log.
         */
        private final ReadableBuffer bytes;

        /**
         * Creates a <code>WriteTask</code> that will write the specified bytes into the actve segment.
         * 
         * @param bytes the bytes to write.
         */
        public WriteTask(ReadableBuffer bytes) {
            this.bytes = bytes;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public ReplayPosition call() throws InterruptedException, IOException {

            if (getActiveSegment() == null || !getActiveSegment().hasCapacityFor(this.bytes.readableBytes())) {
                activateNextSegment();
            }

            return getActiveSegment().write(this.bytes);
        }
    }

    /**
     * Flushes all the in memory data to the disk.
     * 
     */
    private class FlushTask implements Runnable {

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {

            try {
                flush();
            } catch (IOException e) {
                CommitLog.this.logger.error("A problem has occured while flushing the data to the disk", e);
            }
        }
    }
}
