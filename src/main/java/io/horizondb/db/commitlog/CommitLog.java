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
import io.horizondb.db.StorageEngine;
import io.horizondb.db.HorizonDBException;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.model.ErrorCodes;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.Validate;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ListenableFuture;

public final class CommitLog extends AbstractComponent {

    /**
     * The possible sync modes.
     */
    public enum SyncMode {
        
        /**
         * Flushes the data to the disk after the specified interval of time.
         */
        BATCH,
        
        /**
         * Flushes the data to the disk at specified interval of time.
         */
        PERIODIC;
    }
    
    /**
     * The database configuration.
     */
    private final Configuration configuration;

    /**
     * Performs the pre-allocation in the background of the segments.
     */
    private final CommitLogAllocator allocator;

    /**
     * The executor used to performs the writes.
     */
    private WriteExecutor executor;

    /**
     * The segment on which the writes must be performed.
     */
    public CommitLogSegment activeSegment;

    public CommitLog(Configuration configuration, StorageEngine databaseEngine) {

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
        
        this.allocator.register(registry);
        this.executor.register(registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {
        
        this.executor.unregister(registry);
        this.allocator.unregister(registry);
    }

    /**
     * Writes the operation represented by the specified bytes to the commit log.
     * 
     * @param bytes the bytes to add to the log
     * @return the future returning the <code>ReplayPosition</code> for the written bytes.
     */
    public ListenableFuture<ReplayPosition> write(ReadableBuffer bytes) {

        checkRunning();
        
        return this.executor.executeWrite(new WriteTask(bytes));
    }

    /**
     * Waits for the commit log to flush the data to the disk if the sync mode is batch
     * 
     * @param configuration the database configuration
     * @param future the commit log future
     * @throws HorizonDBException if a problem occurs while writing to the commit log
     */
    public static void waitForCommitLogWriteIfNeeded(Configuration configuration, 
                                                     ListenableFuture<ReplayPosition> future) 
                                                             throws HorizonDBException {
        
        if (configuration.getCommitLogSyncMode() == CommitLog.SyncMode.BATCH) {
            
            waitForCommitLogWrite(future);
        }
    }

    /**
     * Wait for the specified future to complete.
     * 
     * @param future the future
     * @throws HorizonDBException if an error occurs
     */
    public static void waitForCommitLogWrite(ListenableFuture<ReplayPosition> future) throws HorizonDBException {
        try {
            
            future.get();
            
        } catch (ExecutionException e) {

            throw new HorizonDBException(ErrorCodes.INTERNAL_ERROR, 
                                         "an internal error has occured: ",
                                         e.getCause());
        } catch (InterruptedException e) {
            
            Thread.currentThread().interrupt();
            
            throw new HorizonDBException(ErrorCodes.INTERNAL_ERROR, 
                                         "an internal error has occured: ",
                                         e.getCause());
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        this.allocator.start();
        
        if (this.configuration.getCommitLogSyncMode() == SyncMode.BATCH) {
        
            this.executor = new BatchWriteExecutor(this.configuration, new FlushTask());  
        
        } else {    
            
            this.executor = new PeriodicWriteExecutor(this.configuration, new FlushTask());
        }    
    }

    /**
     * 
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

        try {

            this.executor.shutdown();

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

        this.logger.debug("Activating new segment");
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
    class WriteTask implements Callable<ReplayPosition> {

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
    final class FlushTask implements Runnable {

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {

            try {
                flush();
            } catch (IOException e) {
                CommitLog.this.logger.error("", e);
            }
        }
    }
}
