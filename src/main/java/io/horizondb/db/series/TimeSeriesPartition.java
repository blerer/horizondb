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

import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.commitlog.CommitLog;
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.PartitionId;
import io.horizondb.model.TimeRange;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.iterators.BinaryTimeSeriesRecordIterator;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;

import static org.apache.commons.lang.Validate.notNull;

/**
 * A partition of a given time series.
 * 
 * @author Benjamin
 * 
 */
@ThreadSafe
public final class TimeSeriesPartition implements TimeSeriesElement {

    /**
     * The logger.
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    /**
     * The database configuration.
     */
    private final Configuration configuration;

    /**
     * The partitions manager.
     */
    private final TimeSeriesPartitionManager manager;

    /**
     * The time series definition.
     */
    private final TimeSeriesDefinition definition;

    /**
     * The partition range.
     */
    private final TimeRange timeRange;

    /**
     * Used to combat heap fragmentation.
     */
    @GuardedBy("this")
    private final SlabAllocator allocator;

    /**
     * The <code>MemoryUsageListener</code>s that must be notified in case of memory usage change.
     */
    private final List<MemoryUsageListener> memoryUsageListeners = new CopyOnWriteArrayList<>();

    /**
     * The time series elements composing this partition.
     */
    private final AtomicReference<TimeSeriesElements> elements = new AtomicReference<>();

    /**
     * Creates a new <code>TimeSeriesPartition</code> for the specified time series.
     * 
     * @param manager the manager that created this time series partition
     * @param configuration the database configuration
     * @param definition the time series definition
     * @param metadata the meta data of this partition
     * @throws IOException if an I/O problem occurs while creating this partition
     */
    public TimeSeriesPartition(TimeSeriesPartitionManager manager,
                               Configuration configuration,
                               TimeSeriesDefinition definition,
                               TimeSeriesPartitionMetaData metadata) throws IOException {

        notNull(manager, "the manager parameter must not be null.");
        notNull(configuration, "the configuration parameter must not be null.");
        notNull(definition, "the definition parameter must not be null.");
        notNull(metadata, "the metadata parameter must not be null.");

        this.configuration = configuration;
        this.manager = manager;
        this.timeRange = metadata.getRange();
        this.definition = definition;
        this.allocator = new SlabAllocator(configuration.getMemTimeSeriesSize());

        TimeSeriesElement file = TimeSeriesFile.open(configuration, definition, metadata);

        this.elements.set(new TimeSeriesElements(configuration, definition, file));
    }

    /**
     * Returns the ID of this partition.
     * 
     * @return the ID of this partition.
     */
    public PartitionId getId() {

        return new PartitionId(this.definition.getDatabaseName(),
                               this.definition.getSeriesName(),
                               this.timeRange.getStart());
    }

    /**
     * Writes the specified record set in this partition.
     * 
     * @param iterator the record iterator containing the record to write.
     * @param future the commit log future
     * @throws IOException if an I/O problem occurs.
     * @throws HorizonDBException if the record set is invalid.
     * @throws InterruptedException if the commit log thread was interrupted
     */
    public synchronized void write(RecordIterator iterator, 
                                   ListenableFuture<ReplayPosition> future) 
                                           throws IOException, 
                                                  HorizonDBException {

        this.logger.debug("writing records to partition {}", getId());

        TimeSeriesElements oldElements = this.elements.get();
        TimeSeriesElements newElements = oldElements.write(this.allocator, iterator, future);
 
        waitForCommitLogWriteIfNeeded(future);
        
        this.elements.set(newElements);

        notifyMemoryUsageListenersIfNeeded(oldElements.getMemoryUsage(), newElements.getMemoryUsage());

        MemTimeSeries memSeries = newElements.getLastMemTimeSeries();

        if (memSeries.isFull()) {

            this.logger.debug("a memTimeSeries of partition {} is full => triggering flush", getId());

            this.manager.flush(this);
        }
    }

    /**
     * Waits for the commit log to flush the data to the disk if the sync mode is batch
     * 
     * @param future the commit log future
     * @throws HorizonDBException if a problem occurs while writing to the commit log
     */
    private void waitForCommitLogWriteIfNeeded(ListenableFuture<ReplayPosition> future) throws HorizonDBException {
        
        if (this.configuration.getCommitLogSyncMode() == CommitLog.SyncMode.BATCH) {
            
            waitForCommitLogWrite(future);
        }
    }

    /**
     * Wait for the specified future to complete.
     * 
     * @param future the future
     * @throws HorizonDBException if an error occurs
     */
    private static void waitForCommitLogWrite(ListenableFuture<ReplayPosition> future) throws HorizonDBException {
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
     * Returns a <code>RecordIterator</code> containing the data from the specified time range.
     * 
     * @param timeRange the time range for which the data must be returned.
     * @return a <code>RecordIterator</code> containing the data from the specified time range.
     * @throws IOException if an I/O problem occurs while writing the data.
     */
    public RecordIterator read(TimeRange timeRange) throws IOException {

        return new TimeRangeRecordIterator(this.definition, new BinaryTimeSeriesRecordIterator(this.definition,
                                                                                               newInput()), timeRange);
    }

    /**
     * Returns the meta data of this partition.
     * 
     * @return the meta data of this partition.
     * @throws ExecutionException if a problem occurred when writing the data to the commit log
     * @throws InterruptedException if the thread was interrupted
     */
    public TimeSeriesPartitionMetaData getMetaData() throws InterruptedException, ExecutionException {

        TimeSeriesFile file = this.elements.get().getFile();

        return TimeSeriesPartitionMetaData.newBuilder(this.timeRange)
                                          .fileSize(file.size())
                                          .replayPosition(file.getFuture().get())
                                          .build();
    }

    /**
     * Adds the specified listener to the list of listeners
     * 
     * @param listener the listener to add.
     */
    public void addMemoryUsageListener(MemoryUsageListener listener) {

        this.memoryUsageListeners.add(listener);
    }

    /**
     * Remove the specified listener from the list of listeners
     * 
     * @param listener the listener to remove.
     */
    public void removeMemoryUsageListener(MemoryUsageListener listener) {

        this.memoryUsageListeners.remove(listener);
    }

    /**
     * Returns the memory usage of this partition.
     * 
     * @return the memory usage of this partition.
     */
    public int getMemoryUsage() {

        return this.elements.get().getMemoryUsage();
    }

    /**
     * Flushes to the disk the <code>MemTimeSeries</code> that are full.
     * 
     * @throws IOException if an I/O problem occurs while flushing the data to the disk.
     * @throws InterruptedException if the thread has been interrupted.
     */
    public void flush() throws IOException, InterruptedException {

        synchronized (this) {

            this.logger.debug("performing flush on the partition {}", getId());

            TimeSeriesElements oldElements = this.elements.get();
            TimeSeriesElements newElements = oldElements.flush();

            if (oldElements == newElements) {

                this.logger.debug("no memTimeSeries had to be flushed for partition {}", getId());

                return;
            }

            this.elements.set(newElements);

            notifyMemoryUsageListenersIfNeeded(oldElements.getMemoryUsage(), newElements.getMemoryUsage());
        }

        this.manager.save(this);
    }

    /**
     * Flushes to the disk all <code>MemTimeSeries</code>.
     * 
     * @throws IOException if an I/O problem occurs while flushing the data to the disk.
     * @throws InterruptedException if the thread has been interrupted.
     */
    public void forceFlush() throws IOException, InterruptedException {

        synchronized (this) {

            TimeSeriesElements oldElements = this.elements.get();

            TimeSeriesElements newElements = oldElements.forceFlush();

            if (oldElements == newElements) {
                return;
            }

            this.elements.set(newElements);
            this.allocator.release();

            notifyMemoryUsageListenersIfNeeded(oldElements.getMemoryUsage(), newElements.getMemoryUsage());
        }

        this.manager.save(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListenableFuture<ReplayPosition> getFuture() {

        TimeSeriesElements elementList = this.elements.get();

        return elementList.getLast().getFuture();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SeekableFileDataInput newInput() throws IOException {

        return this.elements.get().newInput();
    }

    /**
     * Notifies the listeners that the memory usage has changed.
     * 
     * @param previousMemoryUsage the previous memory usage
     * @param newMemoryUsage the new memory usage
     */
    @SuppressWarnings("boxing")
    private void notifyMemoryUsageListenersIfNeeded(int previousMemoryUsage, int newMemoryUsage) {

        if (previousMemoryUsage == newMemoryUsage) {

            return;
        }

        this.logger.debug("memory usage for partition {} changed (previous = {}, new = {})", new Object[] { getId(),
                previousMemoryUsage, newMemoryUsage });

        for (int i = 0, m = this.memoryUsageListeners.size(); i < m; i++) {

            this.memoryUsageListeners.get(i).memoryUsageChanged(this, previousMemoryUsage, newMemoryUsage);
        }
    }
}
