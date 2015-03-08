/**
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

import io.horizondb.db.AbstractComponent;
import io.horizondb.db.Configuration;
import io.horizondb.db.btree.BTreeStore;
import io.horizondb.db.btree.KeyValueIterator;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static org.apache.commons.lang.Validate.notNull;

/**
 * Base class for the <code>TimeSeriesPartitionManager</code>.
 */
abstract class AbstractTimeSeriesPartitionManager extends AbstractComponent implements TimeSeriesPartitionManager {

    /**
     * The B+Tree branching factor.
     */
    private static final int BRANCHING_FACTOR = 128;

    /**
     * The Database server configuration.
     */
    private final Configuration configuration;

    /**
     * The B+Tree in which are stored the partition meta data.
     */
    private BTreeStore<PartitionId, TimeSeriesPartitionMetaData> btree;
    
    /**
     * The created but unsaved partitions.
     */
    @GuardedBy("rwLock")
    private TreeMap<PartitionId, TimeSeriesPartition> unsavedPartitions = new TreeMap<>();

    /**
     * The lock guarding the unsavedPartitions.
     */
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();
    
    /**
     * The flush manager
     */
    private final FlushManager flushManager;

    /**
     * Creates a new <code>AbstractTimeSeriesPartitionManager</code> that will used the specified configuration.
     * 
     * @param configuration the database configuration
     */
    public AbstractTimeSeriesPartitionManager(Configuration configuration) {

        notNull(configuration, "the configuration parameter must not be null.");

        this.configuration = configuration;
        this.flushManager = new FlushManager(configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        this.btree = createBTreeStore(this.configuration,  
                                      BRANCHING_FACTOR);

        this.flushManager.start();
    }

    /**
     * Creates the B+Tree used to store the partition definitions.
     * 
     * @param configuration the database configuration
     * @param branchingFactor the B+Tree branching factor
     * @throws IOException if an I/O problem occurs while creating the B+Tree
     */
    protected abstract BTreeStore<PartitionId, TimeSeriesPartitionMetaData> createBTreeStore(Configuration configuration,
                                                                                             int branchingFactor) 
                                                                                             throws IOException;

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {

        this.btree.register(registry);
        this.flushManager.register(registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {

        this.flushManager.unregister(registry);
        this.btree.unregister(registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void save(PartitionId id, TimeSeriesPartitionMetaData metaData) throws IOException, InterruptedException, ExecutionException {

        this.logger.debug("saving partition {} with meta data: {}", id, metaData);

        if (this.unsavedPartitions.containsKey(id)) {
            
            this.rwLock.writeLock().lock();
            
            try {
                
                this.btree.insert(id, metaData);
                this.unsavedPartitions.remove(id);
                
            } finally {
                
                this.rwLock.writeLock().unlock();
            }
        } else {
        
            this.btree.insert(id, metaData);
        }   
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesPartition getPartitionForWrite(PartitionId partitionId, 
                                                    TimeSeriesDefinition definition)
                                                    throws IOException {
        
        TimeSeriesPartitionMetaData metadata = this.btree.get(partitionId);

        if (metadata != null) {

            return newTimeSeriesPartition(partitionId, definition, metadata);
        }     
        
        metadata = TimeSeriesPartitionMetaData.newBuilder(partitionId.getRange()).build();
        TimeSeriesPartition partition = newTimeSeriesPartition(partitionId, definition, metadata);
        
        this.rwLock.writeLock().lock();
        
        try {
            
            this.unsavedPartitions.put(partitionId, partition);
            
        } finally {
            this.rwLock.writeLock().unlock();
        }
                
        return partition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueIterator<PartitionId, TimeSeriesPartition> getRangeForRead(PartitionId fromId,
                                                                              PartitionId toId,
                                                                              TimeSeriesDefinition definition) 
                                                                              throws IOException {
        
        KeyValueIterator<PartitionId, TimeSeriesPartition> unsavedPartitionsIterator;
        
        this.rwLock.readLock().lock();
        
        try {
            
            NavigableMap<PartitionId, TimeSeriesPartition> subMap = this.unsavedPartitions.subMap(fromId, true, toId, true);
            unsavedPartitionsIterator = new MapKeyValueIterator<>(subMap);
            
        } finally {
            this.rwLock.readLock().unlock();
        }
        
        return  new MergingKeyValueIterator<>(unsavedPartitionsIterator,          
                                              new TimeSeriesPartitionIterator(definition, 
                                                                              this.btree.iterator(fromId, toId)));
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void flush(TimeSeriesPartition timeSeriesPartition, FlushListener... listeners) {

        checkRunning();
        this.flushManager.flush(timeSeriesPartition, listeners);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forceFlush(TimeSeriesPartition timeSeriesPartition, FlushListener... listeners) {

        checkRunning();
        this.flushManager.forceFlush(timeSeriesPartition, listeners);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void forceFlush(long id, TimeSeriesPartition timeSeriesPartition, FlushListener... listeners) {
        
        checkRunning();
        this.flushManager.forceFlush(id, timeSeriesPartition, listeners);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public ListenableFuture<Boolean> forceFlush(long id) {
        return Futures.immediateFuture(Boolean.TRUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

        this.flushManager.shutdown();
        this.btree.close();
    }

    /**
     * Blocks until all the flush tasks previously submitted have been completed.
     * <p>
     * This method is implemented for testing purpose.
     * </p>
     * 
     * @throws Exception if a problem occurs while synchronizing.
     */
    void sync() throws Exception {

        this.flushManager.sync();
    }
    
    /**
     * Creates a new <code>TimeSeriesPartition</code>.
     * 
     * @param partitionId the partition ID
     * @param definition the time series definition
     * @param metadata the partition meta data
     * @return a new <code>TimeSeriesPartition</code>
     * @throws IOException if an I/O problem occurs
     */
    private TimeSeriesPartition newTimeSeriesPartition(PartitionId partitionId,
                                                       TimeSeriesDefinition definition,
                                                       TimeSeriesPartitionMetaData metadata) throws IOException {

        DatabaseDefinition databaseDefinition = new DatabaseDefinition(partitionId.getDatabaseName(), 
                                                                       partitionId.getDatabaseTimestamp());
        return new TimeSeriesPartition(this, this.configuration, databaseDefinition, definition, metadata);
    }

    /**
     * <code>KeyValueIterator</code> used to iterate over a range of partitions.
     */
    private final class TimeSeriesPartitionIterator implements KeyValueIterator<PartitionId, TimeSeriesPartition> {
        
        /**
         * The time series definition.
         */
        private final TimeSeriesDefinition definition;
        
        /**
         * The meta data iterator.
         */
        private final KeyValueIterator<PartitionId, TimeSeriesPartitionMetaData> iterator;

        /**
         * Creates a <code>TimeSeriesPartitionIterator</code>.
         * 
         * @param definition the time series definition
         * @param iterator the meta data iterator
         */
        public TimeSeriesPartitionIterator(TimeSeriesDefinition definition,
                                           KeyValueIterator<PartitionId, TimeSeriesPartitionMetaData> iterator) {
            this.definition = definition;
            this.iterator = iterator;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean next() throws IOException {
            return this.iterator.next();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public PartitionId getKey() {
            return this.iterator.getKey();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TimeSeriesPartition getValue() throws IOException {
            return newTimeSeriesPartition(getKey(), this.definition, this.iterator.getValue());
        }        
    }
}
