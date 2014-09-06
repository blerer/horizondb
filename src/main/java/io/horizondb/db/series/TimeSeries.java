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

import io.horizondb.db.HorizonDBException;
import io.horizondb.db.btree.KeyValueIterator;
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.db.util.concurrent.FutureUtils;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Filter;
import io.horizondb.model.core.Predicate;
import io.horizondb.model.core.Projection;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TimeZone;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Represents a time series.
 * 
 * @author Benjamin
 * 
 */
public final class TimeSeries {

    /**
     * The database name.
     */
    private final String databaseName;
    
    /**
     * The time series definition
     */
    private final TimeSeriesDefinition definition;

    /**
     * The partition manager.
     */
    private final TimeSeriesPartitionManager partitionManager;

    /**
     * Creates a new <code>TimeSeries</code> instance.
     * 
     * @param databaseName the name of database to which belongs this time series 
     * @param definition the time series definition
     * @param partitionManager the partition manager 
     */
    public TimeSeries(String databaseName, 
                      TimeSeriesDefinition definition, 
                      TimeSeriesPartitionManager partitionManager) {

        this.databaseName = databaseName;
        this.partitionManager = partitionManager;
        this.definition = definition;
    }

    /**
     * Returns the time series definition.
     * @return the time series definition.
     */
    public TimeSeriesDefinition getDefinition() {

        return this.definition;
    }

    public void write(List<? extends Record> records, ListenableFuture<ReplayPosition> future, boolean replay) throws IOException, 
                                                                                                  HorizonDBException {
        Range<Field> range = null;
        
        List<Record> batch = new ArrayList<>();
                
        Field timestamp = this.definition.newField(Record.TIMESTAMP_FIELD_NAME);
        
        for (int i = 0, m = records.size(); i < m; i++) {
            
            Record record = records.get(i);
            
            if (record.isDelta()) {
                
                timestamp.add(record.getField(0));
                
            } else {
                
                record.getField(0).copyTo(timestamp);
            }
            
            if (range == null) {
                
                range = this.definition.getPartitionTimeRange(timestamp);
            }
            
            if (!range.contains(timestamp)) {
                
                writeToPartition(toPartitionId(range), batch, future, replay);
                range = this.definition.getPartitionTimeRange(timestamp);
            }
         
            batch.add(record);
        }
        
        if (range != null) {            
            writeToPartition(toPartitionId(range), batch, future, replay);
        }
    }

    /**
     * Returns the records of this time series that match the specified expression.
     *  
     * @param projection the data that must be returned to the user
     * @param predicate the predicate used to filter the data
     * @throws IOException if an I/O problem occurs
     * @throws HorizonDBException if another problem occurs
     */
    public RecordIterator read(Projection projection, Predicate predicate) throws IOException, HorizonDBException {

        Field prototype = this.definition.newField(Record.TIMESTAMP_FIELD_NAME);
        TimeZone timezone = this.definition.getTimeZone();
        
        Filter<String> recordTypeFilter = projection.getRecordTypeFilter();
        RangeSet<Field> timeRanges = predicate.getTimestampRanges(prototype, timezone);
        Filter<Record> filter = predicate.toFilter(this.definition);
        
        return read(timeRanges, recordTypeFilter, filter);
    }
    
    /**
     * Returns the records of this time series that belong to the specified time ranges and are accepted by the 
     * specified filter.
     * 
     * @param timeRanges the time ranges for which the data must be read
     * @throws IOException if an I/O problem occurs
     * @throws HorizonDBException if another problem occurs
     */
    public RecordIterator read(RangeSet<Field> timeRanges,
                               Filter<String> recordTypeFilter,
                               Filter<Record> filter) throws IOException, HorizonDBException {

        Range<Field> span = timeRanges.span();
        
        final Range<Field> from = this.definition.getPartitionTimeRange(span.lowerEndpoint());
        final Range<Field> to;
        
        if (from.contains(span.upperEndpoint())) {
            to = from;
        } else {
            to = this.definition.getPartitionTimeRange(span.upperEndpoint());
        }
        
        KeyValueIterator<PartitionId, TimeSeriesPartition> rangeForRead = 
                this.partitionManager.getRangeForRead(toPartitionId(from), toPartitionId(to), this.definition);
        
        return new PartitionRecordIterator(timeRanges, rangeForRead, recordTypeFilter, filter);
    }

    /**
     * Creates the partition ID associated to the specified time range.
     * 
     * @param range the partition time range
     * @return the partition ID associated to the specified time range.
     */
    private PartitionId toPartitionId(Range<Field> range) {
        return new PartitionId(this.databaseName, this.definition.getName(), range);
    }
    
    /**
     * Writes the specified set of records to the specified partition.
     * 
     * @param partitionId the partition ID
     * @param records the records to write to the partition
     * @param future the commit log future
     * @param replay <code>true</code> if this is a commit log replay
     * @throws IOException if an I/O problem occurs
     * @throws HorizonDBException if a problem occurs
     */
    private void writeToPartition(PartitionId partitionId,
                                  List<Record> records,
                                  ListenableFuture<ReplayPosition> future,
                                  boolean replay) throws IOException, HorizonDBException {
        
        TimeSeriesPartition partition = this.partitionManager.getPartitionForWrite(partitionId, this.definition);
        
        if (replay) {
            
            final ReplayPosition currentReplayPosition = FutureUtils.safeGet(future);
            final ReplayPosition partitionReplayPosition = FutureUtils.safeGet(partition.getFuture());
            
            if (!currentReplayPosition.isAfter(partitionReplayPosition)) {
                
                return;
            }
        }
        
        partition.write(records, future);
    }
    
    /**
     * <code>RecordIterator</code> used to read records over multiple partitions.
     */
    private final class PartitionRecordIterator implements RecordIterator {

        /**
         * The time ranges for which data has been requested. 
         */
        private final RangeSet<Field> timeRanges;
        
        /**
         * The filter used to filter the records by type.
         */
        private final Filter<String> recordTypeFilter;
        
        /**
         * The filter used to filter data.
         */
        private final Filter<Record> filter;
        
        /**
         * The iterator over the partitions.
         */
        private final KeyValueIterator<PartitionId, TimeSeriesPartition> partitionIterator; 
        
        /**
         * The record iterator for the current partition been read.
         */
        private RecordIterator recordIterator;
                        
        /**
         * Creates a new <code>PartitionRecordIterator</code> to read records from the specified partitions.
         * 
         * @param timeRanges the time ranges for which data has been requested
         * @param recordTypeFilter the filter used to filter the records by type.
         * @param filter the filter used to filter the returned data
         * @param partitionIterator the partitions.
         */
        public PartitionRecordIterator(RangeSet<Field> rangeSet,
                                       KeyValueIterator<PartitionId, TimeSeriesPartition> partitionIterator,
                                       Filter<String> recordTypeFilter,
                                       Filter<Record> filter) {
            
            this.timeRanges = rangeSet;
            this.recordTypeFilter = recordTypeFilter;
            this.filter = filter;
            this.partitionIterator = partitionIterator;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            
            closeRecordIteratorIfNeeded();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() throws IOException {

            if (this.recordIterator != null && this.recordIterator.hasNext()) {
                return true;
            }
            
            closeRecordIteratorIfNeeded();
            
            while (this.partitionIterator.next()) {
                
                Range<Field> range = this.partitionIterator.getKey().getRange();
                
                RangeSet<Field> subRangeSet = this.timeRanges.subRangeSet(range);
                
                if (!subRangeSet.isEmpty()) {

                    TimeSeriesPartition partition = this.partitionIterator.getValue();
                    this.recordIterator = partition.read(subRangeSet, this.recordTypeFilter, this.filter);

                    if (this.recordIterator.hasNext()) {
                        return true;
                    }
                }
            }    
            
            return false;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Record next() throws IOException {

            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            
            return this.recordIterator.next();
        }
        
        /**
         * Closes the record iterator if it is not <code>closed</code> yet.
         * 
         * @throws IOException if an I/O problem occurs.
         */
        private void closeRecordIteratorIfNeeded() throws IOException {
            if (this.recordIterator != null) {
                this.recordIterator.close();
            }
        }
    }
}
