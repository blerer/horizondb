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
import io.horizondb.db.OperationContext;
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.db.util.concurrent.FutureUtils;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.model.TimeRange;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.iterators.BinaryTimeSeriesRecordIterator;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

/**
 * @author Benjamin
 * 
 */
public final class TimeSeries {

    /**
     * The database name.
     */
    private final String databaseName;
    
    private final TimeSeriesDefinition definition;

    /**
     * The partition manager.
     */
    private final TimeSeriesPartitionManager partitionManager;

    /**
     * @param definition
     */
    public TimeSeries(String databaseName, TimeSeriesPartitionManager partitionManager, TimeSeriesDefinition definition) {

        this.databaseName = databaseName;
        this.partitionManager = partitionManager;
        this.definition = definition;
    }

    /**
     * @return
     */
    public TimeSeriesDefinition getDefinition() {

        return this.definition;
    }

    public void write(OperationContext context, TimeRange partitionTimeRange, ReadableBuffer buffer) throws IOException,
                                                                                               HorizonDBException {

        PartitionId partitionId = new PartitionId(this.databaseName,
                                                  this.definition.getName(),
                                                  partitionTimeRange.getStart());

        TimeSeriesPartition partition = this.partitionManager.getPartitionForWrite(partitionId, this.definition);
            
        if (context.isReplay()) {
            
            final ReplayPosition currentReplayPosition = FutureUtils.safeGet(context.getFuture());
            final ReplayPosition partitionReplayPosition = FutureUtils.safeGet(partition.getFuture());
            
            if (!currentReplayPosition.isAfter(partitionReplayPosition)) {
                
                return;
            }
        }
        
        BinaryTimeSeriesRecordIterator iterator = new BinaryTimeSeriesRecordIterator(this.definition, buffer);
        
        partition.write(iterator, context.getFuture());
    }

    /**
     * Returns the records of this time series that belong to the specified time range.
     * 
     * @param timeRange the time range for which the data must be read
     * @throws IOException if an I/O problem occurs
     * @throws HorizonDBException if another problem occurs
     */
    public RecordIterator read(TimeRange timeRange) throws IOException, HorizonDBException {

        TimeRange partitionTimeRange = this.definition.getPartitionTimeRange(timeRange.getStart());
        
        PartitionId partitionId = new PartitionId(this.databaseName, 
                                                  this.definition.getName(), 
                                                  partitionTimeRange.getStart());
        
        TimeSeriesPartition partition = this.partitionManager.getPartitionForRead(partitionId,
                                                                                  this.definition);

        return partition.read(timeRange);
    }
}
