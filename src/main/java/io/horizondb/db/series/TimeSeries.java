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
import io.horizondb.io.ReadableBuffer;
import io.horizondb.model.BinaryTimeSeriesRecordIterator;
import io.horizondb.model.PartitionId;
import io.horizondb.model.Query;
import io.horizondb.model.RecordIterator;
import io.horizondb.model.TimeSeriesDefinition;

import java.io.IOException;

/**
 * @author Benjamin
 * 
 */
public final class TimeSeries {

    private final TimeSeriesDefinition definition;

    /**
     * The partition manager.
     */
    private final TimeSeriesPartitionManager partitionManager;

    /**
     * @param definition
     */
    public TimeSeries(TimeSeriesPartitionManager partitionManager, TimeSeriesDefinition definition) {

        this.partitionManager = partitionManager;
        this.definition = definition;
    }

    /**
     * @return
     */
    public TimeSeriesDefinition getDefinition() {

        return this.definition;
    }

    public void write(OperationContext context, long partitionStartTime, ReadableBuffer buffer) throws IOException,
                                                                                               HorizonDBException {

        PartitionId partitionId = new PartitionId(this.definition.getDatabaseName(),
                                                  this.definition.getSeriesName(),
                                                  partitionStartTime);

        TimeSeriesPartition partition = this.partitionManager.getPartitionForWrite(partitionId, this.definition);

        BinaryTimeSeriesRecordIterator recordSet = new BinaryTimeSeriesRecordIterator(this.definition, buffer);

        partition.write(recordSet, context.getFuture());

    }

    /**
     * @param partition
     * @param timeRange
     * @throws IOException
     */
    public RecordIterator read(Query query) throws IOException {

        TimeSeriesPartition partition = this.partitionManager.getPartitionForRead(query.getPartitionId(),
                                                                                  this.definition);

        return partition.read(query.getTimeRange());
    }
}
