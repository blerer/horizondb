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

import io.horizondb.db.Component;
import io.horizondb.db.HorizonDBException;
import io.horizondb.model.PartitionId;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

/**
 * @author Benjamin
 * 
 */
public interface TimeSeriesPartitionManager extends Component {

    /**
     * Saves on disk state of the specified partition.
     * 
     * @param partition the partition for which the state must be saved.
     * @throws IOException if an I/O problem occurs
     */
    void save(TimeSeriesPartition partition) throws IOException;

    /**
     * Returns the partition with the specified ID from the specified time series for read access.
     * 
     * @param PartitionId partitionId the partition id
     * @param seriesDefinition the time series definition
     * @return the time series partition with the specified ID.
     * @throws IOException if an I/O problem occurs while retrieving the partition.
     * @throws HorizonDBException if a problem occurs.
     */
    TimeSeriesPartition getPartitionForRead(PartitionId partitionId, TimeSeriesDefinition seriesDefinition) throws IOException, HorizonDBException;

    /**
     * Returns the partition with the specified ID from the specified time series for write access.
     * 
     * @param PartitionId partitionId the partition id
     * @param seriesDefinition the time series definition
     * @return the time series partition with the specified ID.
     * @throws IOException if an I/O problem occurs while retrieving the partition.
     * @throws HorizonDBException if a problem occurs.
     */
    TimeSeriesPartition getPartitionForWrite(PartitionId partitionId, TimeSeriesDefinition seriesDefinition) throws IOException,
                                                                                                            HorizonDBException;

    /**
     * Flush the specified partition.
     * 
     * @param timeSeriesPartition the partition to flush.
     */
    void flush(TimeSeriesPartition timeSeriesPartition);

    /**
     * Force flush the specified partition.
     * 
     * @param timeSeriesPartition the partition to flush.
     */
    void forceFlush(TimeSeriesPartition timeSeriesPartition);
}
