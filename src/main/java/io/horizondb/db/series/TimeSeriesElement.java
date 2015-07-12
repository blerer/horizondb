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

import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.ResourceIterator;

import java.io.IOException;

import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A time series element. A time series is divided into partition elements ({@link TimeSeriesPartition}).
 * Each time series partition is then divided into several elements: some in memory elements 
 * ({@link MemTimeseries}) and a on disk element (@link {@link TimeSeriesFile}).
 * 
 */
interface TimeSeriesElement {

    /**
     * Returns the commit log future returning the replay position associated to the last record of this element.
     * 
     * @return the commit log future returning the replay position associated to the last record of this element.
     */
    ListenableFuture<ReplayPosition> getFuture();

    /**
     * Returns a new iterator over all the blocks of this element.
     * 
     * @param rangeSet the time range for which the blocks must be returned
     * @return a new iterator that can be used to read all the data of this element.
     * @throws IOException if an I/O problem occurs.
     */
    ResourceIterator<DataBlock> iterator() throws IOException;

    /**
     * Returns a new iterator over the blocks of this element containing the specified time range.
     * 
     * @param rangeSet the time range for which the blocks must be returned
     * @return a new iterator that can be used to read the data of this element.
     * @throws IOException if an I/O problem occurs.
     */
    ResourceIterator<DataBlock> iterator(RangeSet<Field> rangeSet) throws IOException;
}
