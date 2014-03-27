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

/**
 * Listener called when the memory usage or the first commit log segment containing non persisted data change.
 * 
 * @author Benjamin
 * 
 */
interface TimeSeriesPartitionListener {
    
    /**
     * Notification that the memory usage of the specified partition did change.
     * 
     * @param partition the partition that has changed its memory usage
     * @param previousMemoryUsage the previous memory usage
     * @param newMemoryUsage the new memory usage
     */
    void memoryUsageChanged(TimeSeriesPartition partition, int previousMemoryUsage, int newMemoryUsage);
    
    /**
     * Notification that the first segment containing non persisted data did change.
     * 
     * @param partition the partition for which the first segment containing non persisted data changed
     * @param previousSegment the ID of the previous segment
     * @param newSegment the ID of the new segment
     */
    void firstSegmentContainingNonPersistedDataChanged(TimeSeriesPartition partition, 
                                                       Long previousSegment, 
                                                       Long newSegment);
}
