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
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.db.util.concurrent.FutureUtils;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.io.files.SeekableFileDataOutput;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.fields.TimestampField;
import io.horizondb.model.core.iterators.BinaryTimeSeriesRecordIterator;
import io.horizondb.model.core.iterators.LoggingRecordIterator;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.BlockPosition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Benjamin
 * 
 */
@Immutable
final class MemTimeSeries implements TimeSeriesElement {

    /**
     * The database configuration.
     */
    private final Configuration configuration;

    /**
     * The data blocks used to store the in memory data.
     */
    private final DataBlocks blocks;

    /**
     * The last records for each type.
     */
    private final TimeSeriesRecord[] lastRecords;

    /**
     * The future associated to the first write.
     */
    private final ListenableFuture<ReplayPosition> firstFuture;
    
    /**
     * The future associated to the latest write.
     */
    private final ListenableFuture<ReplayPosition> lastFuture;

    /**
     * The range of the regions used by this <code>MemTimeSeries</code>.
     */
    private final Range<Integer> regionRange;

    /**
	 * 
	 */
    public MemTimeSeries(Configuration configuration, TimeSeriesDefinition definition) {

        this(configuration,
             new TimeSeriesRecord[definition.getNumberOfRecordTypes()],
             new DataBlocks(definition),
             null,
             null,
             null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListenableFuture<ReplayPosition> getFuture() {

        return this.lastFuture;
    }

    /**
     * Writes the specified records.
     * 
     * @param allocator the slab allocator used to reduce heap fragmentation
     * @param records the records to write
     * @param future the future returning the <code>ReplayPosition</code> for this write.
     * @return the number of records written.
     * @throws IOException if an I/O problem occurs while writing the records.
     * @throws HorizonDBException if the one of the records is invalid
     */
    public MemTimeSeries write(SlabAllocator allocator, 
                               List<? extends Record> records, 
                               ListenableFuture<ReplayPosition> future) 
                                       throws IOException, HorizonDBException {

        TimeSeriesRecord[] copy = TimeSeriesRecord.deepCopy(this.lastRecords);
        
        DataBlocks newBlocks = this.blocks.write(allocator, copy, records);

        return new MemTimeSeries(this.configuration, 
                                 copy, 
                                 newBlocks, 
                                 getFirstFuture(future), 
                                 future, 
                                 getNewRegionRange(allocator));
    }

    /**
     * Returns <code>true</code> if this <code>MemTimeSeries</code> is full.
     * 
     * @return <code>true</code> if this <code>MemTimeSeries</code> is full.
     * @throws IOException if an I/O problem occurs while computing the block size
     */
    public boolean isFull() throws IOException {

        return this.getRecordsSize() >= this.configuration.getMemTimeSeriesSize();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SeekableFileDataInput newInput() throws IOException {
        return newInput(TimestampField.ALL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SeekableFileDataInput newInput(RangeSet<Field> rangeSet) throws IOException {
        return this.blocks.newInput(rangeSet);
    }

    /**
     * Returns the range of regions used by this <code>MemTimeSeries</code>.
     * 
     * @return the range of regions used by this <code>MemTimeSeries</code>.
     */
    public Range<Integer> getRegionUsage() {

        return this.regionRange;
    }

    /**
     * Returns the ID of the first commit log segment which contains data of this <code>MemTimeSeries</code>.
     * 
     * @return the ID of the first commit log segment which contains data of this <code>MemTimeSeries</code>.
     */
    public long getFirstSegmentId() {
        
        return FutureUtils.safeGet(this.firstFuture).getSegment(); 
    }
    
    /**
     * Writes the content of this <code>MemTimeSeries</code> in a readable format into the specified stream.
     * 
     * @param definition the time series definition
     * @param stream the stream into which the record representation must be written
     * @throws IOException if an I/O problem occurs
     */
    public void writePrettyPrint(TimeSeriesDefinition definition, PrintStream stream) throws IOException {
        
        try (RecordIterator iter = new LoggingRecordIterator(definition,
                                                             new BinaryTimeSeriesRecordIterator(definition, newInput()),
                                                             stream)) {
            while (iter.hasNext()) {

                iter.next();
            }
        }
    }
    
    /**
     * Returns the number of data blocks that contains this <code>MemTimeSeries</code>.  
     * 
     * @return the number of data blocks that contains this <code>MemTimeSeries</code>.  
     */
    public int getNumberOfBlocks() {
        return this.blocks.getNumberOfBlocks();
    }
    
    /**
     * Writes the content of this <code>MemTimeSeries</code> to the specified output.
     * 
     * @param blockPositions the collecting parameter for the block positions
     * @param output the output to write to
     * @throws IOException if an I/O problem occurs
     */
     void writeTo(Map<Range<Field>, BlockPosition> blockPositions, SeekableFileDataOutput output) throws IOException {
        
        this.blocks.writeTo(blockPositions, output);
    }
    
    /**
     * Returns the greatest timestamp of this time series element.
     * 
     * @return the greatest timestamp of this time series element.
     * @throws IOException if an I/O problem occurs while retrieving the greatest timestamp
     */
    long getGreatestTimestamp() throws IOException {

        return getGreatestTimestamp(this.lastRecords);
    }
    
    /**
     * Returns the future of the first set of records written to this <code>MemTimeSeries</code>.
     * 
     * @param future the future of the last record set written
     * @return the future of the first set of records written to this <code>MemTimeSeries</code>
     */
    private ListenableFuture<ReplayPosition> getFirstFuture(ListenableFuture<ReplayPosition> future) {

        if (this.firstFuture == null) {
            
            return future;
        }
        
        return this.firstFuture;
    }

    /**
     * Returns the greatest timestamp of the specified records.
     * 
     * @param records the records
     * @return the greatest timestamp of this time series element.
     * @throws IOException if an I/O problem occurs while retrieving the greatest timestamp
     */
    private static long getGreatestTimestamp(TimeSeriesRecord[] records) throws IOException {

        long maxTimeStamp = 0;

        for (int i = 0, m = records.length; i < m; i++) {

            TimeSeriesRecord record = records[i];

            if (record == null) {

                continue;
            }

            maxTimeStamp = Math.max(maxTimeStamp, record.getTimestampInNanos(0));
        }

        return maxTimeStamp;
    }

    /**
     * Returns the size in bytes of the records.
     * 
     * @return the size in bytes of the records.
     * @throws IOException if an I/O problem occurs while computing the block size
     */
    private int getRecordsSize() throws IOException {
        return this.blocks.size();
    }

    /**
     * Returns the new range of region that is being used.
     * 
     * @param allocator the slab allocator
     * @return the new range of region that is being used.
     */
    private Range<Integer> getNewRegionRange(SlabAllocator allocator) {

        Integer regionCount = Integer.valueOf(allocator.getRegionCount());

        if (this.regionRange == null) {

            return Range.closed(regionCount, regionCount);
        }

        if (this.regionRange.contains(regionCount)) {

            return this.regionRange;
        }

        return Range.closed(this.regionRange.lowerEndpoint(), regionCount);
    }

    /**
	 * 
	 */
    private MemTimeSeries(Configuration configuration,
            TimeSeriesRecord[] lastRecords,
            DataBlocks blocks,
            ListenableFuture<ReplayPosition> firstFuture,
            ListenableFuture<ReplayPosition> lastFuture,
            Range<Integer> regionRange) {

        this.configuration = configuration;
        this.lastRecords = lastRecords;
        this.blocks = blocks;
        this.firstFuture = firstFuture;
        this.lastFuture = lastFuture;
        this.regionRange = regionRange;
    }
}
