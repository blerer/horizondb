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
import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.CompositeBuffer;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.io.files.SeekableFileDataInputs;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.ListenableFuture;

import static io.horizondb.io.encoding.VarInts.computeUnsignedIntSize;
import static io.horizondb.io.encoding.VarInts.writeByte;
import static io.horizondb.io.encoding.VarInts.writeUnsignedInt;

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
     * The composite buffer used to store the in memory data.
     */
    private final CompositeBuffer compositeBuffer;

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
             new CompositeBuffer(),
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
     * @param iterator the iterator containing the records
     * @param future the future returning the <code>ReplayPosition</code> for this write.
     * @return the number of records written.
     * @throws IOException if an I/O problem occurs while writing the records.
     * @throws HorizonDBException if the one of the records is invalid
     */
    public MemTimeSeries write(SlabAllocator allocator, 
                               RecordIterator iterator, 
                               ListenableFuture<ReplayPosition> future) 
                                       throws IOException, HorizonDBException {

        CompositeBuffer buffer = this.compositeBuffer.duplicate();
        TimeSeriesRecord[] copy = TimeSeriesRecord.deepCopy(this.lastRecords);

        while (iterator.hasNext()) {

            write(allocator, copy, buffer, iterator.next());
        }

        return new MemTimeSeries(this.configuration, 
                                 copy, 
                                 buffer, 
                                 getFirstFuture(future), 
                                 future, 
                                 getNewRegionRange(allocator));
    }

    /**
     * Returns <code>true</code> if this <code>MemTimeSeries</code> is full.
     * 
     * @return <code>true</code> if this <code>MemTimeSeries</code> is full.
     */
    public boolean isFull() {

        return this.getRecordsSize() >= this.configuration.getMemTimeSeriesSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SeekableFileDataInput newInput() throws IOException {
        return SeekableFileDataInputs.toSeekableFileDataInput(this.compositeBuffer);
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
    public long getFirstCommitLogSegmentId() {
        
        return FutureUtils.safeGet(this.firstFuture).getSegment(); 
    }
    
    /**
     * Writes the specified record data to the specified buffer.
     * 
     * @param allocator the slab allocator used to reduce heap fragmentation.
     * @param previousRecords the previous value of the records.
     * @param composite the composite buffer to which the records binaries must be added.
     * @param record the record to be written.
     * @throws IOException if an I/O problem occurs.
     * @throws HorizonDBException if the record is not valid.
     */
    private static void write(SlabAllocator allocator,
                              TimeSeriesRecord[] previousRecords,
                              CompositeBuffer composite,
                              Record record) throws IOException, HorizonDBException {

        int type = record.getType();

        if (previousRecords[type] == null) {

            if (record.isDelta()) {

                throw new HorizonDBException(ErrorCodes.INVALID_RECORD_SET,
                                             "The first record of the record set is a delta and should be a full state.");
            }

            previousRecords[type] = record.toTimeSeriesRecord();
            performWrite(allocator, composite, record);

        } else {

            if (record.isDelta()) {

                previousRecords[type].add(record);
                performWrite(allocator, composite, record);

            } else {

                if (record.getTimestampInNanos(0) < getGreatestTimestamp(previousRecords)) {

                    throw new HorizonDBException(ErrorCodes.INVALID_RECORD_SET,
                                                 "The first record of the record set as a timestamp earliest than"
                                                         + " the last record written."
                                                         + " Updates are not yet supported.");
                }

                TimeSeriesRecord delta = toDelta(previousRecords[type], record);
                previousRecords[type].add(delta);
                performWrite(allocator, composite, delta);
            }
        }
    }

    /**
     * Computes the delta between the two specified records.
     * 
     * @param first the first record
     * @param second the second record
     * @return the delta between the two specified records.
     * @throws IOException if an I/O problem occurs while computing the delta
     */
    private static TimeSeriesRecord toDelta(TimeSeriesRecord first, Record second) throws IOException {

        TimeSeriesRecord delta = second.toTimeSeriesRecord();
        delta.subtract(first);
        return delta;
    }

    /**
     * Adds the specified record to the specified composite.
     * 
     * @param allocator the slab allocator
     * @param composite the composite buffer to which the record must bee added
     * @param record the record to add
     * @throws IOException if an I/O problem occurs
     */
    private static void
            performWrite(SlabAllocator allocator, CompositeBuffer composite, Record record) throws IOException {

        int recordSize = record.computeSerializedSize();
        int totalSize = 1 + computeUnsignedIntSize(recordSize) + recordSize;

        Buffer buffer = allocator.allocate(totalSize);

        writeByte(buffer, record.getType());
        writeUnsignedInt(buffer, recordSize);
        record.writeTo(buffer);

        composite.add(buffer);
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
     */
    private int getRecordsSize() {
        return this.compositeBuffer.readableBytes();
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
            CompositeBuffer buffer,
            ListenableFuture<ReplayPosition> firstFuture,
            ListenableFuture<ReplayPosition> lastFuture,
            Range<Integer> regionRange) {

        this.configuration = configuration;
        this.lastRecords = lastRecords;
        this.compositeBuffer = buffer;
        this.firstFuture = firstFuture;
        this.lastFuture = lastFuture;
        this.regionRange = regionRange;
    }
}
