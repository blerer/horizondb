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

import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.db.util.concurrent.FutureUtils;
import io.horizondb.io.Buffer;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.files.CompositeSeekableFileDataInput;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.io.files.SeekableFileDataInputs;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.core.blocks.RecordAppender;
import io.horizondb.model.core.fields.TimestampField;
import io.horizondb.model.core.iterators.BinaryTimeSeriesRecordIterator;
import io.horizondb.model.core.iterators.BlockIterators;
import io.horizondb.model.core.iterators.LoggingRecordIterator;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.ListenableFuture;

import static io.horizondb.model.core.records.BlockHeaderUtils.*;

/**
 * In-memory part of a time series used to store the writes until they are
 * flushed to the disk.
 * 
 */
@Immutable
final class MemTimeSeries implements TimeSeriesElement {

    /**
     * The database configuration.
     */
    private final Configuration configuration;

    /**
     * The time series definition.
     */
    private final TimeSeriesDefinition definition;

    /**
     * The data blocks
     */
    private final List<DataBlock> blocks;

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
             definition,
             Collections.<DataBlock>emptyList(),
             new TimeSeriesRecord[definition.getNumberOfRecordTypes()],
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

        List<DataBlock> newBlocks = new ArrayList<>(this.blocks);
        TimeSeriesRecord[] previousRecords = TimeSeriesRecord.deepCopy(this.lastRecords);

        RecordAppender appender;
        
        if (newBlocks.isEmpty()) {

            appender = new RecordAppender(this.definition,
                                                    allocator,
                                                    previousRecords);
        } else {

            DataBlock lastBlock = newBlocks.remove(newBlocks.size() - 1);
            appender = new RecordAppender(this.definition,
                                                    allocator,
                                                    previousRecords,
                                                    lastBlock);
        }

        for (int i = 0, m = records.size(); i < m; i++) {

            while (!appender.append(records.get(i))) {

                newBlocks.add(appender.getDataBlock());
                appender = new RecordAppender(this.definition,
                                                        allocator,
                                                        this.lastRecords);
            }
        }
        newBlocks.add(appender.getDataBlock());

        return new MemTimeSeries(this.configuration,
                                 this.definition,
                                 newBlocks,
                                 previousRecords,
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

        if (this.blocks.isEmpty()) {
            return SeekableFileDataInputs.empty();
        }

        CompositeSeekableFileDataInput composite = new CompositeSeekableFileDataInput();
        for (int i = 0, m = this.blocks.size(); i < m; i++) {
            DataBlock block = this.blocks.get(i);

            Record header = block.getHeader();
            Range<Field> range = getRange(header);
            if (!rangeSet.subRangeSet(range).isEmpty()) {
                ReadableBuffer buffer = toBuffer(header);
                ReadableBuffer blockBuffer = Buffers.composite(buffer,
                                                               block.getData());
                SeekableFileDataInput seekableFileDataInput = SeekableFileDataInputs.toSeekableFileDataInput(blockBuffer);
                composite.add(seekableFileDataInput);
            }
        }
        return composite;
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
        
        try (ResourceIterator<Record> iter = new LoggingRecordIterator(definition,
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
        return this.blocks.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceIterator<DataBlock> iterator() {
        return BlockIterators.iterator(this.blocks);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceIterator<DataBlock> iterator(RangeSet<Field> rangeSet) throws IOException {
        return BlockIterators.filter(rangeSet, iterator());
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
        int size = 0;
        for (int i = 0, m = this.blocks.size(); i < m; i++) {
            Record header = this.blocks.get(i).getHeader();
            size += getUncompressedBlockSize(header);
        }
        return size;
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
     * Serializes the specified block header.
     *
     * @param header the header to serialize
     * @return the buffer containing the serialized header
     * @throws IOException if an I/O problem occurs
     */
    private static ReadableBuffer toBuffer(Record header) throws IOException {

        int headerSize = header.computeSerializedSize();

        int size = 1 + VarInts.computeUnsignedIntSize(headerSize) + headerSize;

        Buffer buffer = Buffers.allocate(size);

        buffer.writeByte(Record.BLOCK_HEADER_TYPE);
        VarInts.writeUnsignedInt(buffer, headerSize);
        header.writeTo(buffer);

        return buffer;
    }

    /**
	 * 
	 */
    private MemTimeSeries(Configuration configuration,
                           TimeSeriesDefinition definition,
                           List<DataBlock> blocks,
                           TimeSeriesRecord[] lastRecords,
                           ListenableFuture<ReplayPosition> firstFuture,
                           ListenableFuture<ReplayPosition> lastFuture,
                           Range<Integer> regionRange) {

        this.configuration = configuration;
        this.definition = definition;
        this.blocks = blocks;
        this.lastRecords = lastRecords;
        this.firstFuture = firstFuture;
        this.lastFuture = lastFuture;
        this.regionRange = regionRange;
    }
}
