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
import io.horizondb.io.BufferAllocator;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.buffers.CompositeBuffer;
import io.horizondb.io.compression.CompressionType;
import io.horizondb.io.compression.Compressor;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.files.CompositeSeekableFileDataInput;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.io.files.SeekableFileDataInputs;
import io.horizondb.io.files.SeekableFileDataOutput;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.RecordUtils;
import io.horizondb.model.core.fields.TimestampField;
import io.horizondb.model.core.iterators.BinaryTimeSeriesRecordIterator;
import io.horizondb.model.core.iterators.LoggingRecordIterator;
import io.horizondb.model.core.records.BlockHeaderUtils;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.BlockPosition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.ListenableFuture;

import static io.horizondb.io.encoding.VarInts.computeUnsignedIntSize;
import static io.horizondb.io.encoding.VarInts.writeByte;
import static io.horizondb.io.encoding.VarInts.writeUnsignedInt;
import static io.horizondb.model.core.records.BlockHeaderUtils.getCompressedBlockSize;
import static io.horizondb.model.core.records.BlockHeaderUtils.getRecordCount;
import static io.horizondb.model.core.records.BlockHeaderUtils.incrementRecordCount;
import static io.horizondb.model.core.records.BlockHeaderUtils.setCompressedBlockSize;
import static io.horizondb.model.core.records.BlockHeaderUtils.setCompressionType;
import static io.horizondb.model.core.records.BlockHeaderUtils.setFirstTimestamp;
import static io.horizondb.model.core.records.BlockHeaderUtils.setLastTimestamp;
import static io.horizondb.model.core.records.BlockHeaderUtils.setUncompressedBlockSize;

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
     * The block headers
     */
    private final List<TimeSeriesRecord> headers;

    /**
     * The last records for each type.
     */
    private final TimeSeriesRecord[] lastRecords;

    /**
     * The composite buffer used to store the in memory data.
     */
    private final CompositeBuffer compositeBuffer;

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
             Collections.<TimeSeriesRecord>emptyList(),
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
        List<TimeSeriesRecord> newHeaders = new ArrayList<>(this.headers.size());
        for (int i = 0, m = this.headers.size(); i < m; i++) {
            newHeaders.add(this.headers.get(i).newInstance());
        }
        CompositeBuffer buffer = this.compositeBuffer.duplicate();

        for (int i = 0, m = records.size(); i < m; i++) {
            write(allocator, copy, newHeaders, buffer, records.get(i));
        }

        return new MemTimeSeries(this.configuration,
                                 this.definition,
                                 newHeaders,
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

        if (this.headers.isEmpty()) {
            return SeekableFileDataInputs.empty();
        }

        CompositeSeekableFileDataInput composite = new CompositeSeekableFileDataInput();
        CompositeBuffer dataBuffer = this.compositeBuffer.duplicate().readerIndex(0);
        int offset = 0;
        for (TimeSeriesRecord header : this.headers) {

            Range<Field> range = BlockHeaderUtils.getRange(header);
            int size = BlockHeaderUtils.getCompressedBlockSize(header);

            if (!rangeSet.subRangeSet(range).isEmpty()) {

                ReadableBuffer blockBuffer = Buffers.composite(toBuffer(header),
                                                               dataBuffer.slice(offset, size).duplicate());
                composite.add(SeekableFileDataInputs.toSeekableFileDataInput(blockBuffer));
            }
            offset += size;
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
        return this.headers.size();
    }
    
    /**
     * Writes the content of this <code>MemTimeSeries</code> to the specified output.
     * 
     * @param blockPositions the collecting parameter for the block positions
     * @param output the output to write to
     * @throws IOException if an I/O problem occurs
     */
     void writeTo(Map<Range<Field>, BlockPosition> blockPositions, SeekableFileDataOutput output) throws IOException {

         CompressionType compressionType = this.definition.getCompressionType();
         Compressor compressor = compressionType.newCompressor();

         long position = output.getPosition();
         CompositeBuffer dataBuffer = this.compositeBuffer.duplicate();
         int offset = 0;
         for (TimeSeriesRecord header : this.headers) {

             int blockSize = BlockHeaderUtils.getCompressedBlockSize(header);

             ReadableBuffer compressedData = compressor.compress(dataBuffer.slice(offset, blockSize));

             TimeSeriesRecord newHeader = header.newInstance();
             setCompressionType(newHeader, compressor.getType());
             setCompressedBlockSize(newHeader, compressedData.readableBytes());
             setUncompressedBlockSize(newHeader, blockSize);

             RecordUtils.writeRecord(output, newHeader);
             output.transfer(compressedData);

             long newPosition = output.getPosition();
             int length = (int) (newPosition - position);
             BlockPosition blockPosition = new BlockPosition(position, length);
             blockPositions.put(BlockHeaderUtils.getRange(header), blockPosition);
             position = output.getPosition();

             offset += blockSize;
         }
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
     * Writes the specified record data to the specified buffer.
     *
     * @param allocator the slab allocator used to reduce heap fragmentation
     * @param previousRecords the previous value of the records
     * @param headers the block headers
     * @param composite the composite buffer to which the records binaries must be added
     * @param record the record to be written
     * @throws IOException if an I/O problem occurs
     * @throws HorizonDBException if the record is not valid
     */
    private void write(BufferAllocator allocator,
                       TimeSeriesRecord[] previousRecords,
                       List<TimeSeriesRecord> headers,
                       CompositeBuffer composite,
                       Record record) throws IOException, HorizonDBException {

        int type = record.getType();

        if (previousRecords[type] == null) {

            if (record.isDelta()) {

                throw new HorizonDBException(ErrorCodes.INVALID_RECORD_SET, "The first record of the record set "
                        + "is a delta and should be a full state.");
            }

            previousRecords[type] = record.toTimeSeriesRecord();

            TimeSeriesRecord header = getHeaderForWrite(headers, record.computeSerializedSize());
            performWrite(allocator, header, composite, record);

        } else {

            if (record.isDelta()) {

                previousRecords[type].add(record);

                TimeSeriesRecord header = getHeaderForWrite(headers, record.computeSerializedSize());

                if (getRecordCount(header, record.getType()) == 0) {

                    header = getHeaderForWrite(headers, previousRecords[type].computeSerializedSize());
                    performWrite(allocator, header, composite, previousRecords[type]);

                } else {

                    performWrite(allocator, header, composite, record);
                }

            } else {

                if (record.getTimestampInNanos(0) < getGreatestTimestamp(previousRecords)) {

                    throw new HorizonDBException(ErrorCodes.INVALID_RECORD_SET,
                                                 "The first record of the record set as a timestamp earliest than"
                                                         + " the last record written."
                                                         + " Updates are not yet supported.");
                }

                TimeSeriesRecord delta = toDelta(previousRecords[type], record);
                previousRecords[type].add(delta);

                TimeSeriesRecord header = getHeaderForWrite(headers, delta.computeSerializedSize());
                if (getRecordCount(header, record.getType()) == 0) {

                    header = getHeaderForWrite(headers, previousRecords[type].computeSerializedSize());
                    performWrite(allocator, header, composite, previousRecords[type]);

                } else {

                    performWrite(allocator, header, composite, delta);
                }
            }
        }
    }

    private TimeSeriesRecord getHeaderForWrite(List<TimeSeriesRecord> headers, int dataSize) throws IOException {

        if (!headers.isEmpty()) {
            TimeSeriesRecord header = headers.get(headers.size() - 1);
            if (getCompressedBlockSize(header) + dataSize <= this.definition.getBlockSizeInBytes()) {
                return header;
            }
        }

        TimeSeriesRecord header = this.definition.newBlockHeader();
        headers.add(header);
        return header;
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
     * @param header the block header
     * @param composite the composite buffer to which the record must bee added
     * @param record the record to add
     * @throws IOException if an I/O problem occurs
     */
    private static void performWrite(BufferAllocator allocator,
                                     TimeSeriesRecord header,
                                     CompositeBuffer composite,
                                     Record record) throws IOException {

        Buffer buffer = serializeRecord(allocator, record);
        int recordSize = buffer.readableBytes();

        if (BlockHeaderUtils.getFirstTimestampInNanos(header) == 0) {
            setFirstTimestamp(header, record);
        }

        setLastTimestamp(header, record);
        incrementRecordCount(header, record.getType());

        composite.add(buffer);

        setCompressedBlockSize(header, getCompressedBlockSize(header) + recordSize);
    }

    /**
     * Serializes the specified record.
     * 
     * @param allocator the buffer allocator used to allocate the returned <code>Buffer</code>
     * @param record the record to serialize
     * @return a buffer containing the serialized record
     * @throws IOException if an I/O problem occurs
     */
    private static Buffer serializeRecord(BufferAllocator allocator, Record record) throws IOException {

        int recordSize = record.computeSerializedSize();
        int totalSize = 1 + computeUnsignedIntSize(recordSize) + recordSize;

        Buffer buffer = allocator.allocate(totalSize);

        writeByte(buffer, record.getType());
        writeUnsignedInt(buffer, recordSize);
        record.writeTo(buffer);

        return buffer;
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
        for (int i = 0, m = this.headers.size(); i < m; i++) {
            TimeSeriesRecord header = this.headers.get(i);
            size += BlockHeaderUtils.getUncompressedBlockSize(header);
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
                           List<TimeSeriesRecord> headers,
                           TimeSeriesRecord[] lastRecords,
                           CompositeBuffer compositeBuffer,
                           ListenableFuture<ReplayPosition> firstFuture,
                           ListenableFuture<ReplayPosition> lastFuture,
                           Range<Integer> regionRange) {

        this.configuration = configuration;
        this.definition = definition;
        this.headers = headers;
        this.lastRecords = lastRecords;
        this.compositeBuffer = compositeBuffer;
        this.firstFuture = firstFuture;
        this.lastFuture = lastFuture;
        this.regionRange = regionRange;
    }
}
