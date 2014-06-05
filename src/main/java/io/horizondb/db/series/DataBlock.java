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

import io.horizondb.db.HorizonDBException;
import io.horizondb.io.Buffer;
import io.horizondb.io.BufferAllocator;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.buffers.CompositeBuffer;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.io.files.SeekableFileDataInputs;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordUtils;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.Range;

import static io.horizondb.io.encoding.VarInts.computeUnsignedIntSize;
import static io.horizondb.io.encoding.VarInts.writeByte;
import static io.horizondb.io.encoding.VarInts.writeUnsignedInt;
import static io.horizondb.model.core.records.BlockHeaderUtils.getRecordCount;
import static io.horizondb.model.core.records.BlockHeaderUtils.incrementRecordCount;
import static io.horizondb.model.core.records.BlockHeaderUtils.setBlockSize;
import static io.horizondb.model.core.records.BlockHeaderUtils.setFirstTimestamp;
import static io.horizondb.model.core.records.BlockHeaderUtils.setLastTimestamp;

/**
 * A block of records that is self contains.
 * 
 * @author Benjamin
 *
 */
@Immutable
final class DataBlock {

    /**
     * The block header.
     */
    private final TimeSeriesRecord header;
    
    /**
     * The composite buffer used to store the in memory data.
     */
    private final CompositeBuffer compositeBuffer;

    /**
     * The time range that contains this block.
     */
    private final Range<Field> range;
        
    /**
     * Creates a new <code>DataBlock</code>.
     * 
     * @param definition the time series definition
     */
    public DataBlock(TimeSeriesDefinition definition) {
        this(definition.newBlockHeader(), new CompositeBuffer());
    }   
    
    /**
     * Returns the time range that contains this block.
     * 
     * @return the time range that contains this block.
     */
    public Range<Field> getRange() {
        return this.range;
    }

    /**
     * Writes the specified records.
     * 
     * @param allocator the slab allocator used to reduce heap fragmentation
     * @param previousRecords the current values of the records
     * @param records the records to write
     * @return the number of records written.
     * @throws IOException if an I/O problem occurs while writing the records.
     * @throws HorizonDBException if the one of the records is invalid
     */
    public DataBlock write(BufferAllocator allocator, 
                           TimeSeriesRecord[] previousRecords,
                           List<? extends Record> records) throws IOException, HorizonDBException {

        CompositeBuffer buffer = this.compositeBuffer.duplicate();
        TimeSeriesRecord newHeader = this.header.newInstance(); 

        for (int i = 0, m = records.size(); i < m; i++) {

            write(allocator, previousRecords, newHeader, buffer, records.get(i));
        }

        return new DataBlock(newHeader, buffer);
    }
    
    /**
     * Returns the number of records of the specified type.
     * 
     * @param type the record type
     * @return the number of records of the specified type
     */
    public int getNumberOfRecords(int type) {
        return getRecordCount(this.header, type);
    }
    
    /**
     * Returns a new <code>SeekableFileDataInput</code> instance to read the content of this data block.
     * 
     * @return a new <code>SeekableFileDataInput</code> instance to read the content of this data block
     * @throws IOException if an I/O problem occurs
     */
    public SeekableFileDataInput newInput() throws IOException {

        return SeekableFileDataInputs.toSeekableFileDataInput(Buffers.composite(toBuffer(this.header), 
                                                                                this.compositeBuffer.duplicate()));
    }
    
    /**
     * Returns the current block size (including the header).
     * 
     * @return the current block size (including the header).
     */
    public int size() {
        
        return this.compositeBuffer.readableBytes() + RecordUtils.computeSerializedSize(this.header);
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
     * Writes the specified record data to the specified buffer.
     * 
     * @param allocator the slab allocator used to reduce heap fragmentation
     * @param previousRecords the previous value of the records
     * @param header the block header
     * @param composite the composite buffer to which the records binaries must be added
     * @param record the record to be written
     * @throws IOException if an I/O problem occurs
     * @throws HorizonDBException if the record is not valid
     */
    private static void write(BufferAllocator allocator,
                              TimeSeriesRecord[] previousRecords,
                              TimeSeriesRecord header,
                              CompositeBuffer composite,
                              Record record) throws IOException, HorizonDBException {

        int type = record.getType();

        if (previousRecords[type] == null) {

            if (record.isDelta()) {

                throw new HorizonDBException(ErrorCodes.INVALID_RECORD_SET,
                                             "The first record of the record set is a delta and should be a full state.");
            }

            previousRecords[type] = record.toTimeSeriesRecord();
            performWrite(allocator, header, composite, record);

        } else {

            if (record.isDelta()) {

                previousRecords[type].add(record);
                
                if (getRecordCount(header, record.getType()) == 0) {
                    
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
                performWrite(allocator, header, composite, delta);
            }
        }
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
                                     Record record) 
                                     throws IOException {

        Buffer buffer = serializeRecord(allocator, record);

        if (!composite.isReadable()) {
            setFirstTimestamp(header, record);
        } 
            
        setLastTimestamp(header, record);
        incrementRecordCount(header, record.getType());
        
        composite.add(buffer);
                
        setBlockSize(header, composite.readableBytes());
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
     * Creates a new <code>DataBlock</code> with the specified content.
     * 
     * @param header the block header
     * @param compositeBuffer the buffer containing the data
     */
    private DataBlock(TimeSeriesRecord header, CompositeBuffer compositeBuffer) {
        
        this.header = header;
        this.compositeBuffer = compositeBuffer;
        
        Field from = header.getField(0).newInstance();
        Field to = header.getField(1).newInstance();
        to.add(from);
        
        this.range = Range.closed(from, to);
    }

}
