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

import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.util.SerializationUtils;
import io.horizondb.model.schema.BlockPosition;

import java.io.IOException;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;

import static io.horizondb.model.core.util.SerializationUtils.writeRange;

import static io.horizondb.model.core.util.SerializationUtils.computeRangeSerializedSize;

/**
 * @author Benjamin
 * 
 */
@Immutable
public final class TimeSeriesPartitionMetaData implements Serializable {

    /**
     * The parser instance.
     */
    private static final Parser<TimeSeriesPartitionMetaData> PARSER = new Parser<TimeSeriesPartitionMetaData>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public TimeSeriesPartitionMetaData parseFrom(ByteReader reader) throws IOException {
            
            Range<Field> range = SerializationUtils.parseRangeFrom(reader);

            ReplayPosition replayPosition = null;

            if (reader.readBoolean()) {

                replayPosition = ReplayPosition.parseFrom(reader);
            }

            long fileSize = VarInts.readUnsignedLong(reader);
            
            int numberOfBlocks = VarInts.readUnsignedInt(reader);
            
            com.google.common.collect.ImmutableRangeMap.Builder<Field, BlockPosition> builder = ImmutableRangeMap.builder();
            
            for (int i = 0; i < numberOfBlocks; i++) {
                
                Range<Field> timeRange = SerializationUtils.parseRangeFrom(reader);
                BlockPosition position = BlockPosition.parseFrom(reader);
                
                builder.put(timeRange, position);
            }

            return new TimeSeriesPartitionMetaData(range, builder.build(), replayPosition, fileSize);
        }
    };

    /**
     * The partition time range.
     */
    private final Range<Field> range;

    /**
     * The replay position of the latest data written on the disk.
     */
    private final ReplayPosition replayPosition;

    /**
     * The expected file size
     */
    private final long fileSize;
    
    /**
     * The positions of the block within the file.
     */
    private final RangeMap<Field, BlockPosition> blockPositions;

    /**
     * Returns the partition time range.
     * 
     * @return the partition time range.
     */
    public Range<Field> getRange() {
        return this.range;
    }

    /**
     * Return the replay position of the latest data written on the disk.
     * 
     * @return the replay position of the latest data written on the disk
     */
    public ReplayPosition getReplayPosition() {
        return this.replayPosition;
    }

    /**
     * Returns the expected file size.
     * 
     * @return the expected file size
     */
    public long getFileSize() {
        return this.fileSize;
    }
    
    /**
     * Returns the block positions.
     * 
     * @return the block positions.
     */
    public RangeMap<Field, BlockPosition> getBlockPositions() {
        return this.blockPositions;
    }

    /**
     * Creates a new <code>TimeSeriesPartitionMetaData</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static TimeSeriesPartitionMetaData parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>TimeSeriesPartitionMetaData</code> instances.
     * 
     * @return the parser that can be used to deserialize <code>TimeSeriesPartitionMetaData</code> instances.
     */
    public static Parser<TimeSeriesPartitionMetaData> getParser() {

        return PARSER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {

        int size = computeRangeSerializedSize(this.range)
                + VarInts.computeUnsignedLongSize(this.fileSize) + 1;

        if (this.replayPosition != null) {

            size += this.replayPosition.computeSerializedSize();
        }

        Map<Range<Field>, BlockPosition> map = this.blockPositions.asMapOfRanges();
        
        int numberOfBlocks = map.size();
        
        size += VarInts.computeUnsignedIntSize(numberOfBlocks);
        
        for (Map.Entry<Range<Field>, BlockPosition> entry : map.entrySet()) {
            
            size += SerializationUtils.computeRangeSerializedSize(entry.getKey());
            size += entry.getValue().computeSerializedSize();
        }
                
        return size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        writeRange(writer, this.range);

        if (this.replayPosition == null) {

            writer.writeBoolean(false);

        } else {

            writer.writeBoolean(true).writeObject(this.replayPosition);
        }

        VarInts.writeUnsignedLong(writer, this.fileSize);
        
        Map<Range<Field>, BlockPosition> map = this.blockPositions.asMapOfRanges();
        
        int numberOfBlocks = map.size();
        
        VarInts.writeUnsignedInt(writer, numberOfBlocks);
        
        for (Map.Entry<Range<Field>, BlockPosition> entry : map.entrySet()) {
            
            SerializationUtils.writeRange(writer, entry.getKey());
            entry.getValue().writeTo(writer);
        }    
    }

    /**
     * Creates a new <code>Builder</code> instance.
     * 
     * @return a new <code>Builder</code> instance.
     */
    public static Builder newBuilder(Range<Field> range) {

        return new Builder(range);
    }

    /**
     * Creates a new <code>TimeSeriesPartitionMetaData</code> instance using the specified <code>Builder</code>.
     * 
     * @param Builder return a new <code>TimeSeriesPartitionMetaData</code> instance.
     */
    private TimeSeriesPartitionMetaData(Builder builder) {

        this(builder.range, builder.blockPositions, builder.replayPosition, builder.fileSize);
    }

    /**
     * Creates a new <code>TimeSeriesPartitionMetaData</code> instance using the specified <code>Builder</code>.
     * 
     * @param range the partition time range
     * @param blockPositions the positions of the block 
     * @param replayPosition the replay position of the latest data written on the disk
     * @param fileSize the expected file size.
     */
    private TimeSeriesPartitionMetaData(Range<Field> range, 
                                        RangeMap<Field, BlockPosition> blockPositions, 
                                        ReplayPosition replayPosition, 
                                        long fileSize) {

        this.range = range;
        this.blockPositions = blockPositions;
        this.replayPosition = replayPosition;
        this.fileSize = fileSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("range", this.range)
                                                                          .append("blockPositions", this.blockPositions)
                                                                          .append("replayPosition", this.replayPosition)
                                                                          .append("fileSize", this.fileSize)
                                                                          .toString();
    }

    /**
     * Builds instance of <code>TimeSeriesPartitionMetaData</code>.
     * 
     */
    public static class Builder {

        /**
         * The partition time range.
         */
        private final Range<Field> range;

        /**
         * The replay position of the latest data written on the disk.
         */
        private ReplayPosition replayPosition;
        
        /**
         * The positions of the blocks.
         */
        private RangeMap<Field, BlockPosition> blockPositions = ImmutableRangeMap.of();
        
        /**
         * The expected file size.
         */
        private long fileSize = 0;

        /**
         * Creates a new <code>TimeSeriesPartitionMetaData</code> instance.
         * 
         * @return a new <code>TimeSeriesPartitionMetaData</code> instance.
         */
        public TimeSeriesPartitionMetaData build() {

            return new TimeSeriesPartitionMetaData(this);
        }

        /**
         * Specifies the block positions.
         * 
         * @param blockPositions the block positions
         * @return this <code>Builder</code>
         */
        public Builder blockPositions(RangeMap<Field, BlockPosition> blockPositions) {
            
            this.blockPositions = blockPositions;
            return this;
        }
        
        /**
         * Sets the expected file size
         * 
         * @param fileSize the expected file size
         * @return this <code>Builder</code>
         */
        public Builder fileSize(long fileSize) {

            this.fileSize = fileSize;

            return this;
        }

        /**
         * Sets the replay position of the latest data written on the disk.
         * 
         * @param replayPosition the replay position of the latest data written on the disk
         * @return this <code>Builder</code>
         */
        public Builder replayPosition(ReplayPosition replayPosition) {

            this.replayPosition = replayPosition;

            return this;
        }

        /**
         * Must not be called from outside the enclosing class.
         */
        private Builder(Range<Field> range) {

            this.range = range;
        }
    }
}
