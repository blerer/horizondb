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
import io.horizondb.db.HorizonDBFiles;
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.io.files.RandomAccessDataFile;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.io.files.SeekableFileDataInputs;
import io.horizondb.io.files.SeekableFileDataOutput;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.core.fields.TimestampField;
import io.horizondb.model.core.iterators.BlockIterators;
import io.horizondb.model.schema.BlockPosition;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static io.horizondb.model.core.iterators.BlockIterators.compress;
import static io.horizondb.model.core.records.BlockHeaderUtils.getRange;

/**
 * File containing the time series data.
 * 
 */
final class TimeSeriesFile implements Closeable, TimeSeriesElement {

    /**
     * The logger.
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * The file meta data.
     */
    private final FileMetaData metadata;

    /**
     * The time series definition
     */
    private final TimeSeriesDefinition definition;

    /**
     * The block positions
     */
    private final LinkedHashMap<Range<Field>, BlockPosition> blockPositions;

    /**
     * The underlying file.
     */
    private final RandomAccessDataFile file;

    /**
     * The expected file size.
     */
    private final long fileSize;

    /**
     * The future returning the replay position of the data on disk.
     */
    private final ListenableFuture<ReplayPosition> future;

    /**
     * Opens the time series file.
     * 
     * @param configuration the database configuration
     * @param databaseName the database name
     * @param definition the time series definition
     * @param partitionMetadata the partition meta data
     * @return the time series file.
     * @throws IOException if an I/O problem occurs while opening the file.
     */
    public static TimeSeriesFile open(Configuration configuration,
                                      DatabaseDefinition databaseDefinition,
                                      TimeSeriesDefinition definition,
                                      TimeSeriesPartitionMetaData partitionMetadata) throws IOException {

        Path path = getFilePath(configuration, databaseDefinition, definition, partitionMetadata);
        
        RandomAccessDataFile file = RandomAccessDataFile.open(path, false, partitionMetadata.getFileSize());

        FileMetaData fileMetaData;

        if (file.exists() && file.size() != 0) {

            try (SeekableFileDataInput input = file.newInput()) {
                fileMetaData = FileMetaData.parseFrom(input);
            }

        } else {

            fileMetaData = new FileMetaData(databaseDefinition.getName(),
                                            definition.getName(),
                                            partitionMetadata.getRange());
        }

        return new TimeSeriesFile(fileMetaData,
                                  definition,
                                  partitionMetadata.getBlockPositions(),
                                  file,
                                  partitionMetadata.getFileSize(),
                                  Futures.immediateFuture(partitionMetadata.getReplayPosition()));
    }

    /**
     * Returns the file size.
     * 
     * @return the file size.
     */
    public long size() {

        return this.fileSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceIterator<DataBlock> iterator() throws IOException {
        return BlockIterators.iterator(this.definition, newInput());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceIterator<DataBlock> iterator(RangeSet<Field> rangeSet) throws IOException {
        return BlockIterators.iterator(this.definition, newInput(rangeSet));
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
        
        if (this.fileSize == 0) {

            return SeekableFileDataInputs.empty();
        }
        
        List<BlockPosition> blocks = findBlocks(rangeSet.span());
        
        if (blocks.isEmpty()) {
            
            return SeekableFileDataInputs.empty();
        }
                
        BlockPosition block = merge(blocks);
        
        return SeekableFileDataInputs.truncate(this.file.newInput(), 
                                               block.getOffset(), 
                                               block.getLength());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListenableFuture<ReplayPosition> getFuture() {
        return this.future;
    }

    /**
     * Appends the content of the specified <code>memTimeSeries</code> to this file.
     * 
     * @param memTimeSeriesList the set of time series that need to be written to the disk.
     * @param
     * @throws IOException if a problem occurs while writing to the disk.
     * @throws InterruptedException if the tread has been interrupted
     */
    public TimeSeriesFile append(List<TimeSeriesElement> memTimeSeriesList) throws IOException, InterruptedException {

        this.logger.debug("appending " + memTimeSeriesList.size() + " memTimeSeries to file: " + getPath()
                + " at position " + this.fileSize);

        ListenableFuture<ReplayPosition> newFuture = null;

        LinkedHashMap<Range<Field>, BlockPosition> newBlockPositions = new LinkedHashMap<>(this.blockPositions);
        
        try (SeekableFileDataOutput output = this.file.getOutput()) {

            output.seek(this.fileSize);

            writeMetaDataIfNeeded(output);

            for (int i = 0, m = memTimeSeriesList.size(); i < m; i++) {

                TimeSeriesElement memTimeSeries = memTimeSeriesList.get(i);

                append((MemTimeSeries) memTimeSeries, newBlockPositions, output);

                newFuture = memTimeSeries.getFuture();
            }

            output.flush();
        }

        return new TimeSeriesFile(this.metadata,
                                  this.definition,
                                  newBlockPositions,
                                  this.file,
                                  this.file.size(),
                                  newFuture);
    }

    /**
     * Appends the content of the specified <code>MemTimeSeries</code> to the specified output.
     *
     * @param memTimeSeries the memTimeSeries
     * @param blockPositions the collecting parameter for the block positions
     * @param output the output to write to
     * @throws IOException if an I/O problem occurs
     */
    private void append(MemTimeSeries memTimeSeries,
                        LinkedHashMap<Range<Field>, BlockPosition> newBlockPositions,
                        SeekableFileDataOutput output) throws IOException {

        try (ResourceIterator<DataBlock> iterator = compress(this.definition.getCompressionType(),
                                                             memTimeSeries.iterator())) {

            long position = output.getPosition();
            while (iterator.hasNext()) {

                DataBlock block = iterator.next();

                block.writeTo(output);

                long newPosition = output.getPosition();
                int length = (int) (newPosition - position);
                BlockPosition blockPosition = new BlockPosition(position, length);
                newBlockPositions.put(getRange(block.getHeader()), blockPosition);
                position = output.getPosition();
            }
        }
    }

    /**
     * Writes the file meta data if the file is considered as empty.
     * 
     * @param output the file output
     * @throws IOException if an I/O problem occurs.
     */
    private void writeMetaDataIfNeeded(SeekableFileDataOutput output) throws IOException {
        if (this.fileSize == 0) {

            output.writeObject(this.metadata);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        this.file.close();
    }

    /**
     * Returns the file path.
     * 
     * @return the file path.
     */
    public Path getPath() {
        return this.file.getPath();
    }

    /**
     * Returns the block positions.
     * 
     * @return the block positions.
     */
    public Map<Range<Field>, BlockPosition> getBlockPositions() {
        return this.blockPositions;
    }
    
    /**
     * Creates the time series file.
     * 
     * @param metadata the file meta data.
     * @param blockPositions the position of the blocks
     * @param file the underlying file.
     * @param size the expected size of the file.
     * @param compressionType the type of compression used to compress the blocks
     * @param future the future returning the replay position of the last record written to the disk.
     * @throws IOException if an I/O problem occurs.
     */
    private TimeSeriesFile(FileMetaData metadata, 
                           TimeSeriesDefinition definition,
                           LinkedHashMap<Range<Field>, BlockPosition> blockPositions,
                           RandomAccessDataFile file, 
                           long size,
                           ListenableFuture<ReplayPosition> future) 
                                   throws IOException {

        this.metadata = metadata;
        this.definition = definition;
        this.blockPositions = blockPositions;
        this.file = file;
        this.fileSize = size;
        this.future = future;
    }

    /**
     * Returns the path to the data file.
     * 
     * @param configuration the database configuration
     * @param databaseDefinition the database definition
     * @param definition the time series definition
     * @param partitionMetadata the partition meta data
     * @return the path to the data file
     */
    private static Path getFilePath(Configuration configuration,
                                    DatabaseDefinition databaseDefinition,
                                    TimeSeriesDefinition definition,
                                    TimeSeriesPartitionMetaData partitionMetadata) {

        Path seriesDirectory = HorizonDBFiles.getTimeSeriesDirectory(configuration, databaseDefinition, definition);

        return seriesDirectory.resolve(filename(definition, partitionMetadata));
    }

    /**
     * Returns the filename of the data file associated to this partition.
     * 
     * @param partitionMetadata the partition meta data
     * @return the filename of the data file associated to this partition.
     */
    private static String filename(TimeSeriesDefinition definition, TimeSeriesPartitionMetaData partitionMetadata) {

        Range<Field> range = partitionMetadata.getRange();
        
        return new StringBuilder().append(definition.getName())
                                  .append('-')
                                  .append(range.lowerEndpoint().getTimestampInMillis())
                                  .append(".ts")
                                  .toString();
    }
    
    /**
     * Finds the blocks of data that need to be read for retrieving the data for the specified time range.
     * 
     * @param timeRange the range of time for which data must be returned
     * @return the blocks of data that need to be read for retrieving the data for the specified time range.
     */
    private List<BlockPosition> findBlocks(Range<Field> timeRange) {
        
        List<BlockPosition> blocks = new ArrayList<>();
        
        for (Entry<Range<Field>, BlockPosition> entry : this.blockPositions.entrySet()) {
            
            Range<Field> blockRange = entry.getKey();
            
            if (!timeRange.isConnected(blockRange)) {
                
                if (timeRange.upperEndpoint().compareTo(blockRange.lowerEndpoint()) < 0) {
                    break;
                }
                
                continue;
            }
            
            blocks.add(entry.getValue());
        }
        
        return blocks;
    }
        
    /**
     * Merges the specified blocks into one.
     * 
     * @param blocks the blocks to merge
     * @return a block position which is a merged of the specified blocks.
     */
    private static BlockPosition merge(List<BlockPosition> blocks) {
        
        BlockPosition firstBlock = blocks.get(0);
        BlockPosition lastBlock = blocks.get(blocks.size() - 1);
        
        long offset = firstBlock.getOffset();
        long length = (lastBlock.getOffset() + lastBlock.getLength()) - offset;
        
        return new BlockPosition(offset, length);
    }
}
