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
package io.horizondb.db.commitlog;

import io.horizondb.db.Configuration;
import io.horizondb.db.DatabaseEngine;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.checksum.ChecksumByteReader;
import io.horizondb.io.checksum.ChecksumByteWriter;
import io.horizondb.io.files.RandomAccessDataFile;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.io.files.SeekableFileDataOutput;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A segment of the commit log.
 * 
 * @author Benjamin
 * 
 */
final class CommitLogSegment implements Closeable, Comparable<CommitLogSegment>, Flushable {

    /**
     * The marker written out at the end of a segment.
     */
    private static final int END_OF_SEGMENT_MARKER = 0;

    /**
     * The number of bytes of the end segment.
     */
    private static final int END_OF_SEGMENT_MARKER_SIZE = 4;

    /**
     * The prefix of the commit logs file name.
     */
    private static final String FILENAME_PREFIX = "CommitLog-";

    /**
     * The commit log files extension.
     */
    private static final String FILENAME_EXTENSION = ".log";

    /**
     * The regular expression used verify filenames.
     */
    private static final Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile(FILENAME_PREFIX + "(\\d+)"
            + FILENAME_EXTENSION);

    /**
     * The checksum size in number of bytes.
     */
    static final int CHECKSUM_SIZE = 8;

    /**
     * Overhead in bytes for each log entry (int: length + long: head checksum + long: tail checksum).
     */
    static final int LOG_OVERHEAD_SIZE = 4 + CHECKSUM_SIZE + CHECKSUM_SIZE;

    /**
     * The logger.
     */
    private final static Logger LOGGER = LoggerFactory.getLogger(CommitLogSegment.class);

    /**
     * The database configuration.
     */
    private final Configuration configuration;

    /**
     * The segment ID.
     */
    public final long id;

    /**
     * The segment file.
     */
    private final RandomAccessDataFile file;

    /**
     * The <code>FileDataOutput</code> used to write to the disk.
     */
    private final SeekableFileDataOutput output;

    /**
     * The utility to compute checksum.
     */
    private final ChecksumByteWriter crcOutput;

    /**
     * <code>true</code> if some data need to be flushed on the disk.
     */
    private boolean needsFlush = false;

    /**
     * <code>true</code> if the segment is closed.
     */
    private boolean closed;

    /**
     * Returns <code>true</code> if the filename of the specified path is the one of a commit log segment.
     * 
     * @param path the file path.
     * @return <code>true</code> if the filename of the specified path is the one of a commit log segment,
     * <code>false</code> otherwise.
     */
    public static boolean isCommitLogSegment(Path path) {

        String filename = path.getFileName().toString();
        Matcher matcher = COMMIT_LOG_FILE_PATTERN.matcher(filename);
        return matcher.matches();
    }

    /**
     * Creates a new segment file.
     * 
     * @param configuration the database configuration.
     * @return the new <code>CommitLogSegment</code>.
     * @throws IOException if a problem occurs while creating the segment.
     * @throws InterruptedException if the thread is interrupted while retrieving the output stream.
     */
    public static CommitLogSegment freshSegment(Configuration configuration) throws IOException, InterruptedException {

        long id = IdFactory.nextId();
        Path commitLogDirectory = configuration.getCommitLogDirectory();
        Path newFilePath = commitLogDirectory.resolve(fileName(id));

        LOGGER.debug("Creating new commit log segment {}", newFilePath);

        return new CommitLogSegment(configuration, id, newFilePath);
    }

    /**
     * Restores an existing commit log segment.
     * 
     * @param configuration the database configuration.
     * @param path the file path.
     * @return a <code>CommitLogSegment</code> that contains the data of the specified file.
     * @throws IOException if a problem occurs while restoring the segment.
     * @throws InterruptedException if the thread is interrupted while retrieving the output stream.
     */
    public static CommitLogSegment loadFromFile(Configuration configuration, Path path) throws IOException,
                                                                                       InterruptedException {

        return new CommitLogSegment(configuration, path);
    }

    /**
     * Creates a new segment by recycling the specified one.
     * 
     * @param segment the segment to recycle.
     * @return the recycled segment.
     * @throws IOException if a problem occurs while recycling the specified segment.
     * @throws InterruptedException if the thread is interrupted while retrieving the output stream.
     */
    public static CommitLogSegment recycleSegment(CommitLogSegment segment) throws IOException, InterruptedException {

        long id = IdFactory.nextId();
        Path commitLogDirectory = segment.configuration.getCommitLogDirectory();
        Path newFilePath = commitLogDirectory.resolve(fileName(id));

        segment.close();

        LOGGER.debug("Re-using discarded CommitLog segment for {} from {}", Long.valueOf(id), segment.getPath());

        Files.move(segment.getPath(), newFilePath);

        return new CommitLogSegment(segment.configuration, id, newFilePath);
    }

    /**
     * Checks if the specified amount of bytes can be written to this segment.
     * 
     * @return <code>true</code> if there is room to write the specified amount of bytes.
     * @param size the size of the data to add to the segment.
     * @throws IOException if an I/O problem occurs while retrieving the file size.
     */
    public boolean hasCapacityFor(long size) throws IOException {

        return (size + LOG_OVERHEAD_SIZE) <= writableBytes();
    }

    /**
     * Writes the specified bytes to this segment.
     * 
     * @param bytes the bytes to write to this segments.
     * @return the replay position corresponding to the position after the specified bytes have been written.
     * @throws IOException if an I/O problem occurs while writing to the file.
     */
    public ReplayPosition write(ReadableBuffer bytes) throws IOException {

        if (this.closed) {

            throw new IllegalStateException("This segment " + getPath() + " has already been closed.");
        }

        int length = bytes.readableBytes();

        if (length == 0) {

            return new ReplayPosition(this.id, this.output.getPosition());
        }

        this.crcOutput.reset();

        this.crcOutput.writeInt(length);
        this.crcOutput.writeChecksum();

        this.crcOutput.transfer(bytes);
        this.crcOutput.writeChecksum();

        ReplayPosition position = new ReplayPosition(this.id, this.output.getPosition());

        if (writableBytes() >= END_OF_SEGMENT_MARKER_SIZE) {

            writeEndOfSegmentMarkerAndRewind();
        }

        this.needsFlush = true;
        return position;
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
     * Returns the segment ID.
     * 
     * @return the segment ID.
     */
    public long getId() {

        return this.id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() throws IOException {

        if (this.needsFlush) {

            this.output.flush();
            this.needsFlush = false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

        if (this.closed) {

            return;
        }

        try {

            this.output.close();
            this.file.close();

        } catch (IOException e) {
            // Do nothing.
        }

        this.closed = true;
    }

    /**
     * Replays the content of this segment.
     * 
     * @param databaseEngine the database engine on which the data must be replayed.
     * @return the number of message replayed.
     * @throws IOException if an I/O problem occurs while replaying the data.
     */
    public int replay(DatabaseEngine databaseEngine) throws IOException {

        int count = 0;

        try (SeekableFileDataInput input = this.file.newInput()) {

            ChecksumByteReader crcInput = ChecksumByteReader.wrap(input);

            while (input.getPosition() < this.file.size()) {

                crcInput.resetChecksum();

                int length = crcInput.readInt();

                if (length == END_OF_SEGMENT_MARKER) {
                    break;
                }

                if (!crcInput.readChecksum()) {

                    break;
                }

                ReadableBuffer bytes = crcInput.slice(length);

                if (!crcInput.readChecksum()) {

                    break;
                }

                ReplayPosition replayPosition = new ReplayPosition(this.id, input.getPosition());
                databaseEngine.replay(replayPosition, bytes);
                count++;
            }

        } catch (IndexOutOfBoundsException e) {

            LOGGER.error("The file is shorter than expected. The last entry will not be replayed.", e);
        }

        return count;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("path", getPath()).toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(CommitLogSegment other) {
        return Long.compare(this.id, other.id);
    }

    /**
     * Creates the filename for the segment with the specified id.
     * 
     * @param id the segment id.
     * @return the filename for the segment with the specified id.
     */
    private static String fileName(long id) {

        return FILENAME_PREFIX + id + FILENAME_EXTENSION;
    }

    /**
     * Creates a segment instance by recovering the data from the specified file.
     * 
     * @param configuration the database configuration.
     * @param filePath the path to the existing segment.
     * @throws IOException if a problem occurs while reading the existing file.
     * @throws InterruptedException if the thread is interrupted while retrieving the output stream.
     */
    private CommitLogSegment(Configuration configuration, Path filePath) throws IOException, InterruptedException {

        this.configuration = configuration;

        String filename = filePath.getFileName().toString();

        Matcher matcher = COMMIT_LOG_FILE_PATTERN.matcher(filename);

        if (!matcher.matches()) {

            throw new IllegalStateException("The file: " + filePath + " is not a valid commit log segment.");
        }

        this.id = Long.parseLong(matcher.group(1));

        this.file = RandomAccessDataFile.mmap(filePath);
        this.output = this.file.getOutput();
        this.crcOutput = ChecksumByteWriter.wrap(this.output);

    }

    /**
     * Creates a new segment instance.
     * 
     * @param configuration the database configuration.
     * @param id the segment id.
     * @param filePath the path to the file where the data must be stored.
     * @throws IOException if a problem occurs while opening the existing file.
     * @throws InterruptedException if the thread is interrupted while retrieving the output stream.
     */
    private CommitLogSegment(Configuration configuration, long id, Path filePath) throws IOException,
            InterruptedException {

        this.configuration = configuration;
        this.id = id;

        long segmentSize = configuration.getCommitLogSegmentSize();

        this.file = RandomAccessDataFile.mmap(filePath, segmentSize);
        this.output = this.file.getOutput();
        this.crcOutput = ChecksumByteWriter.wrap(this.output);

        writeEndOfSegmentMarkerAndRewind();

        this.needsFlush = true;
    }

    /**
     * Returns the number of writable bytes.
     * 
     * @return the number of writable bytes.
     * @throws IOException if an I/O problem occurs while retrieving the file size.
     */
    private long writableBytes() throws IOException {
        return this.file.size() - this.output.getPosition();
    }

    /**
     * Writes the end of segment marker and rewind at the position where it starts.
     * 
     * @throws IOException if an I/O problem occurs.
     */
    private void writeEndOfSegmentMarkerAndRewind() throws IOException {

        this.output.writeInt(END_OF_SEGMENT_MARKER);
        this.output.seek(this.output.getPosition() - END_OF_SEGMENT_MARKER_SIZE);
    }
}
