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

import io.horizondb.io.Buffer;
import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.checksum.ChecksumByteReader;
import io.horizondb.io.checksum.ChecksumByteWriter;
import io.horizondb.io.checksum.ChecksumMismatchException;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.files.FileUtils;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.core.Field;

import java.io.IOException;
import java.util.Arrays;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.google.common.collect.Range;

import static io.horizondb.model.core.util.SerializationUtils.parseRangeFrom;
import static io.horizondb.model.core.util.SerializationUtils.writeRange;

/**
 * The meta data of a time series file.
 * 
 * @author Benjamin
 * 
 */
@Immutable
public final class FileMetaData implements Serializable {

    /**
     * The parser instance.
     */
    private static final Parser<FileMetaData> PARSER = new Parser<FileMetaData>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public FileMetaData parseFrom(ByteReader reader) throws IOException {

            ChecksumByteReader checksumByteReader = ChecksumByteReader.wrap(reader);
            ReadableBuffer buffer = checksumByteReader.slice(METADATA_LENGTH - CHECKSUM_LENGTH);
            
            if (!checksumByteReader.readChecksum()) {
                throw new ChecksumMismatchException("The meta data CRC does not match the expected one.");
            }

            byte[] magicNumber = new byte[MAGIC_NUMBER.length];

            buffer.readBytes(magicNumber);

            if (!Arrays.equals(MAGIC_NUMBER, magicNumber)) {

                throw new IOException("The file is not a time series file.");
            }

            byte version = buffer.readByte();
            String database = VarInts.readString(buffer);
            String timeSeries = VarInts.readString(buffer);
            Range<Field> range = parseRangeFrom(buffer);

            return new FileMetaData(version, database, timeSeries, range);
        }
    };

    /**
     * The length in byte of the meta data.
     */
    public static final int METADATA_LENGTH = FileUtils.ONE_KB;

    /**
     * The magic number starting the meta data header.
     */
    public static final byte[] MAGIC_NUMBER = "delta".getBytes();

    /**
     * The CRC length in bytes.
     */
    private static final int CHECKSUM_LENGTH = 8;

    /**
     * The default file format version.
     */
    public static final byte DEFAULT_VERSION = 1;

    /**
     * The version of the file format used for this file.
     */
    private final byte version;

    /**
     * The name of the database to which belongs this file.
     */
    private final String database;

    /**
     * The name of the time series to which belongs this file.
     */
    private final String timeSeries;

    /**
     * The partition range.
     */
    private final Range<Field> range;

    /**
     * Creates the file meta data.
     * 
     * @param database the name of the database to which belongs this file
     * @param timeSeries the name of the time series to which belongs this file
     * @param range the partition range
     */
    public FileMetaData(String database, String timeSeries, Range<Field> range) {

        this(DEFAULT_VERSION, database, timeSeries, range);
    }

    /**
     * Creates the file meta data.
     * 
     * @param version the file format version
     * @param database the name of the database to which belongs this file
     * @param timeSeries the name of the time series to which belongs this file
     * @param range the partition range
     */
    private FileMetaData(byte version, String database, String timeSeries, Range<Field> range) {

        this.version = version;
        this.database = database;
        this.timeSeries = timeSeries;
        this.range = range;
    }

    /**
     * Returns the version of the file format used for this file.
     * 
     * @return the version of the file format used for this file.
     */
    public byte getVersion() {
        return this.version;
    }

    /**
     * Returns the name of the database to which belongs this file.
     * 
     * @return the name of the database to which belongs this file.
     */
    public String getDatabase() {
        return this.database;
    }

    /**
     * Returns the name of the time series to which belongs this file.
     * 
     * @return the name of the time series to which belongs this file.
     */
    public String getTimeSeries() {
        return this.timeSeries;
    }

    /**
     * Returns the partition range.
     * 
     * @return the partition range.
     */
    public Range<Field> getRange() {
        return this.range;
    }

    /**
     * Creates a new <code>FileMetaData</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static FileMetaData parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>FileMetaData</code> instances.
     * 
     * @return the parser that can be used to deserialize <code>FileMetaData</code> instances.
     */
    public static Parser<FileMetaData> getParser() {

        return PARSER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return METADATA_LENGTH;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        Buffer buffer = Buffers.allocate(METADATA_LENGTH);

        ChecksumByteWriter checksumByteWriter = ChecksumByteWriter.wrap(buffer);

        checksumByteWriter.writeBytes(MAGIC_NUMBER).writeByte(this.version);

        VarInts.writeString(checksumByteWriter, this.database);
        VarInts.writeString(checksumByteWriter, this.timeSeries);
        writeRange(checksumByteWriter, this.range);

        checksumByteWriter.writeZeroBytes(buffer.writeableBytes() - CHECKSUM_LENGTH);
        checksumByteWriter.writeChecksum();

        writer.transfer(buffer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {

        if (object == this) {
            return true;
        }
        if (!(object instanceof FileMetaData)) {
            return false;
        }
        FileMetaData rhs = (FileMetaData) object;
        return new EqualsBuilder().append(this.range, rhs.range)
                                  .append(this.timeSeries, rhs.timeSeries)
                                  .append(this.database, rhs.database)
                                  .append(this.version, rhs.version)
                                  .isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(-500494123, 1424401799).append(this.range)
                                                          .append(this.timeSeries)
                                                          .append(this.database)
                                                          .append(this.version)
                                                          .toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("range", this.range)
                                                                          .append("timeSeries", this.timeSeries)
                                                                          .append("database", this.database)
                                                                          .append("version", this.version)
                                                                          .toString();
    }

}
