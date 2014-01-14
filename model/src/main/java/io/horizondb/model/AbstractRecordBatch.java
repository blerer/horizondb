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
package io.horizondb.model;

import java.io.IOException;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Base class for the record batch classes.
 * 
 * @author Benjamin
 * 
 */
public abstract class AbstractRecordBatch implements Serializable {

    /**
     * The database in which the records must be inserted.
     */
    private final String databaseName;
    /**
     * The time series in which the records must be inserted.
     */
    private final String seriesName;

    /**
     * The partition ID.
     */
    private final long partition;

    /**
     * Creates a new <code>AbstractRecordBatch</code> for the specified database
     * and the specified series.
     * 
     * @param databaseName the database name
     * @param seriesName the time series name
     * @param partition the partition ID
     */
    public AbstractRecordBatch(String databaseName, String seriesName, long partition) {

        this.databaseName = databaseName;
        this.seriesName = seriesName;
        this.partition = partition;
    }

    /**
     * Returns the database name.
     * 
     * @return the database name.
     */
    public String getDatabaseName() {
        return this.databaseName;
    }

    /**
     * Returns the time series name.
     * 
     * @return the time series name.
     */
    public String getSeriesName() {
        return this.seriesName;
    }

    /**
     * Returns the partition ID.
     * 
     * @return the partition ID.
     */
    public long getPartition() {
        return this.partition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int computeSerializedSize() {
        return VarInts.computeStringSize(this.databaseName) + VarInts.computeStringSize(this.seriesName)
                + VarInts.computeUnsignedLongSize(this.partition) + computeRecordSetSerializedSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void writeTo(ByteWriter writer) throws IOException {

        VarInts.writeString(writer, this.databaseName);
        VarInts.writeString(writer, this.seriesName);
        VarInts.writeUnsignedLong(writer, this.partition);
        writeRecordSetTo(writer);
    }

    /**
     * Computes the serialized size of the record set.
     * 
     * @return the serialized size of the record set.
     */
    protected abstract int computeRecordSetSerializedSize();

    /**
     * Writes the record set to the specified writer.
     * 
     * @param writer the writer to write to
     * @throws IOException if an I/O problem occurs while writing the record
     *             set.
     */
    protected abstract void writeRecordSetTo(ByteWriter writer) throws IOException;

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("databaseName", this.databaseName)
                                                                          .append("seriesName", this.seriesName)
                                                                          .append("partition", this.partition)
                                                                          .toString();
    }

    protected static abstract class AbstractParser<T extends AbstractRecordBatch, S> implements Parser<T> {

        /**
         * {@inheritDoc}
         */
        @Override
        public final T parseFrom(ByteReader reader) throws IOException {

            String databaseName = VarInts.readString(reader);
            String seriesName = VarInts.readString(reader);
            long partition = VarInts.readUnsignedLong(reader);

            S recordSet = parseRecordSetFrom(reader);

            return newRecordBatch(databaseName, seriesName, partition, recordSet);
        }

        /**
         * Parses the record set from the specified reader.
         * 
         * @param reader the reader
         * @return the record set.
         */
        protected abstract S parseRecordSetFrom(ByteReader reader) throws IOException;

        /**
         * Creates a new record batch instance.
         * 
         * @param databaseName the database name
         * @param seriesName the series name
         * @param recordSet the record set.
         * @param partition the partition ID
         * @return a new record batch instance.
         */
        protected abstract T newRecordBatch(String databaseName, String seriesName, long partition, S recordSet);

    }
}