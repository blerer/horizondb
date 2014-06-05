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

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.core.Field;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.collect.Range;

import static io.horizondb.model.core.util.SerializationUtils.computeRangeSerializedSize;
import static io.horizondb.model.core.util.SerializationUtils.parseRangeFrom;
import static io.horizondb.model.core.util.SerializationUtils.writeRange;

/**
 * ID used to identify uniquely a time series partition.
 * 
 * @author Benjamin
 * 
 */
@Immutable
final class PartitionId implements Comparable<PartitionId>, Serializable {

    /**
     * The parser instance.
     */
    private static final Parser<PartitionId> PARSER = new Parser<PartitionId>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public PartitionId parseFrom(ByteReader reader) throws IOException {

            String database = VarInts.readString(reader);
            String timeSeries = VarInts.readString(reader);
            Range<Field> range = parseRangeFrom(reader);
            
            return new PartitionId(database, timeSeries, range);
        }
    };

    /**
     * The database name.
     */
    private final String databaseName;

    /**
     * The time series name.
     */
    private final String seriesName;

    /**
     * The partition time range.
     */
    private final Range<Field> range;

    /**
     * Creates a new <code>TimeSeriesId</code>.
     * 
     * @param databaseName the database name
     * @param seriesName the time series name
     * @param range the partition time range
     */
    public PartitionId(String databaseName, String seriesName, Range<Field> range) {

        this.databaseName = databaseName.toLowerCase();
        this.seriesName = seriesName.toLowerCase();
        this.range = range;
    }

    /**
     * Creates a new <code>TimeSeriesId</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static PartitionId parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>TimeSeriesId</code> instances.
     * 
     * @return the parser that can be used to deserialize <code>TimeSeriesId</code> instances.
     */
    public static Parser<PartitionId> getParser() {

        return PARSER;
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
     * Returns the partition range.
     * 
     * @return the partition range.
     */
    public Range<Field> getRange() {
        return this.range;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof PartitionId)) {
            return false;
        }
        PartitionId rhs = (PartitionId) object;
        return new EqualsBuilder().append(this.databaseName, rhs.databaseName)
                                  .append(this.seriesName, rhs.seriesName)
                                  .append(this.range, rhs.range)
                                  .isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(-881768609, -777990173).append(this.databaseName)
                                                          .append(this.seriesName)
                                                          .append(this.range)
                                                          .toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new StringBuilder().append(this.databaseName)
                                  .append('.')
                                  .append(this.seriesName)
                                  .append('[')
                                  .append(this.range.lowerEndpoint().getTimestampInMillis())
                                  .append("..")
                                  .append(this.range.upperEndpoint().getTimestampInMillis())
                                  .append(')')
                                  .toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(PartitionId other) {
        return new CompareToBuilder().append(this.databaseName, other.databaseName)
                                     .append(this.seriesName, other.seriesName)
                                     .append(this.range.lowerEndpoint(), other.range.lowerEndpoint())
                                     .append(this.range.upperEndpoint(), other.range.upperEndpoint())
                                     .toComparison();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeStringSize(this.databaseName) 
                + VarInts.computeStringSize(this.seriesName)
                + computeRangeSerializedSize(this.range);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeString(writer, this.databaseName);
        VarInts.writeString(writer, this.seriesName);
        writeRange(writer, this.range);
    }
}
