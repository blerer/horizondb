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

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * ID used to identify uniquely a time series.
 * 
 * @author Benjamin
 * 
 */
@Immutable
final class TimeSeriesId implements Comparable<TimeSeriesId>, Serializable {

    /**
     * The parser instance.
     */
    private static final Parser<TimeSeriesId> PARSER = new Parser<TimeSeriesId>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public TimeSeriesId parseFrom(ByteReader reader) throws IOException {

            return new TimeSeriesId(VarInts.readString(reader), VarInts.readString(reader));
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
     * Creates a new <code>TimeSeriesId</code>.
     * 
     * @param databaseName the database name.
     * @param seriesName the time series name.
     */
    public TimeSeriesId(String databaseName, String seriesName) {
        this.databaseName = databaseName.toLowerCase();
        this.seriesName = seriesName.toLowerCase();
    }

    /**
     * Creates a new <code>TimeSeriesId</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static TimeSeriesId parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>TimeSeriesId</code> instances.
     * 
     * @return the parser that can be used to deserialize <code>TimeSeriesId</code> instances.
     */
    public static Parser<TimeSeriesId> getParser() {

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
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof TimeSeriesId)) {
            return false;
        }
        TimeSeriesId rhs = (TimeSeriesId) object;
        return new EqualsBuilder().append(this.databaseName, rhs.databaseName)
                                  .append(this.seriesName, rhs.seriesName)
                                  .isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(-881768609, -777990173).append(this.databaseName)
                                                          .append(this.seriesName)
                                                          .toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("databaseName", this.databaseName)
                                                                          .append("seriesName", this.seriesName)
                                                                          .toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(TimeSeriesId other) {
        return new CompareToBuilder().append(this.databaseName, other.databaseName)
                                     .append(this.seriesName, other.seriesName)
                                     .toComparison();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeStringSize(this.databaseName) + VarInts.computeStringSize(this.seriesName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeString(writer, this.databaseName);
        VarInts.writeString(writer, this.seriesName);
    }
}
