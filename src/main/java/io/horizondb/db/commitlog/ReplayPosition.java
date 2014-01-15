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

import java.io.IOException;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Represents a position within the commitlog.
 * 
 * @author benjamin
 */
@Immutable
public final class ReplayPosition implements Comparable<ReplayPosition>, Serializable {

    /**
     * The parser instance.
     */
    private static final Parser<ReplayPosition> PARSER = new Parser<ReplayPosition>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public ReplayPosition parseFrom(ByteReader reader) throws IOException {

            long segment = VarInts.readUnsignedLong(reader);
            long position = VarInts.readUnsignedLong(reader);

            return new ReplayPosition(segment, position);
        }
    };

    /**
     * The ID of commitlog segment.
     */
    private final long segment;

    /**
     * The position within the segment.
     */
    private final long position;

    /**
     * Creates a new <code>ReplayPosition</code> for the specified segment and the specified position.
     * 
     * @param segment the ID of the commitlog segment
     * @param position the position within the commit log segment
     */
    public ReplayPosition(long segment, long position) {

        this.segment = segment;
        this.position = position;
    }

    /**
     * Returns the ID of the commitlog segment.
     * 
     * @return the ID of the commitlog segment
     */
    public long getSegment() {
        return this.segment;
    }

    /**
     * Returns the position within the segment.
     * 
     * @return the position within the segment.
     */
    public long getPosition() {
        return this.position;
    }

    /**
     * Creates a new <code>ReplayPosition</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static ReplayPosition parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>ReplayPosition</code> instances.
     * 
     * @return the parser that can be used to deserialize <code>ReplayPosition</code> instances.
     */
    public static Parser<ReplayPosition> getParser() {

        return PARSER;
    }

    /**
     * 
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeUnsignedLongSize(this.segment) + VarInts.computeUnsignedLongSize(this.position);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeUnsignedLong(writer, this.segment);
        VarInts.writeUnsignedLong(writer, this.position);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof ReplayPosition)) {
            return false;
        }
        ReplayPosition rhs = (ReplayPosition) object;
        return new EqualsBuilder().append(this.segment, rhs.segment).append(this.position, rhs.position).isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {

        return new HashCodeBuilder(-965631597, -371498275).append(this.segment).append(this.position).toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(ReplayPosition other) {

        return new CompareToBuilder().append(this.segment, other.segment)
                                     .append(this.position, other.position)
                                     .toComparison();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("segment", this.segment)
                                                                          .append("position", this.position)
                                                                          .toString();
    }
}
