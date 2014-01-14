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

import javax.annotation.concurrent.Immutable;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;

/**
 * A query used to request data from the server.
 * 
 * @author Benjamin
 *
 */
@Immutable
public final class Query implements Serializable {
	
    /**
     * The parser instance.
     */
    private static final Parser<Query> PARSER = new Parser<Query>() {

	    /**
	     * {@inheritDoc}
	     */
	    @Override
	    public Query parseFrom(ByteReader reader) throws IOException {

	    	PartitionId partitionId = PartitionId.parseFrom(reader);	    	
	    	TimeRange range = TimeRange.parseFrom(reader);
	    	
		    return new Query(partitionId, range);
	    }
    };
	
    /**
     * The partition targeted by the query.
     */
    private final PartitionId partitionId;
    
	/**
	 * The time range for which data must be returned from the partition.
	 */
	private final TimeRange timeRange;

	/**
	 * @param timeRange
	 */
    public Query(PartitionId partitionId, TimeRange timeRange) {

    	this.partitionId = partitionId;
	    this.timeRange = timeRange;
    }

	/**
	 * Returns the database name.
	 * @return the database name.
	 */    
    public String getDatabaseName() {
	    return this.partitionId.getDatabaseName();
    }

	/**
	 * Returns the time series name.
	 * @return the time series name.
	 */
	public String getSeriesName() {
	    return this.partitionId.getSeriesName();
    }

	/**
	 * Returns the partition identifier.
	 * @return the partition identifier.
	 */
	public long getPartition() {
	    return this.partitionId.getId();
    }



	/**
     * Returns the time range for which data must be returned.     
     * @return the time range for which data must be returned.
     */
    public TimeRange getTimeRange() {
		return this.timeRange;
	}
    

	/**
	 * @return
	 */
    public PartitionId getPartitionId() {
	    return this.partitionId;
    }

	/**
     * Creates a new <code>Query</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static Query parseFrom(ByteReader reader) throws IOException {

	    return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>Query</code> instances.
     * @return the parser that can be used to deserialize <code>Query</code> instances.
     */
    public static Parser<Query> getParser() {

	    return PARSER;
    }
    
	/**
	 * {@inheritDoc}
	 */
    @Override
    public int computeSerializedSize() {
	    return this.partitionId.computeSerializedSize() + this.timeRange.computeSerializedSize();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
    	this.partitionId.writeTo(writer);
    	this.timeRange.writeTo(writer);
    }
}
