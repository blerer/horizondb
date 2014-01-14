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

import io.horizondb.io.ByteWriter;

import java.io.IOException;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Batch of records to be inserted within a time series.
 * 
 * @author Benjamin
 *
 */
public class RecordBatch extends AbstractRecordBatch {
	
	/**
	 * The records that must be inserted.
	 */
	private final TimeSeriesRecordIterator timeSeriesRecordIterator;
	
	
	public RecordBatch(String databaseName, String seriesName, long partition, TimeSeriesRecordIterator timeSeriesRecordIterator) {
		
		super(databaseName, seriesName, partition);
		this.timeSeriesRecordIterator = timeSeriesRecordIterator;
	}

	/**
	 * {@inheritDoc}
	 */
    @Override
    protected int computeRecordSetSerializedSize() {
	    return this.timeSeriesRecordIterator.computeSerializedSize();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    protected void writeRecordSetTo(ByteWriter writer) throws IOException {
    	this.timeSeriesRecordIterator.writeTo(writer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
	public String toString() {
	    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).appendSuper(super.toString())
	    		                                                          .append("timeSeriesRecordIterator", this.timeSeriesRecordIterator)
	    		                                                          .toString();
	}

}
