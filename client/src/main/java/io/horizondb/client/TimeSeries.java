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
package io.horizondb.client;

import io.horizondb.model.CompositeRecordIterator;
import io.horizondb.model.PartitionId;
import io.horizondb.model.Query;
import io.horizondb.model.RecordBatch;
import io.horizondb.model.TimeRange;
import io.horizondb.model.TimeSeriesDefinition;
import io.horizondb.model.TimeSeriesRecordIterator;
import io.horizondb.protocol.Msg;
import io.horizondb.protocol.OpCode;

import java.util.Map.Entry;

import org.apache.commons.lang.Validate;

/**
 * @author Benjamin
 *
 */
public final class TimeSeries {

	private final ConnectionManager manager;
	
	/**
	 * The definition of this time series.
	 */
	private final TimeSeriesDefinition definition;
	
	/**
	 * 
	 */
	TimeSeries(ConnectionManager manager, TimeSeriesDefinition definition) {
		
		this.manager = manager;
		this.definition = definition;
	}

	/**
	 * Returns the time series name.
	 * 
	 * @return the time series name.
	 */
    public String getName() {
	    return this.definition.getSeriesName();
    }
	
	public RecordSet.Builder newRecordSetBuilder() {
		
		return RecordSet.newBuilder(this.definition);
	}
	
	public void write(RecordSet recordSet) {

		Validate.isTrue(this.definition.equals(recordSet.getTimeSeriesDefinition()), 
		                "the recordSet is not associated to this time series but to " 
		                		+ this.definition.getDatabaseName() + "." + this.definition.getSeriesName());
		
		CompositeRecordIterator compositeRecordIterator = (CompositeRecordIterator) recordSet.getIterator(); 
		
		for (Entry<TimeRange, TimeSeriesRecordIterator> entry : compositeRecordIterator.toMap().entrySet()) {
	        
			RecordBatch batch = new RecordBatch(this.definition.getDatabaseName(),
			                                    this.definition.getSeriesName(),
			                                    entry.getKey().getStart(),
			                                    entry.getValue());

			this.manager.send(Msg.newRequestMsg(OpCode.BATCH_INSERT, batch));
        }
	}
	
	public RecordSet read(long startTimeInMillis, long endTimeInMillis) {

		TimeRange range = new TimeRange(startTimeInMillis, endTimeInMillis);

		TimeRange partitionTimeRange = this.definition.getPartitionTimeRange(startTimeInMillis);

		PartitionId id = new PartitionId(this.definition.getDatabaseName(),
		                                 this.definition.getSeriesName(),
		                                 partitionTimeRange.getStart());

		Msg<Query> query = Msg.newRequestMsg(OpCode.QUERY, new Query(id, range));

		return new RecordSet(this.definition, new StreamedRecordIterator(this.definition,
		                                                                  this.manager.getConnection(),
		                                                                  query));
	}
}
