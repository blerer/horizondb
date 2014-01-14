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

import io.horizondb.model.records.TimeSeriesRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * @author Benjamin
 *
 */
public class CompositeRecordIterator implements RecordIterator {
	
	private final Map<TimeRange, TimeSeriesRecordIterator> iteratorPerRanges = new TreeMap<>();
	
	private Iterator<TimeSeriesRecordIterator> rangeIterator;
	
	private TimeSeriesRecordIterator recordIterator;
	
    public CompositeRecordIterator(Builder builder) {
    	
    	for (Entry<TimeRange, TimeSeriesRecordIterator.Builder> entry : builder.records.entrySet()) {
	        
    		this.iteratorPerRanges.put(entry.getKey(), entry.getValue().build());
        }
    }
    
    /**    
     * {@inheritDoc}
     */
	@Override
    public boolean hasNext() {
	    
		if (this.rangeIterator == null) {
			
			this.rangeIterator = this.iteratorPerRanges.values().iterator();
		}
		
		if ((this.recordIterator == null || !this.recordIterator.hasNext()) && this.rangeIterator.hasNext()) {
			
			this.recordIterator = this.rangeIterator.next();
		}
		
	    return this.recordIterator != null && this.recordIterator.hasNext() ;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
	public TimeSeriesRecord next() {
		
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		
		return this.recordIterator.next();
	}
	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public void close() {

    }

	/**
	 * Returns a map view of this <code>CompositeRecordIterator</code>. 
	 * 
	 * @return a map view of this <code>CompositeRecordIterator</code>. 
	 */
	public Map<TimeRange, TimeSeriesRecordIterator> toMap() {
		
		return Collections.unmodifiableMap(this.iteratorPerRanges);
	}
	
	/**
	 * Creates a new <code>Builder</code> for building record iterator for the specified time series.
	 * 
	 * @param definition the time series definition
	 * @return a new <code>Builder</code> for building record iterator for the specified time series.
	 */
	public static Builder newBuilder(TimeSeriesDefinition definition) {

		return new Builder(definition);
	}
	
	public static final class Builder extends AbstractRecordIteratorBuilder<CompositeRecordIterator> {

		private final Map<TimeRange, TimeSeriesRecordIterator.Builder> records = new TreeMap<>();

		private TimeRange currentPartitionRange;
		
		private Builder(TimeSeriesDefinition definition) {

			super(definition);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		protected void addRecord(TimeSeriesRecord record) {

			try {
				
				long timestampInMillis = record.getTimestampInMillis(0);
				
				if (this.currentPartitionRange == null || !this.currentPartitionRange.includes(timestampInMillis)) {
					
					this.currentPartitionRange = getDefinition().getPartitionTimeRange(timestampInMillis);
				} 
				
				TimeSeriesRecordIterator.Builder builder = this.records.get(this.currentPartitionRange);

				if (builder == null) {
					builder = TimeSeriesRecordIterator.newBuilder(getDefinition());
					this.records.put(this.currentPartitionRange, builder);
				}

				builder.addRecord(record);
				
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}

		/**
		 * {@inheritDoc}
		 */
        @Override
        protected CompositeRecordIterator
                newRecordIterator(AbstractRecordIteratorBuilder<CompositeRecordIterator> builder) {
        	return new CompositeRecordIterator(this);
        }
	}
}
