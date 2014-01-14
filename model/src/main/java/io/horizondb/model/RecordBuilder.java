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

import io.horizondb.model.FieldType;
import io.horizondb.model.records.TimeSeriesRecord;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


/**
 * @author Benjamin
 *
 */
public final class RecordBuilder {

	private final int recordType;
	
	private final TimeUnit timestampUnit;
	
	private final FieldType[] types;
	
	private TimeSeriesRecord record;
	
	/**
	 * 
	 */
	public RecordBuilder(int recordType, TimeUnit timestampUnit, FieldType... types) {
		
		this.recordType = recordType;
		this.timestampUnit = timestampUnit;
		this.types = types;
		this.record = new TimeSeriesRecord(recordType, timestampUnit, types);
	}
	
	public RecordBuilder setLong(int index, long l) {
		
		this.record.setLong(index, l);
		return this;
	}
	
	public RecordBuilder setInt(int index, int i) {
		
		this.record.setInt(index, i);
		return this;
	}
	
	public RecordBuilder setTimestampInNanos(int index, long l) {
		
		this.record.setTimestampInNanos(index, l);
		return this;
	}
	
	public RecordBuilder setTimestampInMicros(int index, long l) {
		
		this.record.setTimestampInMicros(index, l);
		return this;
	}
	
	public RecordBuilder setTimestampInMillis(int index, long l) {
		
		this.record.setTimestampInMillis(index, l);
		return this;
	}
	
	public RecordBuilder setTimestampInSeconds(int index, long l) {
		
		this.record.setTimestampInSeconds(index, l);
		return this;
	}
	
	public RecordBuilder setByte(int index, byte b) {
		
		this.record.setByte(index, b);
		return this;
	}

	public TimeSeriesRecord build() throws IOException {
		
		TimeSeriesRecord result = this.record;
		
		this.record = new TimeSeriesRecord(this.recordType, this.timestampUnit, this.types);
		result.copyTo(this.record);
		
		return result;
	}
}
