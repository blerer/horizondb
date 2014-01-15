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
import io.horizondb.model.Record;
import io.horizondb.model.RecordIterator;
import io.horizondb.model.TimeSeriesDefinition;
import io.horizondb.model.records.TimeSeriesRecord;

import java.io.IOException;

import static org.apache.commons.lang.Validate.isTrue;

/**
 * @author Benjamin
 * 
 */
public final class RecordSet implements AutoCloseable {

	/**
	 * The definition of the time series targeted 
	 */
	private final TimeSeriesDefinition definition;
	
	/**
	 * The current record being exposed.
	 */
	private TimeSeriesRecord current;

	/**
	 * <code>true</code> if the end of the record set has been reached.
	 */
	private boolean endOfRecordSet;

	/**
	 * <code>true</code> if the record set has been closed.
	 */
	private boolean closed;

	/**
	 * The time series records for each types.
	 */
	private final TimeSeriesRecord[] records;

	private final RecordIterator iterator;

	/**
	 * 
	 * @param definition
	 */
	RecordSet(TimeSeriesDefinition definition, RecordIterator iterator) {

		this.definition = definition;
		this.records = definition.newRecords();
		this.iterator = iterator;
	}

	/**
	 * Returns the time series definition.
	 * @return the time series definition.
	 */
	public TimeSeriesDefinition getTimeSeriesDefinition() {
		return this.definition;
	}

	/**
	 * @param definition
	 * @return
	 */
	public static Builder newBuilder(TimeSeriesDefinition definition) {
		return new Builder(definition);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void close() {

		if (this.closed) {
			return;
		}

		this.closed = true;

		try {

			this.iterator.close();

		} catch (IOException e) {
			throw new HorizonDBException("", e);
		}
	}

	/**
	 * Moves the cursor forward one record from its current position. A
	 * <code>RecordSet</code> cursor is initially positioned before the first
	 * record.
	 * 
	 * @return <code>true</code> if the new current record is valid,
	 *         <code>false</code> if there are no more record
	 */
	public final boolean next() {

		try {

			this.endOfRecordSet = !this.iterator.hasNext();

			if (this.endOfRecordSet) {
				return false;
			}

			Record next = this.iterator.next();

			this.current = this.records[next.getType()];

			if (next.isDelta()) {

				this.current.add(next);

			} else {

				next.copyTo(this.current);
			}

			return true;

		} catch (IOException e) {

			this.endOfRecordSet = true;
			throw new HorizonDBException("", e);
		}
	}

	/**
	 * Returns this record type.
	 * 
	 * @return this record type.
	 */
	public final int getType() {

		checkState();
		return this.current.getType();
	}

	/**
	 * Returns the value of the specified field as a time stamp in seconds.
	 * 
	 * @param index the field index.
	 * @return the value of the specified field as a time stamp in seconds.
	 */
	public final long getTimestampInSeconds(int index) {

		checkState();
		try {
			return this.current.getTimestampInSeconds(index);
		} catch (IOException e) {
			throw new HorizonDBException("", e);
		}
	}

	/**
	 * Returns the value of the specified field as a time stamp in milliseconds.
	 * 
	 * @param index the field index.
	 * @return the value of the specified field as a time stamp in milliseconds.
	 */
	public final long getTimestampInMillis(int index) {

		checkState();
		try {
			return this.current.getTimestampInMillis(index);
		} catch (IOException e) {
			throw new HorizonDBException("", e);
		}
	}

	/**
	 * Returns the value of the specified field as a time stamp in microseconds.
	 * 
	 * @param index the field index.
	 * @return the value of the specified field as a time stamp in microseconds.
	 */
	public final long getTimestampInMicros(int index) {

		checkState();
		try {
			return this.current.getTimestampInMicros(index);
		} catch (IOException e) {
			throw new HorizonDBException("", e);
		}
	}

	/**
	 * Returns the value of the specified field as a time stamp in nanoseconds.
	 * 
	 * @param index the field index.
	 * @return the value of the specified field as a time stamp in nanoseconds.
	 */
	public final long getTimestampInNanos(int index) {

		checkState();
		try {
			return this.current.getTimestampInNanos(index);
		} catch (IOException e) {
			throw new HorizonDBException("", e);
		}
	}

	/**
	 * Returns the value of the specified field as a <code>long</code>.
	 * 
	 * @param index the field index.
	 * @return the value of the specified field as a <code>long</code>.
	 */
	public final long getLong(int index) {

		checkState();
		try {
			return this.current.getLong(index);
		} catch (IOException e) {
			throw new HorizonDBException("", e);
		}
	}

	/**
	 * Returns the value of the specified field as an <code>int</code>.
	 * 
	 * @param index the field index.
	 * @return the value of the specified field as an <code>int</code>.
	 */
	public final int getInt(int index) {

		checkState();
		try {
			return this.current.getInt(index);
		} catch (IOException e) {
			throw new HorizonDBException("", e);
		}
	}

	/**
	 * Returns the value of the specified field as a <code>byte</code>.
	 * 
	 * @param index the field index.
	 * @return the value of the specified field as a <code>byte</code>.
	 */
	public final int getByte(int index) {

		checkState();
		try {
			return this.current.getByte(index);
		} catch (IOException e) {
			throw new HorizonDBException("", e);
		}
	}

	/**
	 * Returns the value of the mantissa of the specified decimal field.
	 * 
	 * @param index the field index.
	 * @return the value of the mantissa of the specified decimal field.
	 */
	public final long getDecimalMantissa(int index) {

		checkState();
		try {
			return this.current.getDecimalMantissa(index);
		} catch (IOException e) {
			throw new HorizonDBException("", e);
		}
	}

	/**
	 * Returns the value of the exponent of the specified decimal field.
	 * 
	 * @param index the field index.
	 * @return the value of the exponent of the specified decimal field.
	 */
	public final byte getDecimalExponent(int index) {

		checkState();
		try {
			return this.current.getDecimalExponent(index);
		} catch (IOException e) {
			throw new HorizonDBException("", e);
		}
	}

	/**
	 * Returns the underlying iterator.
	 * 
	 * @return the underlying iterator.
	 */
	RecordIterator getIterator() {
		return this.iterator;
	}

	/**
	 * Checks that this record set is in a valid state for reading fields.
	 */
	private void checkState() {

		isTrue(!this.closed, "The RecordSet has been closed.");
		isTrue(!this.endOfRecordSet, "All the records of the RecordSet has been read.");
		isTrue(this.current != null, "The next method must be called before trying to read the record fields.");
	}

	public static class Builder {

		/**
		 * The underlying builder.
		 */
		private final CompositeRecordIterator.Builder builder;

		private final TimeSeriesDefinition definition;

		public Builder(TimeSeriesDefinition definition) {

			this.definition = definition;
			this.builder = CompositeRecordIterator.newBuilder(definition);
		}

		public final Builder newRecord(String recordType) {

			this.builder.newRecord(recordType);
			return this;
		}

		public final Builder newRecord(int recordTypeIndex) {

			this.builder.newRecord(recordTypeIndex);
			return this;
		}

		public final Builder setLong(int index, long l) {

			this.builder.setLong(index, l);
			return this;
		}

		public final Builder setInt(int index, int i) {

			this.builder.setInt(index, i);
			return this;
		}

		public final Builder setTimestampInNanos(int index, long l) {

			this.builder.setTimestampInNanos(index, l);
			return this;
		}

		public final Builder setTimestampInMicros(int index, long l) {

			this.builder.setTimestampInMicros(index, l);
			return this;
		}

		public final Builder setTimestampInMillis(int index, long l) {

			this.builder.setTimestampInMillis(index, l);
			return this;
		}

		public final Builder setTimestampInSeconds(int index, long l) {

			this.builder.setTimestampInSeconds(index, l);
			return this;
		}

		public final Builder setByte(int index, int b) {

			this.builder.setByte(index, b);
			return this;
		}

		public final Builder setDecimal(int index, long mantissa, int exponent) {

			this.builder.setDecimal(index, mantissa, exponent);
			return this;
		}

		public final RecordSet build() {

			return new RecordSet(this.definition, this.builder.build());
		}

	}
}
