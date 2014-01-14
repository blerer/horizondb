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

import io.horizondb.model.RecordIterator.Builder;
import io.horizondb.model.records.TimeSeriesRecord;

import static org.apache.commons.lang.Validate.notNull;

/**
 * Base class for the <code>RecordIterator.Builder</code>.
 * 
 * @author Benjamin
 *
 */
abstract class AbstractRecordIteratorBuilder<T extends RecordIterator> implements RecordIterator.Builder<T> {
	
	/**
	 * The definition of the time series to which is associated the record set.
	 */
	private final TimeSeriesDefinition definition;

	/**
	 * The current record being filled.
	 */
	private TimeSeriesRecord current;

	public AbstractRecordIteratorBuilder(TimeSeriesDefinition definition) {

		notNull(definition, "the definition parameter must not be null.");

		this.definition = definition;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Builder<T> newRecord(String recordType) {

		newRecord(this.definition.getRecordTypeIndex(recordType));
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Builder<T> newRecord(int recordTypeIndex) {

		addCurrentToRecords();

		this.current = this.definition.newRecord(recordTypeIndex);

		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Builder<T> setLong(int index, long l) {

		this.current.setLong(index, l);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Builder<T> setInt(int index, int i) {

		this.current.setInt(index, i);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Builder<T> setTimestampInNanos(int index, long l) {

		this.current.setTimestampInNanos(index, l);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Builder<T> setTimestampInMicros(int index, long l) {

		this.current.setTimestampInMicros(index, l);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Builder<T> setTimestampInMillis(int index, long l) {

		this.current.setTimestampInMillis(index, l);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Builder<T> setTimestampInSeconds(int index, long l) {

		this.current.setTimestampInSeconds(index, l);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Builder<T> setByte(int index, int b) {

		this.current.setByte(index, b);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Builder<T> setDecimal(int index, long mantissa, int exponent) {

		this.current.setDecimal(index, mantissa, exponent);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final T build() {

		addCurrentToRecords();

		return newRecordIterator(this);
	}

	/**
	 * Return the time series definition.
	 * 
	 * @return the time series definition.
	 */
	protected final TimeSeriesDefinition getDefinition() {
		return this.definition;
	}

	/**
	 * Creates a new RecordIterator instance from the specified builder.
	 * 
	 * @param builder the builder used to create the record set.
	 * @return a new RecordSet instance
	 */
	protected abstract T newRecordIterator(AbstractRecordIteratorBuilder<T> builder);

	/**
	 * Adds the specified record to the record set.
	 * @param record the record to add.
	 */
	protected abstract void addRecord(TimeSeriesRecord record);
		
	/**
	 * Adds the current record to the record set.
	 */
	private void addCurrentToRecords() {

		if (this.current != null) {
			addRecord(this.current);
		}
	}
}
