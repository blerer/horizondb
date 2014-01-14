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
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.records.TimeSeriesRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Benjamin
 * 
 */
public class TimeSeriesRecordIterator implements RecordIterator, Serializable {

	/**
	 * The records.
	 */
	private final List<TimeSeriesRecord> records;

	/**
	 * The record index.
	 */
	private int index;

	/**
	 * Creates a new <code>Builder</code> for building record set for the specified time series.
	 * 
	 * @param definition the time series definition
	 * @return a new <code>Builder</code> for building record set for the specified time series.
	 */
	public static Builder newBuilder(TimeSeriesDefinition definition) {

		return new Builder(definition);
	}

	private TimeSeriesRecordIterator(Builder builder) {
		
		this.records = new ArrayList<>(builder.records);
		Collections.sort(this.records);
		
		int numberOfRecordTypes = builder.getDefinition().getNumberOfRecordTypes();
		TimeSeriesRecord[] nextRecords = new TimeSeriesRecord[numberOfRecordTypes];

		for (int i = this.records.size() - 1; i >= 0; i--) {

			TimeSeriesRecord current = this.records.get(i);
			int type = current.getType();

			if (nextRecords[type] != null) {

				try {

					nextRecords[type].subtract(current);

				} catch (IOException e) {

					// must never occurs
					throw new IllegalStateException(e);
				}
			}

			nextRecords[type] = current;
		}
	}

	/**	
	 * {@inheritDoc}
	 */
	@Override
    public boolean hasNext() {
	    return this.index < this.records.size();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TimeSeriesRecord next() {

		if (!hasNext()) {
			throw new NoSuchElementException();
		}

		return this.records.get(this.index++);
	}
	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public void close() {

    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int computeSerializedSize() {

		int size = 0;

		for (int i = 0, m = this.records.size(); i < m; i++) {

			Record record = this.records.get(i);
			int serializedSize = record.computeSerializedSize();
			size += 1 + VarInts.computeUnsignedIntSize(serializedSize) + serializedSize;
		}

		return size;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeTo(ByteWriter writer) throws IOException {

		for (int i = 0, m = this.records.size(); i < m; i++) {

			Record record = this.records.get(i);
			int serializedSize = record.computeSerializedSize();
			writer.writeByte(record.getType());
			VarInts.writeUnsignedInt(writer, serializedSize);
			record.writeTo(writer);
		}
	}
	
	public static final class Builder extends AbstractRecordIteratorBuilder<TimeSeriesRecordIterator> {

		private final List<TimeSeriesRecord> records = new ArrayList<>();

		private Builder(TimeSeriesDefinition definition) {

			super(definition);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		protected void addRecord(TimeSeriesRecord record) {
			this.records.add(record);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		protected TimeSeriesRecordIterator
		        newRecordIterator(AbstractRecordIteratorBuilder<TimeSeriesRecordIterator> builder) {
			return new TimeSeriesRecordIterator(this);
		}
	}
}
