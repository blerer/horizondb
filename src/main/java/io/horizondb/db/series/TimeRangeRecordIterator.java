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

import io.horizondb.model.TimeRange;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.NoSuchElementException;

import static org.apache.commons.lang.Validate.notNull;

/**
 * A <code>RecordIterator</code> that filter records that are out of the specified time range.
 * 
 * @author Benjamin
 * 
 */
class TimeRangeRecordIterator implements RecordIterator {

    /**
     * The decorated iterator.
     */
    private final RecordIterator iterator;

    /**
     * The time range for which the records must be returned.
     */
    private final TimeRange range;

    /**
     * The last timestamp for each record type.
     */
    private long[] timestamps;

    /**
     * The record to return on the call to next.
     */
    private Record next;

    /**
     * <code>true</code> if no more records need to be returned, <code>false</code> otherwise.
     */
    private boolean endOfRecords;

    /**
     * The records used to store not returned data.
     */
    private TimeSeriesRecord[] records;

    /**
     * Specify if some records have been buffered.
     */
    private boolean[] addToRecord;

    public TimeRangeRecordIterator(TimeSeriesDefinition definition, RecordIterator iterator, TimeRange range) {

        notNull(iterator, "the iterator parameter must not be null.");
        notNull(range, "the range parameter must not be null.");

        this.records = definition.newRecords();
        this.timestamps = new long[this.records.length];
        this.addToRecord = new boolean[this.records.length];
        this.iterator = iterator;
        this.range = range;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        this.iterator.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() throws IOException {

        if (this.endOfRecords) {
            return false;
        }

        if (this.next != null) {

            return true;
        }

        while (this.iterator.hasNext()) {

            Record record = this.iterator.next();

            int type = record.getType();

            if (record.isDelta()) {
                this.timestamps[type] += record.getTimestampInMillis(0);
            } else {
                this.timestamps[type] = record.getTimestampInMillis(0);
            }

            if (this.range.includes(this.timestamps[type])) {

                if (record.isDelta() && this.addToRecord[type]) {

                    this.records[type].add(record);
                    this.next = this.records[type];
                    this.addToRecord[type] = false;

                } else {

                    this.next = record;
                }

                break;
            }

            if (this.range.isBefore(this.timestamps[type])) {

                this.endOfRecords = true;
                break;
            }

            if (record.isDelta() && this.addToRecord[type]) {
                this.records[type].add(record);
            } else {
                record.copyTo(this.records[type]);
                this.addToRecord[type] = true;
            }
        }

        return this.next != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Record next() throws IOException {

        if (!hasNext()) {

            throw new NoSuchElementException("No more elements are available.");
        }

        Record record = this.next;

        this.next = null;

        return record;
    }

}
