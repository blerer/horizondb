/**
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

import java.io.IOException;

import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 * @author Benjamin
 *
 */
public class FilteringRecordIterator implements RecordIterator {

    private final RecordIterator iterator;
    
    private final Filter<Record> filter;
    
    private final TimeSeriesRecord[] records;
    
    private final boolean[] addToRecord;

    public FilteringRecordIterator(TimeSeriesDefinition definition, Filter<Record> filter, RecordIterator iterator) {
        
        this.records = definition.newRecords();
        this.addToRecord = new boolean[this.records.length];
        this.filter = filter;
        this.iterator = iterator;
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
        return !this.filter.isDone() && this.iterator.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Record next() throws IOException {
        
        Record next = this.iterator.next();
        
        if (this.filter.accept(next)) {
            return next;
        }
        
        next.copyTo(this.records[next.getType()]);
        this.addToRecord[next.getType()] = true;
        
        return null;
    }
}
