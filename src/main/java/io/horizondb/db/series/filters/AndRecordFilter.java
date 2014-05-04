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
package io.horizondb.db.series.filters;

import java.io.IOException;

import io.horizondb.db.series.Filter;
import io.horizondb.db.series.RecordFilter;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 * <code>RecordFilter</code> that filter record based on some field value performing an AND operation with the 
 * next filter.
 * 
 * @author Benjamin
 *
 */
public final class AndRecordFilter extends BaseFieldRecordFilter {
        
    /**
     * Creates a new <code>RecordFilter</code> that perform an AND operation with the next filter.
     * 
     * @param definition the time series definition
     * @param next the next filter in the pipeline
     * @param filter the filter used to filter records based on field value
     * @param fieldName the name of the field on which is filtering the filter
     */
    public AndRecordFilter(TimeSeriesDefinition definition, 
                           RecordFilter next, 
                           Filter<Field> filter, 
                           String fieldName) {
        
        super(definition, next, filter, fieldName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(Record record) throws IOException {
        
        updateField(record);
        
        boolean result = this.next.accept(record);
        
        if (!result) {
            
            return false;
        }
        
        return this.filter.accept(getField(record.getType()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDone() {
        
        return this.next.isDone() || this.filter.isDone();
    }
}
