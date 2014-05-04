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
 * <code>RecordFilter</code> that filter record based on some field value.
 * 
 * @author Benjamin
 *
 */
public class OrRecordFilter implements RecordFilter {

    /**
     * The next record filter.
     */
    private final RecordFilter next;
    
    /**
     * The filter used to filter records.
     */
    private final Filter<Field> filter;
    
    /**
     * The field index per record type.
     */
    private int[] fieldIndices;
    
    /**
     * The current value of the field per record type.
     */
    private Field[] fields;
        
    /**
     * @param next
     * @param filter
     * @param fieldIndex
     * @param field
     */
    public OrRecordFilter(TimeSeriesDefinition definition, 
                             RecordFilter next, 
                             Filter<Field> filter, 
                             String fieldName) {
        this.next = next;
        this.filter = filter;
        
        int numberOfRecordTypes = definition.getNumberOfRecordTypes();
        
        this.fieldIndices = new int[numberOfRecordTypes];
        this.fields = new Field[numberOfRecordTypes];
        
        for (int i = 0; i < numberOfRecordTypes; i++) {
            
            this.fieldIndices[i] = definition.getFieldIndex(i, fieldName);
            this.fields[i] = definition.newField(i, this.fieldIndices[i]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(Record record) throws IOException {
        
        updateField(record);
        
        return this.next.accept(record) || this.filter.accept(this.fields[record.getType()]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDone() {
        
        return this.next.isDone() && this.filter.isDone();
    }
    
    /**
     * Updates the field value.
     * 
     * @param record the new record
     * @throws IOException if an I/O problem occurs
     */
    private void updateField(Record record) throws IOException {
        
        int type = record.getType();
        Field field = this.fields[type];
        Field recordField = record.getField(this.fieldIndices[type]);
        
        if (record.isDelta()) {
            
            field.add(recordField);
        
        } else { 
        
            recordField.copyTo(field);
        }
    }
}
