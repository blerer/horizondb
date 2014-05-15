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

import io.horizondb.db.series.Filter;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

/**
 * Base class for <code>RecordFilter</code>s that filter record based on some field value.
 * 
 * @author Benjamin
 *
 */
public final class FieldRecordFilter implements Filter<Record> {
    
    /**
     * The field index per record type.
     */
    private final int[] fieldIndices;
    
    /**
     * The current value of the field per record type.
     */
    private final Field[] fields;
    
    /**
     * The filter used to filter records.
     */
    protected final Filter<Field> filter;
        
    /**
     * Creates a new <code>BaseFieldRecordFilter</code>.
     * 
     * @param definition the time series definition
     * @param fieldName the name of the field on which is filtering the filter
     * @param filter the filter used to filter records based on field value
     * @param next the next filter in the pipeline
     */
    public FieldRecordFilter(TimeSeriesDefinition definition, 
                             String fieldName, 
                             Filter<Field> filter) {

        this.filter = filter;
        
        int numberOfRecordTypes = definition.getNumberOfRecordTypes();
        
        this.fieldIndices = new int[numberOfRecordTypes];
        this.fields = new Field[numberOfRecordTypes];
        
        for (int i = 0; i < numberOfRecordTypes; i++) {
            
            this.fieldIndices[i] = definition.getFieldIndex(i, fieldName);
            
            if (this.fieldIndices[i] >= 0) {
                
                this.fields[i] = definition.newField(i, this.fieldIndices[i]);
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(Record record) throws IOException {
        
        if (!updateField(record)) {
            
            return false;
        }
        
        return this.filter.accept(this.fields[record.getType()]);
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public boolean isDone() {
        return this.filter.isDone();
    }

    /**
     * Updates the field value.
     * 
     * @param record the new record
     * @return <code>true</code> if the field has been updated, <code>false</code> otherwise.
     * @throws IOException if an I/O problem occurs
     */
    private final boolean updateField(Record record) throws IOException {
        
        int type = record.getType();
        Field field = this.fields[type];
        
        if (field == null) {
            
            return false;
        }
        
        Field recordField = record.getField(this.fieldIndices[type]);
        
        if (record.isDelta()) {
            
            field.add(recordField);
        
        } else { 
        
            recordField.copyTo(field);
        }

        return true;
    }
}
