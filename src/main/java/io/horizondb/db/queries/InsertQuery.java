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
package io.horizondb.db.queries;

import io.horizondb.db.HorizonDBException;
import io.horizondb.db.Query;
import io.horizondb.db.QueryContext;
import io.horizondb.db.databases.Database;
import io.horizondb.db.databases.DatabaseManager;
import io.horizondb.db.series.TimeSeries;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.Collections;

/**
 * Query requesting the insertion of a record within a time series.
 * 
 * @author Benjamin
 *
 */
public final class InsertQuery implements Query {

    /**
     * The name of the time series.
     */
    private final String timeSeriesName;
    
    /**
     * The record type name.
     */
    private final String recordType; 
    
    /**
     * The field names.
     */
    private final String[] fieldNames;
    
    /**
     * The field values.
     */
    private final String[] fieldValues;
    
    /**
     * Creates an insert query that will insert the specified record within the specified time series.
     * 
     * @param timeSeriesName the time series name
     * @param recordType the record type
     * @param fieldNames the field names
     * @param fieldValues the field values
     */
    public InsertQuery(String timeSeriesName, String recordType, String[] fieldNames, String[] fieldValues) {
        
        this.timeSeriesName = timeSeriesName;
        this.recordType = recordType;
        this.fieldNames = fieldNames;
        this.fieldValues = fieldValues;
    }    

    /**
     * {@inheritDoc}
     */
    @Override
    public Object execute(QueryContext context) throws IOException, HorizonDBException {
        
        DatabaseManager manager = context.getDatabaseManager();
        Database database = manager.getDatabase(context.getDatabaseName());
        
        TimeSeries series = database.getTimeSeries(this.timeSeriesName);
        
        TimeSeriesDefinition definition = series.getDefinition();
        
        int recordIndex = definition.getRecordTypeIndex(this.recordType);
        TimeSeriesRecord record = definition.newRecord(recordIndex);
        
        for (int i = 0; i < this.fieldNames.length; i++) {
            
            String fieldName = this.fieldNames[i];
            int fieldIndex = definition.getFieldIndex(recordIndex, fieldName);
            
            record.getField(fieldIndex).setValueFromString(definition.getTimeZone(), 
                                                           this.fieldValues[i]);
        }
        
        series.write(Collections.singletonList(record), null, context.isReplay());
        
        return Msg.emptyMsg(MsgHeader.newResponseHeader(context.getRequestHeader(), OpCode.NOOP, 0, 0));
    }

    /**
     * Returns the name of the time series where the record must be inserted.
     * @return the name of the time series where the record must be inserted
     */
    public String getTimeSeriesName() {
        return this.timeSeriesName;
    }

    /**
     * Returns the type of records that must be inserted.
     * @return the type of record that must be inserted
     */
    public String getRecordType() {
        return this.recordType;
    }

    /**
     * Returns the field names.
     * @return the field names
     */
    public String[] getFieldNames() {
        return this.fieldNames;
    }

    /**
     * Returns the field values.
     * @return the field values
     */
    public String[] getFieldValues() {
        return this.fieldValues;
    }
}
