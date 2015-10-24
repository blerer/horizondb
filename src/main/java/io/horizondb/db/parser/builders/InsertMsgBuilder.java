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
package io.horizondb.db.parser.builders;

import io.horizondb.db.HorizonDBException;
import io.horizondb.db.databases.Database;
import io.horizondb.db.databases.DatabaseManager;
import io.horizondb.db.parser.BadHqlGrammarException;
import io.horizondb.db.parser.HqlBaseListener;
import io.horizondb.db.parser.HqlParser.InsertContext;
import io.horizondb.db.parser.HqlParser.RecordNameContext;
import io.horizondb.db.parser.MsgBuilder;
import io.horizondb.db.series.TimeSeries;
import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.model.core.RecordUtils;
import io.horizondb.model.core.records.BlockHeaderUtils;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.protocol.InsertPayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.protocol.Payload;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.NotNull;

import static java.lang.String.format;

/**
 * <code>Builder</code> for messages requesting some data insertion.
 */
final class InsertMsgBuilder extends HqlBaseListener implements MsgBuilder {

    /**
     * The database manager.
     */
    private final DatabaseManager databaseManager;
    
    /**
     * The original request header.
     */
    private final MsgHeader requestHeader;
    
    /**
     * The name of the database in which the data must be inserted.
     */
    private String databaseName; 
    
    /**
     * The time series in which the data must be inserted.
     */
    private String series;
    
    /**
     * The type of the record that must be inserted.
     */
    private String recordType;
    
    /**
     * The name of the fields in which some data must be inserted.
     */
    private List<String> fieldNames;
    
    /**
     * The field values.
     */
    private List<String> fieldValues;
    
    /**
     * Creates a new <code>CreateTimeSeriesRequestBuilder</code> instance.
     * 
     * @param databaseManager the database manager
     * @param requestHeader the original request header
     * @param databaseName the name of the database in which the data must be inserted
     */
    public InsertMsgBuilder(DatabaseManager databaseManager, MsgHeader requestHeader, String databaseName) {
        
        this.databaseManager = databaseManager;
        this.requestHeader = requestHeader;
        this.databaseName = databaseName;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void enterInsert(@NotNull InsertContext ctx) {

        if (ctx.databaseName() != null) {
            this.databaseName = ctx.databaseName().getText();
        }
        
        RecordNameContext recordName = ctx.recordName();
        
        this.series = recordName.timeSeriesName().getText();
        this.recordType = recordName.ID().getText();
        
        this.fieldNames = toList(ctx.fieldList());
        this.fieldValues = toList(ctx.valueList());
    }

    /**
     * Extracts a list of values from the specified context. 
     * 
     * @param ctx the context from which the list must be extracted
     * @return a list of values
     */
    private static List<String> toList(ParserRuleContext ctx) {
        
        if (ctx == null) {
            return Collections.emptyList();
        }
        
        List<String> list = new ArrayList<>();
        
        for (int i  = 0, m = ctx.getChildCount(); i  < m; i += 2) {
            list.add(ctx.getChild(i).getText());
        }
        
        return list;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Msg<?> build() throws IOException, HorizonDBException {

        Database database = this.databaseManager.getDatabase(this.databaseName);
        TimeSeries timeSeries = database.getTimeSeries(this.series);
        TimeSeriesDefinition definition = timeSeries.getDefinition();

        int recordTypeIndex = definition.getRecordTypeIndex(this.recordType);
        TimeSeriesRecord record = newRecord(definition, recordTypeIndex);
        int size = RecordUtils.computeSerializedSize(record);

        TimeSeriesRecord header = definition.newBlockHeader();
        BlockHeaderUtils.setFirstTimestamp(header, record);
        BlockHeaderUtils.setLastTimestamp(header, record);
        BlockHeaderUtils.setCompressedBlockSize(header, size);
        BlockHeaderUtils.setRecordCount(header, recordTypeIndex, 1);

        Buffer buffer = Buffers.allocate(RecordUtils.computeSerializedSize(header) + size);
        RecordUtils.writeRecord(buffer, header);
        RecordUtils.writeRecord(buffer, record);

        Payload payload = new InsertPayload(this.databaseName,
                                            this.series,
                                            recordTypeIndex,
                                            buffer);
        
        return Msg.newRequestMsg(this.requestHeader, OpCode.INSERT, payload);
    }
    
    /**
     * Creates a new record from the specified payload.
     * 
     * @param definition the time series definition
     * @param payload the payload
     * @return the new record
     * @throws BadHqlGrammarException if some values are invalid
     */
    private TimeSeriesRecord newRecord(TimeSeriesDefinition definition, int recordIndex) throws BadHqlGrammarException {
        
        String fieldValue = null;
        
        try {
            TimeSeriesRecord record = definition.newRecord(recordIndex);

            if (this.fieldNames.isEmpty()) {
                for (int i = 0, m = this.fieldValues.size();  i < m ; i++) {
                    fieldValue = this.fieldValues.get(i);
                    record.getField(i).setValueFromString(definition.getTimeZone(), this.fieldValues.get(i));
                }
            } else {
                for (int i = 0, m = this.fieldNames.size();  i < m ; i++) {
                    String fieldName = this.fieldNames.get(i);
                    fieldValue = this.fieldValues.get(i);
                    int fieldIndex = definition.getFieldIndex(recordIndex, fieldName);
                     
                    record.getField(fieldIndex).setValueFromString(definition.getTimeZone(), this.fieldValues.get(i));
                }
            }
            return record;
        } catch (NumberFormatException e) {
            throw new BadHqlGrammarException(format("The value %s cannot be converted into a number", fieldValue));
        } catch (IllegalArgumentException e) {
            throw new BadHqlGrammarException(e.getMessage());
        }
    }
}
