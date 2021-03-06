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

import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.databases.DatabaseManager;
import io.horizondb.db.parser.HqlBaseListener;
import io.horizondb.db.parser.HqlParser.CreateTimeSeriesContext;
import io.horizondb.db.parser.HqlParser.FieldDefinitionContext;
import io.horizondb.db.parser.HqlParser.RecordDefinitionContext;
import io.horizondb.db.parser.HqlParser.TimeSeriesOptionContext;
import io.horizondb.db.parser.MsgBuilder;
import io.horizondb.model.protocol.CreateTimeSeriesPayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.protocol.Payload;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.antlr.v4.runtime.misc.NotNull;

/**
 * <code>Builder</code> for messages requesting the creation of a time series.
 * 
 * @author Benjamin
 */
final class CreateTimeSeriesMsgBuilder extends HqlBaseListener implements MsgBuilder {

    /**
     * The database configuration.
     */
    private final Configuration configuration;
    
    /**
     * The database manager.
     */
    private final DatabaseManager databaseManager;
    
    /**
     * The original request header.
     */
    private final MsgHeader requestHeader;
    
    /**
     * The database in which the time series must be created.
     */
    private String databaseName;    
    
    /**
     * The time series definition builder.
     */
    private TimeSeriesDefinition.Builder timeSeriesDefBuilder;
    
    /**
     * The record type builder.
     */
    private RecordTypeDefinition.Builder recordTypeDefBuilder;
            
    /**
     * Creates a new <code>CreateTimeSeriesMsgBuilder</code> instance.
     * 
     * @param configuration the database configuration
     * @param databaseManager the database manager used to retrieve the database
     * @param requestHeader the original request header
     * @param databaseName the name of the database in which the time series must be created
     */
    public CreateTimeSeriesMsgBuilder(Configuration configuration, 
                                      DatabaseManager databaseManager,
                                      MsgHeader requestHeader, 
                                      String databaseName) {
        
        this.configuration = configuration;
        this.databaseManager = databaseManager;
        this.requestHeader = requestHeader;
        this.databaseName = databaseName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterRecordDefinition(@NotNull RecordDefinitionContext ctx) {
       
        String recordTypeName = ctx.ID().getText();
        this.recordTypeDefBuilder = RecordTypeDefinition.newBuilder(recordTypeName); 
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void exitRecordDefinition(@NotNull RecordDefinitionContext ctx) {
        
        this.timeSeriesDefBuilder.addRecordType(this.recordTypeDefBuilder);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exitFieldDefinition(@NotNull FieldDefinitionContext ctx) {

        String fieldName = ctx.ID().getText();
        String fieldType = ctx.type().getText();

        this.recordTypeDefBuilder.addField(fieldName, FieldType.valueOf(fieldType.toUpperCase()));
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterCreateTimeSeries(@NotNull CreateTimeSeriesContext ctx) {
        
        if (ctx.databaseName() != null) {
            this.databaseName = ctx.databaseName().getText();
        }
        String timeSeriesName = ctx.timeSeriesName().getText();
        this.timeSeriesDefBuilder = TimeSeriesDefinition.newBuilder(timeSeriesName)
                                                        .blockSize(this.configuration.getBlockSizeInBytes())
                                                        .compressionType(this.configuration.getCompressionType());
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterTimeSeriesOption(@NotNull TimeSeriesOptionContext ctx) {
        
        String option = ctx.getChild(0).getText();
        
        if ("TIME_UNIT".equals(option)) {
            
            TimeUnit unit = TimeUnit.valueOf(ctx.timeUnit().getText().toUpperCase()); 
            this.timeSeriesDefBuilder.timeUnit(unit);
        
        } else if ("TIMEZONE".equals(option)) {
            
            String quotedID = ctx.STRING().getText();
            String ID = quotedID.substring(1, quotedID.length() - 1);
            TimeZone timeZone = TimeZone.getTimeZone(ID); 
            this.timeSeriesDefBuilder.timeZone(timeZone);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Msg<?> build() throws IOException, HorizonDBException {
        
        // Checks that the database exists.
        this.databaseManager.getDatabase(this.databaseName);

        Payload payload = new CreateTimeSeriesPayload(this.databaseName, this.timeSeriesDefBuilder.build());
        return Msg.newRequestMsg(this.requestHeader, OpCode.CREATE_TIMESERIES, payload);
    }
}
