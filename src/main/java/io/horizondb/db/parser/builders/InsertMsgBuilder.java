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

import io.horizondb.db.parser.HqlBaseListener;
import io.horizondb.db.parser.HqlParser.InsertContext;
import io.horizondb.db.parser.HqlParser.RecordNameContext;
import io.horizondb.db.parser.MsgBuilder;
import io.horizondb.model.protocol.InsertPayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.protocol.Payload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.NotNull;

/**
 * <code>Builder</code> for messages requesting some data insertion.
 * 
 * @author Benjamin
 *
 */
final class InsertMsgBuilder extends HqlBaseListener implements MsgBuilder {

    /**
     * The original request header.
     */
    private final MsgHeader requestHeader;
    
    /**
     * The name of the database in which the data must be inserted.
     */
    private final String database; 
    
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
     * @param requestHeader the original request header
     * @param database the database in which the data must be inserted
     */
    public InsertMsgBuilder(MsgHeader requestHeader, String database) {
        
        this.requestHeader = requestHeader;
        this.database = database;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void enterInsert(@NotNull InsertContext ctx) {

        RecordNameContext recordName = ctx.recordName();
        
        this.series = recordName.ID(0).getText();
        this.recordType = recordName.ID(1).getText();
        
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
    public Msg<?> build() throws IOException {
        Payload payload = new InsertPayload(this.database,
                                            this.series,
                                            this.recordType,
                                            this.fieldNames,
                                            this.fieldValues);
        
        return Msg.newRequestMsg(this.requestHeader, OpCode.INSERT, payload);
    }
}
