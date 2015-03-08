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
import io.horizondb.db.databases.DatabaseManager;
import io.horizondb.db.parser.HqlBaseListener;
import io.horizondb.db.parser.HqlParser.DropTimeSeriesContext;
import io.horizondb.db.parser.MsgBuilder;
import io.horizondb.model.protocol.DropTimeSeriesPayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.protocol.Payload;

import java.io.IOException;

import org.antlr.v4.runtime.misc.NotNull;

/**
 * <code>Builder</code> for messages requesting the deletion of a time series.
 */
final class DropTimeSeriesMsgBuilder extends HqlBaseListener implements MsgBuilder {

    /**
     * The database manager.
     */
    private final DatabaseManager databaseManager;
    
    /**
     * The original request header.
     */
    private final MsgHeader requestHeader;
    
    /**
     * The time series database.
     */
    private String databaseName;    
    
    /**
     * The time series name.
     */
    private String timeSeriesName;
       
    /**
     * Creates a new <code>DropTimeSeriesMsgBuilder</code> instance.
     * 
     * @param databaseManager the database manager used to retrieve the database
     * @param requestHeader the original request header
     * @param databaseName the name of the time series database
     */
    public DropTimeSeriesMsgBuilder(DatabaseManager databaseManager,
                                    MsgHeader requestHeader, 
                                    String databaseName) {
        
        this.databaseManager = databaseManager;
        this.requestHeader = requestHeader;
        this.databaseName = databaseName;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterDropTimeSeries(@NotNull DropTimeSeriesContext ctx) {
        
        if (ctx.databaseName() != null) {
            this.databaseName = ctx.databaseName().getText();
        }
        this.timeSeriesName = ctx.timeSeriesName().getText();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Msg<?> build() throws IOException, HorizonDBException {
        
        // Checks that the database exists.
        this.databaseManager.getDatabase(this.databaseName);

        Payload payload = new DropTimeSeriesPayload(this.databaseName, this.timeSeriesName);
        return Msg.newRequestMsg(this.requestHeader, OpCode.DROP_TIMESERIES, payload);
    }
}
