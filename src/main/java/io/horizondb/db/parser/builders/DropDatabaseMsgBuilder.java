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
import io.horizondb.db.parser.BadHqlGrammarException;
import io.horizondb.db.parser.HqlBaseListener;
import io.horizondb.db.parser.HqlParser.DropDatabaseContext;
import io.horizondb.db.parser.MsgBuilder;
import io.horizondb.model.protocol.DropDatabasePayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.protocol.Payload;

import java.io.IOException;

import org.antlr.v4.runtime.misc.NotNull;

/**
 * <code>Builder</code> for messages requesting the deletion of a database.
 */
final class DropDatabaseMsgBuilder extends HqlBaseListener implements MsgBuilder {

    /**
     * The database manager.
     */
    private final DatabaseManager databaseManager;
    
    /**
     * The original request header.
     */
    private final MsgHeader requestHeader;
    
    /**
     * The database.
     */
    private String currentDatabase; 
    
    /**
     * The database.
     */
    private String databaseName;    

    /**
     * Creates a new <code>DropDatabaseMsgBuilder</code> instance.
     * 
     * @param databaseManager the database manager used to retrieve the database
     * @param requestHeader the original request header
     * @param databaseName the name of the time series database
     */
    public DropDatabaseMsgBuilder(DatabaseManager databaseManager,
                                  MsgHeader requestHeader, 
                                  String databaseName) {
        
        this.databaseManager = databaseManager;
        this.requestHeader = requestHeader;
        this.currentDatabase = databaseName;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterDropDatabase(@NotNull DropDatabaseContext ctx) {
        
        this.databaseName = ctx.ID().getText();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Msg<?> build() throws IOException, HorizonDBException {
        
        if (this.currentDatabase.equals(this.databaseName)) {
            
            String msg = String.format("Cannot drop %s as it is the currently used database."
                                        + " You must first switch to an other database.",
                                       this.databaseName);
            throw new BadHqlGrammarException(msg);
        }

        // Checks that the database exists.
        this.databaseManager.getDatabase(this.databaseName);

        Payload payload = new DropDatabasePayload(this.databaseName);
        return Msg.newRequestMsg(this.requestHeader, OpCode.DROP_DATABASE, payload);
    }
}
