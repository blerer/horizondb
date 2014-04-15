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
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.protocol.SetDatabasePayload;

import java.io.IOException;

/**
 * Query requesting the use of a specified database.
 * 
 * @author Benjamin
 *
 */
public final class UseDatabaseQuery implements Query {

    /**
     * The database name.
     */
    private final String name;
        
    /**
     * Use the database with the specified name.
     * 
     * @param name the name of the database to use.
     */
    public UseDatabaseQuery(String name) {
        this.name = name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object execute(QueryContext context) throws IOException, HorizonDBException {
        
        DatabaseManager manager = context.getDatabaseManager();
        Database database = manager.getDatabase(this.name);
        
        return Msg.newResponseMsg(MsgHeader.newResponseHeader(context.getRequestHeader(), OpCode.SET_DATABASE, 0, 0),
                                  new SetDatabasePayload(database.getDefinition()));
    }
}
