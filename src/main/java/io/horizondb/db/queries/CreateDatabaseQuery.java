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
import io.horizondb.db.databases.DatabaseManager;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.schema.DatabaseDefinition;

import java.io.IOException;

/**
 * Query requesting the creation of a database.
 * 
 * @author Benjamin
 *
 */
public final class CreateDatabaseQuery implements Query {

    /**
     * The definition of the database to create.
     */
    private final DatabaseDefinition definition;
        
    /**
     * Creates the database with the specified definition.
     * 
     * @param definition the definition of the database to create.
     */
    public CreateDatabaseQuery(DatabaseDefinition definition) {
        this.definition = definition;
    }

    /**
     * Returns the definition of the database that must be created.
     * 
     * @return the definition of the database that must be created.
     */
    public DatabaseDefinition getDefinition() {
        return this.definition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object execute(QueryContext context) throws IOException, HorizonDBException {
        
        DatabaseManager manager = context.getDatabaseManager();
        manager.createDatabase(this.definition, context.isReplay());
        
        return Msg.emptyMsg(MsgHeader.newResponseHeader(context.getRequestHeader(), OpCode.NOOP, 0, 0));
    }
}
