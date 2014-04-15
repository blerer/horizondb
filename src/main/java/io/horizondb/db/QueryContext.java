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
package io.horizondb.db;

import io.horizondb.db.databases.DatabaseManager;
import io.horizondb.model.protocol.MsgHeader;

/**
 * The context in which a query must be executed.
 * 
 * @author Benjamin
 *
 */
public final class QueryContext {

    /**
     * The operation context.
     */
    private final OperationContext operationContext;
    
    /**
     * The request message header.
     */
    private final MsgHeader requestHeader;
    
    /**
     * The name of the database on which the query must be performed.
     */
    private final String databaseName;
            
    /**
     * Creates a new <code>QueryContext</code> that will be executed within the specified 
     * operation context.
     * 
     * @param operationContext the operation context
     * @param requestHeader the request message header
     * @param databaseName the database name
     */
    public QueryContext(OperationContext operationContext, 
                        MsgHeader requestHeader,
                        String databaseName) {
        
        this.operationContext = operationContext;
        this.requestHeader = requestHeader;
        this.databaseName = databaseName;
    }

    /**
     * Returns the database manager.
     * 
     * @return the database manager.
     */
    public DatabaseManager getDatabaseManager() {
        return this.operationContext.getDatabaseManager();
    }

    /**
     * Returns <code>true</code> if the query is a replay from the commit log.
     * 
     * @return <code>true</code> if the query is a replay from the commit log.
     */
    public boolean isReplay() {
        return this.operationContext.isReplay();
    }

    /**
     * Returns the header of the request message.
     * 
     * @return the header of the request message.
     */
    public MsgHeader getRequestHeader() {
        return this.requestHeader;
    }

    /**
     * Returns the name of the database on which the query must be performed.
     * 
     * @return the name of the database on which the query must be performed.
     */
    public String getDatabaseName() {
        return this.databaseName;
    }
}
