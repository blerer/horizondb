/**
 * Copyright 2013 Benjamin Lerer
 * 
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
package io.horizondb.db.operations;

import io.horizondb.db.HorizonDBException;
import io.horizondb.db.Operation;
import io.horizondb.db.OperationContext;
import io.horizondb.model.DatabaseDefinition;
import io.horizondb.protocol.Msg;
import io.horizondb.protocol.MsgHeader;

import java.io.IOException;

/**
 * Operation used to create a new database.
 * 
 * @author Benjamin
 */
public final class CreateDatabaseOperation implements Operation {

    /**
     * {@inheritDoc}
     */
    @Override
    public Object perform(OperationContext context, Msg<?> request) throws IOException, HorizonDBException {

        @SuppressWarnings("unchecked")
        Msg<DatabaseDefinition> msg = (Msg<DatabaseDefinition>) request;

        context.getDatabaseManager().createDatabase(msg.getPayload().getName(), !context.isReplay());

        return Msg.emptyMsg(MsgHeader.newResponseHeader(msg.getHeader(), 0, 0));
    }
}
