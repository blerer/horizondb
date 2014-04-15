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
package io.horizondb.db.operations;

import io.horizondb.db.HorizonDBException;
import io.horizondb.db.Operation;
import io.horizondb.db.OperationContext;
import io.horizondb.db.Query;
import io.horizondb.db.QueryContext;
import io.horizondb.db.parser.QueryParser;
import io.horizondb.model.protocol.HqlQueryPayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.Msgs;

import java.io.IOException;

/**
 * <code>Operation</code> that execute an HQL query.
 * 
 * @author Benjamin
 */
final class HqlQueryOperation implements Operation {

    /**
     * {@inheritDoc}
     */
    @Override
    public Object perform(OperationContext context, Msg<?> request) throws IOException, HorizonDBException {

        HqlQueryPayload payload = Msgs.getPayload(request);

        Query query = QueryParser.parse(payload.getQuery());
        
        return query.execute(new QueryContext(context, request.getHeader(), payload.getDatabaseName()));
    }
}
