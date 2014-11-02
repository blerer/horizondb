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

import java.io.IOException;

import io.horizondb.db.parser.HqlBaseListener;
import io.horizondb.db.parser.HqlParser.UseDatabaseContext;
import io.horizondb.db.parser.MsgBuilder;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.protocol.Payload;
import io.horizondb.model.protocol.UseDatabasePayload;

import org.antlr.v4.runtime.misc.NotNull;

/**
 * <code>Builder</code> for <code>UseDatabaseMsg</code> instances.
 * 
 * @author Benjamin
 *
 */
final class UseDatabaseMsgBuilder extends HqlBaseListener implements MsgBuilder {

    /**
     * The original request header.
     */
    private final MsgHeader requestHeader;
    
    /**
     * The name of the database that must be used.
     */
    private String databaseName;

    /**
     * Creates a new <code>CreateDatabaseRequestBuilder</code> instance.
     * 
     * @param requestHeader the original request header
     */
    public UseDatabaseMsgBuilder(MsgHeader requestHeader) {
        
        this.requestHeader = requestHeader;
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterUseDatabase(@NotNull UseDatabaseContext ctx) {
        this.databaseName = ctx.ID().getText();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Msg<?> build() throws IOException {

        Payload payload = new UseDatabasePayload(this.databaseName);
        return Msg.newRequestMsg(this.requestHeader, OpCode.USE_DATABASE, payload);
    }

}
