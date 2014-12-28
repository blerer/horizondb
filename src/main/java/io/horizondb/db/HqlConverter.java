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
import io.horizondb.db.parser.QueryParser;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.protocol.HqlQueryPayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.Msgs;
import io.horizondb.model.protocol.OpCode;

import java.io.IOException;

import com.codahale.metrics.MetricRegistry;

/**
 * Decorator that convert HQL query messages in low-level messages. 
 */
public class HqlConverter extends AbstractComponent implements DatabaseEngine {

    /**
     * The database configuration.
     */
    private final Configuration configuration;
    
    /**
     * The database engine.
     */
    private final DatabaseEngine databaseEngine;

    public HqlConverter(Configuration configuration, DatabaseEngine databaseEngine) {

        this.configuration = configuration;
        this.databaseEngine = databaseEngine;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {

        this.databaseEngine.register(registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {

        this.databaseEngine.unregister(registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        this.databaseEngine.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

        this.databaseEngine.shutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseManager getDatabaseManager() {
        return this.databaseEngine.getDatabaseManager();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object execute(Msg<?> request, ReadableBuffer buffer) {

        try {

            OpCode opCode = request.getOpCode();
            
            this.logger.debug("Message received with operation code: " + opCode);

            if (opCode.isHql()) {
                
                @SuppressWarnings("unchecked")
                Msg<?> msg = QueryParser.parse(this.configuration, 
                                               this.databaseEngine.getDatabaseManager(),
                                               (Msg<HqlQueryPayload>) request);

                return this.databaseEngine.execute(msg, null);
            } 

            return this.databaseEngine.execute(request, buffer);

        } catch (HorizonDBException e) {
            
            return Msgs.newErrorMsg(e.getCode(), e.getMessage());
            
        } catch (Exception e) {

            this.logger.error("", e);

            return Msgs.newErrorMsg(ErrorCodes.INTERNAL_ERROR, e.getMessage());
        }
    }
}
