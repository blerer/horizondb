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
package io.horizondb.db;

import io.horizondb.db.commitlog.CommitLog;
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.io.Buffer;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.Msgs;

import java.io.IOException;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Benjamin
 * 
 */
public class DefaultDatabaseEngine extends AbstractComponent implements DatabaseEngine {

    /**
     * The commit log.
     */
    private final CommitLog commitLog;
    
    /**
     * The storage engine being used.
     */
    private final StorageEngine storageEngine;

    /**
     * 
     * @param configuration the database configuration
     */
    public DefaultDatabaseEngine(Configuration configuration) {

        this.storageEngine = new DefaultStorageEngine(configuration);
        this.commitLog = new CommitLog(configuration, this.storageEngine);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {

        register(registry, this.storageEngine, this.commitLog);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {

        unregister(registry, this.commitLog, this.storageEngine);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        start(this.storageEngine, this.commitLog);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

        shutdown(this.commitLog, this.storageEngine);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object execute(Msg<?> request, ReadableBuffer buffer) {

        try {

            this.logger.debug("Message received with operation code: " + request.getHeader().getOpCode());

            ListenableFuture<ReplayPosition> future = null;
            
            if (request.isMutation()) {

                ReadableBuffer bytes;
                
                if (buffer == null) {
                    
                    bytes = Buffers.allocateDirect(request.computeSerializedSize());
                    request.writeTo((Buffer) bytes);
                
                } else {
                    
                    bytes = buffer.duplicate().readerIndex(0);
                }

                future = this.commitLog.write(bytes);
            }

            return this.storageEngine.execute(request, future);

        } catch (Exception e) {

            this.logger.error("", e);

            return Msgs.newErrorMsg(ErrorCodes.INTERNAL_ERROR, e.getMessage());
        }
    }
}
