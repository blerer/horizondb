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
import io.horizondb.db.databases.DatabaseManager;
import io.horizondb.db.databases.DatabaseManagerCache;
import io.horizondb.db.databases.DefaultDatabaseManager;
import io.horizondb.db.operations.Operations;
import io.horizondb.db.series.DefaultTimeSeriesManager;
import io.horizondb.db.series.DefaultTimeSeriesPartitionManager;
import io.horizondb.db.series.TimeSeriesManager;
import io.horizondb.db.series.TimeSeriesManagerCache;
import io.horizondb.db.series.TimeSeriesPartitionManager;
import io.horizondb.db.series.TimeSeriesPartitionManagerCaches;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.Msgs;
import io.horizondb.model.protocol.OpCode;

import java.io.IOException;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Benjamin
 * 
 */
public class DefaultDatabaseEngine extends AbstractComponent implements DatabaseEngine {

    private final CommitLog commitLog;

    private final DatabaseManager databaseManager;

    /**
     * The builder used to build the operation context.
     */
    private final OperationContext.Builder contextBuilder;

    public DefaultDatabaseEngine(Configuration configuration) {

        TimeSeriesPartitionManager partitionManager = new TimeSeriesPartitionManagerCaches(configuration,
                                                                                           new DefaultTimeSeriesPartitionManager(configuration));

        TimeSeriesManager seriesManager = new TimeSeriesManagerCache(configuration,
                                                                     new DefaultTimeSeriesManager(partitionManager,
                                                                                                  configuration));

        this.databaseManager = new DatabaseManagerCache(configuration, new DefaultDatabaseManager(configuration,
                                                                                                  seriesManager));

        this.contextBuilder = OperationContext.newBuilder(this.databaseManager);

        this.commitLog = new CommitLog(configuration, this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {

        this.databaseManager.register(registry);
        this.commitLog.register(registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {

        this.commitLog.unregister(registry);
        this.databaseManager.unregister(registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        this.databaseManager.start();
        this.commitLog.start();

        this.contextBuilder.replay(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

        this.commitLog.shutdown();
        this.databaseManager.shutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object execute(ReadableBuffer buffer) {

        try {

            Msg<?> request = Msg.parseFrom(buffer);

            this.logger.debug("Message received with operation code: " + request.getHeader().getOpCode());

            if (request.isMutation()) {

                ReadableBuffer duplicate = buffer.duplicate().readerIndex(0);
                this.contextBuilder.future(this.commitLog.write(duplicate));

            } else {

                this.contextBuilder.future(null);
            }

            return performOperation(request);

        } catch (Exception e) {

            this.logger.error("", e);

            return Msgs.newErrorMsg(ErrorCodes.INTERNAL_ERROR, e.getMessage());
        }
    }

    private Object performOperation(Msg<?> msg) throws IOException {

        MsgHeader header = msg.getHeader();
        OpCode opCode = header.getOpCode();

        try {

            Operation operation = Operations.getOperationFor(opCode);

            if (operation == null) {

                String message = "The operation code message " + opCode + " is unknown.";

                this.logger.error(message);

                return Msgs.newErrorMsg(header, ErrorCodes.UNKNOWN_OPERATION_CODE, message);
            }

            OperationContext context = this.contextBuilder.build();

            return operation.perform(context, msg);

        } catch (HorizonDBException e) {

            this.logger.error("", e);

            return Msgs.newErrorMsg(header, e.getCode(), e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListenableFuture<Boolean> forceFlush(long id) {

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void replay(ReplayPosition replayPosition, ReadableBuffer bytes) throws IOException {

        Msg<?> request = Msg.parseFrom(bytes);

        this.contextBuilder.replay(true).future(Futures.immediateFuture(replayPosition));

        performOperation(request);
    }
}
