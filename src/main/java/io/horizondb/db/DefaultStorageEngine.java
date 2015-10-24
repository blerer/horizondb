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

import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.db.databases.DatabaseManager;
import io.horizondb.db.databases.DatabaseManagerCache;
import io.horizondb.db.databases.OnDiskDatabaseManager;
import io.horizondb.db.operations.Operations;
import io.horizondb.db.series.OnDiskTimeSeriesManager;
import io.horizondb.db.series.OnDiskTimeSeriesPartitionManager;
import io.horizondb.db.series.TimeSeriesManager;
import io.horizondb.db.series.TimeSeriesManagerCache;
import io.horizondb.db.series.TimeSeriesPartitionManager;
import io.horizondb.db.series.TimeSeriesPartitionManagerCaches;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.Msgs;
import io.horizondb.model.protocol.OpCode;

import java.io.IOException;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * The default implementation for the <code>StorageEngine</code>.
 */
public class DefaultStorageEngine extends AbstractComponent implements StorageEngine {

    /**
     * The database manager.
     */
    private final DatabaseManager databaseManager;

    /**
     * The builder used to build the operation context.
     */
    private final OperationContext.Builder contextBuilder;

    public DefaultStorageEngine(Configuration configuration) {

        TimeSeriesPartitionManager partitionManager = new TimeSeriesPartitionManagerCaches(configuration,
                                                                                           new OnDiskTimeSeriesPartitionManager(configuration));

        TimeSeriesManager seriesManager = new TimeSeriesManagerCache(configuration,
                                                                     new OnDiskTimeSeriesManager(partitionManager,
                                                                                                 configuration));

        this.databaseManager = new DatabaseManagerCache(configuration, new OnDiskDatabaseManager(configuration,
                                                                                                 seriesManager));        
        this.contextBuilder = OperationContext.newBuilder(this.databaseManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseManager getDatabaseManager() {
        return this.databaseManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {

        register(registry, this.databaseManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {

        unregister(registry, this.databaseManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        start(this.databaseManager);

        this.contextBuilder.replay(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

        shutdown(this.databaseManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object execute(Msg<?> request, ListenableFuture<ReplayPosition> future) throws IOException, HorizonDBException {

        this.contextBuilder.future(future);

        OpCode opCode = request.getOpCode();

        Operation operation = Operations.getOperationFor(opCode);

        if (operation == null) {

            String message = String.format("The operation code message %s is unknown.", opCode);

            this.logger.error(message);

            return Msgs.newErrorMsg(request.getHeader(), ErrorCodes.UNKNOWN_OPERATION_CODE, message);
        }

        OperationContext context = this.contextBuilder.build();

        return operation.perform(context, request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListenableFuture<Boolean> forceFlush(long id) throws InterruptedException {

        TimeSeriesManager timeSeriesManager = this.databaseManager.getTimeSeriesManager();
        TimeSeriesPartitionManager partitionManager = timeSeriesManager.getPartitionManager();

        return partitionManager.forceFlush(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void replay(ReplayPosition replayPosition, ReadableBuffer bytes) throws IOException {

        try {
        
            Msg<?> request = Msg.parseFrom(bytes);

            this.contextBuilder.replay(true);

            execute(request, Futures.immediateFuture(replayPosition));
        
        } catch (HorizonDBException e) {
            
            this.logger.warn("The following exception has occured during commit log replay: ", e);
        }
    }
}
