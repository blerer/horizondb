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

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Specify the context in which an operation occurs.
 * 
 * @author Benjamin
 * 
 */
public final class OperationContext {

    /**
     * The database manager.
     */
    private final DatabaseManager databaseManager;

    /**
     * <code>true</code> if the operation is a replay from the commit log.
     */
    private final boolean replay;

    /**
     * The future returned by the commit log.
     */
    private final ListenableFuture<ReplayPosition> future;

    /**
     * Returns the database manager.
     * 
     * @return the database manager.
     */
    public DatabaseManager getDatabaseManager() {
        return this.databaseManager;
    }

    /**
     * Returns <code>true</code> if the operation is a replay from the commit log.
     * 
     * @return <code>true</code> if the operation is a replay from the commit log.
     */
    public boolean isReplay() {
        return this.replay;
    }

    /**
     * Returns the commit log write future.
     * 
     * @return the commit log write future.
     */
    public ListenableFuture<ReplayPosition> getFuture() {
        return this.future;
    }

    /**
     * Creates a new <code>Builder</code> for the <code>OperationContext</code>s.
     * 
     * @param manager the database manager
     * @return a new <code>Builder</code> for the <code>OperationContext</code>s.
     */
    public static Builder newBuilder(DatabaseManager manager) {

        return new Builder(manager);
    }

    /**
     * Create a new operation context.
     * 
     * @param builder the builder
     */
    private OperationContext(Builder builder) {

        this.databaseManager = builder.databaseManager;
        this.replay = builder.replay;
        this.future = builder.future;
    }

    /**
     * The OperationContext builder.
     */
    public static class Builder {

        /**
         * The database manager.
         */
        private final DatabaseManager databaseManager;

        /**
         * The future returning the replay position for the mutation.
         */
        private ListenableFuture<ReplayPosition> future;

        /**
         * <code>true</code> if the operation is a replay from the commit log.
         */
        private boolean replay;

        /**
         * Creates a new <code>Builder</code> with the specified database manager.
         * 
         * @param databaseManager the database manager.
         */
        public Builder(DatabaseManager databaseManager) {

            this.databaseManager = databaseManager;
        }

        public Builder replay(boolean replay) {

            this.replay = replay;
            return this;
        }

        public Builder future(ListenableFuture<ReplayPosition> future) {

            this.future = future;
            return this;
        }

        public OperationContext build() {

            return new OperationContext(this);
        }
    }
}
