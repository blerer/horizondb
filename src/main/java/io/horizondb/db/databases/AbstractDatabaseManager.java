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
package io.horizondb.db.databases;

import io.horizondb.db.AbstractComponent;
import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.Names;
import io.horizondb.db.btree.BTree;
import io.horizondb.db.btree.NodeManager;
import io.horizondb.db.series.TimeSeriesManager;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.schema.DatabaseDefinition;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.codahale.metrics.MetricRegistry;

import static org.apache.commons.lang.Validate.notNull;

/**
 * The base class for the database manager.
 */
abstract class AbstractDatabaseManager extends AbstractComponent implements DatabaseManager {

    /**
     * The B+Tree branching factor.
     */
    private static final int BRANCHING_FACTOR = 16;

    /**
     * The database server configuration.
     */
    protected final Configuration configuration;

    /**
     * The time series manager.
     */
    private final TimeSeriesManager timeSeriesManager;

    /**
     * The B+Tree in which are stored the databases meta data.
     */
    private BTree<String, DatabaseDefinition> btree;

    /**
     * The B+Tree node manager.
     */
    private NodeManager<String, DatabaseDefinition> nodeManager;

    /**
     * Creates a new <code>AbstractDatabaseManager</code> that will used the specified configuration.
     * 
     * @param configuration the database configuration
     * @param timeSeriesManager the time series manager
     */
    public AbstractDatabaseManager(Configuration configuration, TimeSeriesManager timeSeriesManager) {

        notNull(configuration, "the configuration parameter must not be null.");

        this.configuration = configuration;
        this.timeSeriesManager = timeSeriesManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void register(MetricRegistry registry) {
        
        register(registry, this.nodeManager, this.timeSeriesManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void unregister(MetricRegistry registry) {
        unregister(registry, this.timeSeriesManager, this.nodeManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void doStart() throws IOException, InterruptedException {

        this.nodeManager = createNodeManager(this.configuration, getName());

        this.btree = new BTree<>(this.nodeManager, BRANCHING_FACTOR);

        this.timeSeriesManager.start();
    }

    /**
     * Creates the node manager used by this <code>DatabaseManager</code>.
     * 
     * @param configuration the database configuration
     * @param name the database manager name
     * 
     * @throws IOException if an I/O problem occurs while creating the node manager
     */
    protected abstract NodeManager<String, DatabaseDefinition> createNodeManager(Configuration configuration, 
                                                                                 String name) 
                                                                                 throws IOException; 

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void doShutdown() throws InterruptedException {

        this.timeSeriesManager.shutdown();
        this.nodeManager.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void createDatabase(DatabaseDefinition definition, 
                                     boolean throwExceptionIfExists) 
                                     throws IOException, HorizonDBException {

        String name = definition.getName();
        String lowerCaseName = name.toLowerCase();

        Names.checkDatabaseName(name);

        if (!this.btree.insertIfAbsent(lowerCaseName, definition)) {

            if (throwExceptionIfExists) {
                throw new HorizonDBException(ErrorCodes.DUPLICATE_DATABASE, "Duplicate database name " + name);
            }
        
        } else {
            afterCreate(definition);
        }
    }

    /**
     * Called once the database has been created.
     * @param definition the database definition.
     * @throws IOException if an I/O problem occurs.
     */
    protected void afterCreate(DatabaseDefinition definition) throws IOException {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Database getDatabase(String name) throws IOException, HorizonDBException {

        String lowerCaseName = name.toLowerCase();
        
        if (StringUtils.isEmpty(lowerCaseName)) {
            throw new HorizonDBException(ErrorCodes.UNKNOWN_DATABASE, "No database has been specified.");
        }

        DatabaseDefinition definition = this.btree.get(lowerCaseName);

        if (definition == null) {
            throw new HorizonDBException(ErrorCodes.UNKNOWN_DATABASE, "The database '" + name + "' does not exists.");
        }

        return new Database(definition, this.timeSeriesManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void dropDatabase(String name,
                             boolean throwExceptionIfDoesNotExist) throws IOException, HorizonDBException {

        String lowerCaseName = name.toLowerCase();

        if (!this.btree.deleteIfPresent(lowerCaseName) && throwExceptionIfDoesNotExist) {

            throw new HorizonDBException(ErrorCodes.UNKNOWN_DATABASE, "The database '" + name + "' does not exists.");
        }
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public final TimeSeriesManager getTimeSeriesManager() {
        return this.timeSeriesManager;
    }
}
