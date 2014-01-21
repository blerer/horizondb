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

import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.series.TimeSeries;
import io.horizondb.db.series.TimeSeriesManager;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Benjamin
 * 
 */
public final class Database {

    /**
     * The Database server configuration.
     */
    private final Configuration configuration;

    /**
     * The database meta data.
     */
    private DatabaseDefinition definition;

    /**
     * The time series manager.
     */
    private final TimeSeriesManager timeSeriesManager;

    /**
     * The database directory.
     */
    private Path directory;

    /**
     * @param configuration
     * @param metaData
     * @param timeSeriesManager the time series manager used to create or retrieve the database time series.
     * @throws IOException if a problem occurs while creating the database.
     */
    public Database(Configuration configuration, DatabaseDefinition definition, TimeSeriesManager timeSeriesManager) throws IOException {

        this.configuration = configuration;
        this.definition = definition;
        this.timeSeriesManager = timeSeriesManager;

        Path dataDirectory = this.configuration.getDataDirectory();
        this.directory = dataDirectory.resolve(definition.getName());

        createDirectoriesIfNeeded();
    }

    /**
     * Returns the database name.
     * 
     * @return the database name.
     */
    public String getName() {
        return this.definition.getName();
    }

    /**
     * Returns the definition of this database.
     * 
     * @return the definition of this database.
     */
    public DatabaseDefinition getDefinition() {
        return new DatabaseDefinition(getName());
    }

    /**
     * Creates the specified time series.
     * 
     * @param definition the time series definition.
     * @param throwExceptionIfExists <code>true</code> if an exception must be thrown if the time series already exists.
     * @throws IOException if an I/O problem occurs while creating the time series.
     * @throws HorizonDBException if a time series with the same name already exists.
     */
    public void createTimeSeries(TimeSeriesDefinition definition, boolean throwExceptionIfExists) throws IOException,
                                                                                                 HorizonDBException {

        this.timeSeriesManager.createTimeSeries(definition, throwExceptionIfExists);
    }

    /**
     * Returns the time series with the specified name.
     * 
     * @param seriesName the series name
     * @return the time series with the specified name.
     * @throws IOException if an I/O problem occurs while retrieving the time series.
     * @throws HorizonDBException if the time series does not exists.
     */
    public TimeSeries getTimeSeries(String seriesName) throws IOException, HorizonDBException {

        return this.timeSeriesManager.getTimeSeries(getName(), seriesName);
    }

    /**
     * Creates the database directory if it does not exists.
     * 
     * @throws IOException if an I/O problem occurs.
     */
    private void createDirectoriesIfNeeded() throws IOException {

        if (!Files.exists(this.directory)) {
            Files.createDirectories(this.directory);
        }
    }
}
