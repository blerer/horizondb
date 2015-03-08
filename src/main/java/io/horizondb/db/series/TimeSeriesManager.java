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
package io.horizondb.db.series;

import io.horizondb.db.Component;
import io.horizondb.db.HorizonDBException;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

public interface TimeSeriesManager extends Component {

    /**
     * Creates the specified time series.
     * 
     * @param databaseDefinition the database definition.
     * @param timeSeriesdefinition the time series definition.
     * @param throwExceptionIfExists <code>true</code> if an exception must be thrown if the time series already exists.
     * @throws IOException if an I/O problem occurs while creating the time series.
     * @throws HorizonDBException if a time series with the same name already exists.
     */
    void createTimeSeries(DatabaseDefinition databaseDefinition, 
                          TimeSeriesDefinition timeSeriesdefinition, 
                          boolean throwExceptionIfExists) 
                                  throws IOException,
                                         HorizonDBException;

    /**
     * Returns the time series of the specified database with the specified name if it exists.
     * 
     * @param databaseDefinition the database definition.
     * @param seriesName the time series name.
     * @return the time series with the specified name if it exists.
     * @throws IOException if an I/O problem occurs while retrieving the time series.
     * @throws HorizonDBException if the time series with the specified name does not exists.
     */
    TimeSeries getTimeSeries(DatabaseDefinition databaseDefinition, 
                             String seriesName) throws IOException, HorizonDBException;

    /**
     * Drops the specified time series.
     *
     * @param databaseDefinition the database definition.
     * @param seriesName the time series name.
     * @param throwExceptionIfDoesNotExist <code>true</code> if an exception must be thrown if the time series does not exists.
     * @throws IOException if an I/O problem occurs while creating the time series.
     * @throws HorizonDBException if a time series does not exists.
     */
    void dropTimeSeries(DatabaseDefinition databaseDefinition, 
                        String seriesName, 
                        boolean throwExceptionIfDoesNotExist) 
                                throws IOException,
                                       HorizonDBException;

    /**
     * Returns the time series with the specified ID if it exists.
     * 
     * @param id the time series ID
     * @return the time series with the specified ID if it exists.
     * @throws IOException if an I/O problem occurs while retrieving the time series.
     * @throws HorizonDBException if the time series with the specified name does not exists.
     */
    TimeSeries getTimeSeries(TimeSeriesId id) throws IOException, HorizonDBException;
    
    /**
     * Returns the <code>TimeSeriesPartitionManager</code> used by this <code>TimeSeriesManager</code>.
     * 
     * @return the <code>TimeSeriesPartitionManager</code> used by this <code>TimeSeriesManager</code>.
     */
    TimeSeriesPartitionManager getPartitionManager();
}
