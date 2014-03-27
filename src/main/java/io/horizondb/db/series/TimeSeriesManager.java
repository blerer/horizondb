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
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;

public interface TimeSeriesManager extends Component {

    /**
     * Creates the specified time series.
     * 
     * @param databaseName the database name.
     * @param definition the time series definition.
     * @param future the commit log future
     * @param throwExceptionIfExists <code>true</code> if an exception must be thrown if the time series already exists.
     * @throws IOException if an I/O problem occurs while creating the time series.
     * @throws HorizonDBException if a time series with the same name already exists.
     */
    void createTimeSeries(String databaseName, 
                          TimeSeriesDefinition definition, 
                          ListenableFuture<ReplayPosition> future, 
                          boolean throwExceptionIfExists) 
                                  throws IOException,
                                         HorizonDBException;

    /**
     * Returns the time series of the specified database with the specified name if it exists.
     * 
     * @param databaseName the database name.
     * @param seriesName the time series name.
     * @return the time series with the specified name if it exists.
     * @throws IOException if an I/O problem occurs while retrieving the time series.
     * @throws HorizonDBException if the time series with the specified name does not exists.
     */
    TimeSeries getTimeSeries(String databaseName, String seriesName) throws IOException, HorizonDBException;

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
