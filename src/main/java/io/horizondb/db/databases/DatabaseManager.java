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

import io.horizondb.db.Component;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.series.TimeSeriesManager;
import io.horizondb.model.schema.DatabaseDefinition;

import java.io.IOException;

/**
 * Databases manager.
 */
public interface DatabaseManager extends Component {

    /**
     * Creates the database with the specified definition.
     * 
     * @param definition the database definition.
     * @param throwExceptionIfExists <code>true</code> if an exception must be thrown if the database already exists.
     * @throws IOException if an I/O problem occurs while creating the database.
     * @throws HorizonDBException if a database with the same name already exists and throwExceptionIfExists is
     * <code>true</code>.
     */
    void createDatabase(DatabaseDefinition definition, 
                        boolean throwExceptionIfExists) throws IOException, HorizonDBException;

    /**
     * Returns the database with the specified name if it exists.
     * 
     * @param name the database name.
     * @return the database with the specified name if it exists.
     * @throws IOException if an I/O problem occurs while retrieving the database.
     * @throws HorizonDBException if the database with the specified name does not exists.
     */
    Database getDatabase(String name) throws IOException, HorizonDBException;
    
    /**
     * Returns the <code>TimeSeriesManager</code> used by this <code>DatabaseManager</code>.
     * 
     * @return the <code>TimeSeriesManager</code> used by this <code>DatabaseManager</code>.
     */
    TimeSeriesManager getTimeSeriesManager();

    /**
     * Drops the specified database.
     * @param name the name of the database to drop.
     * @param throwExceptionIfDoesNotExist <code>true</code> if an exception must be thrown if the time series does not exists.
     * @throws IOException if an I/O problem occurs while dropping the database.
     * @throws HorizonDBException if the database does not exists.
     */
    void dropDatabase(String name,
                      boolean throwExceptionIfDoesNotExist) throws IOException, HorizonDBException;
}
