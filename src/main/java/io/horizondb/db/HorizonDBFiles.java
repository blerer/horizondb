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

import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.nio.file.Path;

/**
 * Utility methods related the HorizonDB files.  
 */
public final class HorizonDBFiles {

    /**
     * The name of the databases file.
     */
    private static final String DATABASES_FILENAME = "databases.b3";
    
    /**
     * The name of the time series file.
     */
    private static final String TIMESERIES_FILENAME = "timeseries.b3";
    
    /**
     * Returns the databases file path.
     *
     * @param configuration the HorizonDB configuration
     * @return the databases file path
     */
    public static Path getDatabasesFile(Configuration configuration) {
        
        return getSystemDirectory(configuration).resolve(DATABASES_FILENAME);
    }

    /**
     * Returns the timeseries file path.
     *
     * @param configuration the HorizonDB configuration
     * @return the timeseries file path
     */
    public static Path getTimeSeriesFile(Configuration configuration) {
        
        return getSystemDirectory(configuration).resolve(TIMESERIES_FILENAME);
    }
     
    /**
     * Returns the system directory, creating it if it does not exists.
     *
     * @param configuration the HorizonDB configuration
     * @return the system directory
     */
    public static Path getSystemDirectory(Configuration configuration) {
        
        Path dataDirectory = configuration.getDataDirectory();
        return dataDirectory.resolve("system");
    }

    /**
     * Returns the directory where the time series for the specified database must be stored.
     *
     * @param configuration the HorizonDB configuration
     * @param definition the database definition
     * @return the directory where the time series for the specified database must be stored.
     */
    public static Path getDatabaseDirectory(Configuration configuration, DatabaseDefinition definition) {

        Path dataDirectory = configuration.getDataDirectory();
        return dataDirectory.resolve(definition.getName() + "-" + definition.getTimestamp());
    }
    
    /**
     * Returns the directory where the time series partitions for the specified time series must be stored.
     *
     * @param configuration the HorizonDB configuration
     * @param databaseDefinition the database definition
     * @param timeSeriesDefinition the time series definition
     * @return the directory where the time series partition for the specified time series must be stored.
     */
    public static Path getTimeSeriesDirectory(Configuration configuration,
                                              DatabaseDefinition databaseDefinition,
                                              TimeSeriesDefinition timeSeriesDefinition) {

        Path databaseDirectory = getDatabaseDirectory(configuration, databaseDefinition);
        return databaseDirectory.resolve(timeSeriesDefinition.getName() + "-" + timeSeriesDefinition.getTimestamp());
    }
    
    /**
     * Must not be instantiated
     */
    private HorizonDBFiles() {
    }
}
