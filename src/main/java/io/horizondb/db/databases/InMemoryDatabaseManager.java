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
package io.horizondb.db.databases;

import java.io.IOException;

import io.horizondb.db.Configuration;
import io.horizondb.db.btree.InMemoryNodeManager;
import io.horizondb.db.btree.NodeManager;
import io.horizondb.db.series.TimeSeriesManager;
import io.horizondb.model.schema.DatabaseDefinition;

/**
 * <code>DatabaseManager</code> that store all data in memory.
 */
public final class InMemoryDatabaseManager extends AbstractDatabaseManager {

    /**
     * Creates a new <code>InMemoryDatabaseManager</code> that will used the specified configuration.
     * 
     * @param configuration the database configuration
     * @param timeSeriesManager the time series manager
     */
    public InMemoryDatabaseManager(Configuration configuration, TimeSeriesManager timeSeriesManager) {
        super(configuration, timeSeriesManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NodeManager<String, DatabaseDefinition> createNodeManager(Configuration configuration, String name) throws IOException {
        return new InMemoryNodeManager<>(name);
    }
}
