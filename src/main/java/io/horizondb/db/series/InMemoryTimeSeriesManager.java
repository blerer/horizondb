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
package io.horizondb.db.series;

import java.io.IOException;

import io.horizondb.db.Configuration;
import io.horizondb.db.btree.BTreeStore;
import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 * A <code>TimeSeriesManager</code> that store its data in memory.
 */
public final class InMemoryTimeSeriesManager extends AbstractTimeSeriesManager {

    public InMemoryTimeSeriesManager(TimeSeriesPartitionManager partitionManager, Configuration configuration) {
        super(partitionManager, configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BTreeStore<TimeSeriesId, TimeSeriesDefinition> createBTreeStore(Configuration configuration, 
                                                                              int branchingFactor) 
                                                                              throws IOException {
        return BTreeStore.newInMemoryStore(getName(), branchingFactor);
    }    
}
