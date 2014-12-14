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

import io.horizondb.db.Configuration;
import io.horizondb.db.btree.BTreeStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * <code>TimeSeriesPartitionManager</code> that stores its data on disk.
 */
public final class OnDiskTimeSeriesPartitionManager extends AbstractTimeSeriesPartitionManager {

    /**
     * Creates a new <code>OnDiskTimeSeriesPartitionManager</code>.
     *
     * @param configuration the database configuration
     */
    public OnDiskTimeSeriesPartitionManager(Configuration configuration) {
        super(configuration);
    }

    /**
     * The name of the partition file.
     */
    private static final String PARTITIONS_FILENAME = "partitions.b3";

    
    /**
     * {@inheritDoc}
     */
    @Override
    protected BTreeStore<PartitionId, TimeSeriesPartitionMetaData> createBTreeStore(Configuration configuration,
                                                                                    int branchingFactor) 
                                                                                    throws IOException {
        Path dataDirectory = configuration.getDataDirectory();
        Path systemDirectory = dataDirectory.resolve("system");

        if (!Files.exists(systemDirectory)) {
            Files.createDirectories(systemDirectory);
        }

        Path partitionFile = systemDirectory.resolve(PARTITIONS_FILENAME);

        return BTreeStore.newDiskStore(getName(),
                                       partitionFile,
                                       branchingFactor,
                                       PartitionId.getParser(),
                                       TimeSeriesPartitionMetaData.getParser());
    }
}
