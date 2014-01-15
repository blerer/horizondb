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
package io.horizondb.db.btree;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Pointer towards some data serialized on disk.
 * 
 * @author Benjamin
 * 
 */
class DataPointer<K extends Comparable<K>, V> {

    /**
     * The manager that should be used to retrieve the data.
     */
    private final OnDiskNodeManager<K, V> manager;

    /**
     * The position of the data within the file.
     */
    private final long position;

    /**
     * The size of the sub-tree on disk.
     */
    private final int subTreeSize;

    /**
     * Creates a new pointer towards some data stored on the disk.
     * 
     * @param manager The manager that should be used to retrieve the data..
     * @param position The position of the data within the file.
     * @param subTreeSize The size in byte of the sub-tree.
     */
    public DataPointer(OnDiskNodeManager<K, V> manager, long position, int subTreeSize) {

        this.manager = manager;
        this.position = position;
        this.subTreeSize = subTreeSize;
    }

    /**
     * Returns the manager that should be used to retrieve the data.
     * 
     * @return the manager that should be used to retrieve the data.
     */
    public final OnDiskNodeManager<K, V> getManager() {
        return this.manager;
    }

    /**
     * Returns the position of the data within the file.
     * 
     * @return the position of the data within the file.
     */
    public final long getPosition() {
        return this.position;
    }

    /**
     * Returns the size of the sub-tree on disk.
     * 
     * @return the size of the sub-tree on disk.
     */
    public int getSubTreeSize() {
        return this.subTreeSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {

        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("manager", this.manager)
                                                                          .append("position", this.position)
                                                                          .append("subTreeSize", this.subTreeSize)
                                                                          .toString();
    }
}
