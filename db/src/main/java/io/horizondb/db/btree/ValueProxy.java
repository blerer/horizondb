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

import java.io.IOException;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * A proxy toward a some data stored on the disk.
 * 
 * @author Benjamin
 * 
 */
final class ValueProxy<K extends Comparable<K>, V> extends DataPointer<K, V> implements ValueWrapper<V> {

    /**
     * Creates a new proxy toward a value stored on disk.
     * 
     * @param manager The manager associated to this pointer.
     * @param position The position of the data within the file.
     * @param length The length of the data.
     */
    public ValueProxy(OnDiskNodeManager<K, V> manager, long position, int length) {

        super(manager, position, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V getValue() throws IOException {

        return getManager().loadValue(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).appendSuper(super.toString()).toString();
    }
}
