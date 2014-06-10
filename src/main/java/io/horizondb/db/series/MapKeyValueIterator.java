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

import io.horizondb.db.btree.KeyValueIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * <code>KeyValueIterator</code> that iterate over the data of a <code>Map</code> in the key ascending order.
 * 
 * @author Benjamin
 *
 */
public final class MapKeyValueIterator<K extends Comparable<K>, V> implements KeyValueIterator<K, V> {

    /**
     * The iterator over the map entries.
     */
    private final Iterator<Entry<K, V>> iterator;
    
    /**
     * The current map entry.
     */
    private Entry<K, V> entry;
    
    /**
     * Creates a <code>MapKeyValueIterator</code> instance that will iterate over the entries of the specified 
     * <code>Map</code>.
     * 
     * @param map the key-value mapping on which the iterator will iterate in the key ascending order.
     */
    public MapKeyValueIterator(Map<K, V> map) {
        
        this.iterator = new TreeMap<>(map).entrySet().iterator();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean next() {
        
        if (this.iterator.hasNext()) {
            
            this.entry = this.iterator.next();
            return true;
        }
        
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K getKey() {
        return this.entry.getKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V getValue() {
        return this.entry.getValue();
    }
}
