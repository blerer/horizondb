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

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * <code>KeyValueIterator</code> used to merge the results returned by multiple iterators.
 * 
 * @author Benjamin
 *
 */
public final class MergingKeyValueIterator<K extends Comparable<K>, V> implements KeyValueIterator<K, V> {

    /**
     * The sorted map used to sort the values returned by the iterators.
     */
    private SortedMap<K, KeyValueIterator<K, V>> map = new TreeMap<K, KeyValueIterator<K, V>>();
    
    /**
     * The next key and value to be returned.
     */
    private KeyValueIterator<K, V> next;
    
    /**
     * Creates a new <code>MergingKeyValueIterator</code> the will merge the results of the specified iterators.
     * 
     * @param iterators the iterators
     * @throws IOException if an I/O problem occurs
     */
    @SafeVarargs
    public MergingKeyValueIterator(KeyValueIterator<K, V>... iterators) throws IOException {
        
        for (KeyValueIterator<K, V> iterator : iterators) {
            if (iterator.next()) {
                this.map.put(iterator.getKey(), iterator);
            }
        }
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public boolean next() throws IOException {
        
        if (this.next != null && this.next.next()) {
            this.map.put(this.next.getKey(), this.next);
        }
        
        if (this.map.isEmpty()) {
            return false;
        }
        
        K key = this.map.firstKey();
        this.next = this.map.remove(key);
        
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K getKey() {
        return this.next.getKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V getValue() throws IOException {
        return this.next.getValue();
    }
}
