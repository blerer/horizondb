/**
 * Copyright 2014 Benjamin Lerer
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
package io.horizondb.db.cache;

import java.io.IOException;

import io.horizondb.db.Component;
import io.horizondb.db.HorizonDBException;

/**
 * A caching component.
 * 
 * @param K the key type
 * @param V the value type
 * 
 * @author Benjamin
 */
public interface Cache<K, V> extends Component {

    /**
     * Returns the value associated to the specified key. If the key is not present in the cache the value will be 
     * loaded using the specified <code>ValueLoader</code>.  
     * 
     * @param key the value key
     * @param loader the loader that will be used to load the value if the key is not present within the cache
     * @return the value associated to the specified key
     * @throws IOException if an I/O problem occurs
     * @throws HorizonDBException if a problem occurs
     */
    V get(K key, ValueLoader<K, V> loader) throws IOException, HorizonDBException;
}
