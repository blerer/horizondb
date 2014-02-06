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

import io.horizondb.db.HorizonDBException;

import java.io.IOException;

/**
 * @author Benjamin
 *
 */
public interface ValueLoader<K, V> {

    /**
     * Loads the value associated to the specified key. 
     * 
     * @param key the cache key
     * @return the value associated to the specified key
     * @throws IOException if an I/O problem occurs
     * @throws HorizonDBException if another problem occurs
     */
    V loadValue(K key) throws IOException, HorizonDBException;
}
