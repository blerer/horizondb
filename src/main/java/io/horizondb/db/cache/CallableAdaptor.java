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

import java.util.concurrent.Callable;

/**
 * <code>Adaptor</code> that adapts a <code>ValueLoader</code> to the <code>Callable</code> interface. 
 * 
 * @author Benjamin
 */
final class CallableAdaptor<K, V> implements Callable<V> {

    /**
     * The adapted <code>ValueLoader</code>.
     */
    private final ValueLoader<K, V> loader;
    
    /**
     * The key for which the value must be loaded.
     */
    private final K key;
    
    /**
     * Creates a new <code>CallableAdaptor</code> that adapt the specified loader.
     * 
     * @param key the key for which the value must be loaded
     * @param loader the loader to adapt
     */
    public CallableAdaptor(K key, ValueLoader<K, V> loader) {
        
        this.key = key;
        this.loader = loader;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public V call() throws Exception {
        return this.loader.loadValue(this.key);
    }   
}
