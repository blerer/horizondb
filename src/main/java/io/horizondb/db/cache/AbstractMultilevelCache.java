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
package io.horizondb.db.cache;

import io.horizondb.db.Configuration;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Base class for <code>Cache</code> implementation with multilevel.
 * 
 * @author Benjamin
 * 
 */
public abstract class AbstractMultilevelCache<K, V> extends AbstractCache<K, V> {

    /**
     * The second level cache.
     */
    private final io.horizondb.db.cache.Cache<K, V> secondLevelCache;

    /**
     * Creates a <code>AbstractCache</code>.
     * 
     * @param configuration the database configuration.
     * @param secondLevelCache the second level cache.
     */
    public AbstractMultilevelCache(Configuration configuration, 
                                   io.horizondb.db.cache.Cache<K, V> secondLevelCache) {

        super(configuration);
        this.secondLevelCache = secondLevelCache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
            .appendSuper(super.toString())
            .append("secondLevelCache", this.secondLevelCache)
            .toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected V doGet(final K key, final ValueLoader<K, V> loader) throws ExecutionException {
        
        return doGet(key, new Callable<V>() {
            
            /**
             * {@inheritDoc}
             */
            @Override
            public V call() throws Exception {
                
                final V value = AbstractMultilevelCache.this.secondLevelCache.get(key, loader);
                
                AbstractMultilevelCache.this.afterLoad(value);
                
                return value;
            }
        });
    }
    
  /**
   * Allows a sub-classes to perform operations on a loaded value before returning it.
   *  
   * @param value the loaded value
   */
  protected void afterLoad(V value) {

  }
}
