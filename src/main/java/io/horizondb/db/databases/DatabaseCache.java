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
package io.horizondb.db.databases;

import io.horizondb.db.Configuration;
import io.horizondb.db.cache.AbstractCache;

import com.google.common.cache.CacheBuilder;

/**
 * A <code>Cache</code> for databases.
 * 
 * @author Benjamin
 * 
 */
final class DatabaseCache extends AbstractCache<String, Database> {

    /**
     * Creates a <code>DatabaseCache</code> to cache the databases.
     * 
     * @param configuration the database configuration.
     */
    public DatabaseCache(Configuration configuration) {

        super(configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CacheBuilder<Object, Object> newBuilder(Configuration configuration) {

        return CacheBuilder.newBuilder()
                .maximumSize(configuration.getDatabaseCacheMaximumSize())
                .recordStats();
    }
}
