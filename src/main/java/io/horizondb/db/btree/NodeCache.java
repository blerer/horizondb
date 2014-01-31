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
package io.horizondb.db.btree;

import io.horizondb.db.metrics.CacheMetrics;
import io.horizondb.db.metrics.Monitorable;
import io.horizondb.db.metrics.PrefixFilter;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Cache used by the <code>OnDiskNodeManagers</code> to reduce the number of disk read 
 * 
 * @author Benjamin
 *
 */
public final class NodeCache<K extends Comparable<K>, V> implements Monitorable {

    /**
     * The cache used to reduce disk read.
     */
    private final Cache<NodeProxy<K, V>, Node<K, V>> cache;
    
    /**
     * The name of this cache.
     */
    private final String name;
    
    /**
     * @param name
     * @param cache
     */
    public NodeCache(String name, int cacheSize) {
        
        this.name = name;
        this.cache = CacheBuilder.newBuilder()
                                 .maximumSize(cacheSize)
                                 .recordStats()
                                 .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {
        registry.registerAll(new CacheMetrics(this.name, this.cache));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {
        registry.removeMatching(new PrefixFilter(getName()));
        
    }
    
    /**
     * Adds the specified node to the cache.
     * 
     * @param proxy the proxy associated to the specified node
     * @param node the node
     */
    void put(NodeProxy<K, V> proxy, Node<K, V> node) {
        this.cache.put(proxy, node);
    }
    
    /**
     * Returns the node associated to the specified proxy.
     * 
     * @param proxy the node proxy
     * @param reader the reader used to retrieve the node if it is not already in memory
     * @return the node associated to the specified proxy
     * @throws IOException if an I/O problem occurs.
     */
    Node<K, V> get(final NodeProxy<K, V> proxy, final NodeReader<K, V> reader) throws IOException {
        try {
            
            return this.cache.get(proxy, new Callable<Node<K,V>>() {
                
                /**
                 * {@inheritDoc}
                 */
                @Override
                public Node<K, V> call() throws Exception {
                    
                    return reader.readNode(proxy.getBTree(), proxy.getPosition());
                }
            });

        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }
    }
}
