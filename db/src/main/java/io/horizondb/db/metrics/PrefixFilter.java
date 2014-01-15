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
package io.horizondb.db.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

/**
 * Filter that matches the filters which have a name that starts with the specified prefix.
 * 
 * @author Benjamin
 * 
 */
public final class PrefixFilter implements MetricFilter {

    /**
     * The prefix to match.
     */
    private final String prefix;

    /**
     * Creates a new <code>PrefixFilter</code> that matches the filters with a name starting with the specified prefix.
     * 
     * @param prefix the prefix.
     */
    public PrefixFilter(String prefix) {
        this.prefix = prefix;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean matches(String name, Metric metric) {
        return name.startsWith(this.prefix);
    }
}
