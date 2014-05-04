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
package io.horizondb.db.series.filters;

import io.horizondb.db.series.Filter;

import java.io.IOException;

/**
 * A decorating filter that negate the decorated filter.
 * 
 * @author Benjamin
 *
 */
final class NotFilter<T extends Comparable<T>> implements Filter<T> {

    /**
     * The filter
     */
    private final Filter<T> filter;
    
    /**
     * Creates a new <code>NotFilter</code> that negate the specified filter.
     * 
     * @param filter the filter to negate
     */
    public NotFilter(Filter<T> filter) {
        this.filter = filter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(T value) throws IOException {
        return !this.filter.accept(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDone() {
        return false;
    }
}
