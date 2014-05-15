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
 * Base class for <code>Filter</code>s that perform a logical expression.
 * 
 * @author Benjamin
 *
 */
abstract class LogicalFilter<T> implements Filter<T> {
    
    /**
     * The left filter.
     */
    private final Filter<T> leftFilter;
    
    /**
     * The right filter.
     */
    private final Filter<T> rightFilter;
            
    /**
     * Creates a new <code>LogicalFilter</code>.
     * 
     * @param leftFilter the left filter
     * @param rightFilter the right filter
     */
    public LogicalFilter(Filter<T> leftFilter, 
                         Filter<T> rightFilter) {
        
        this.leftFilter = leftFilter;
        this.rightFilter = rightFilter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean accept(T value) throws IOException {
        return performOperation(this.leftFilter.accept(value), this.rightFilter.accept(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean isDone() {
        return performOperation(this.leftFilter.isDone(), this.rightFilter.isDone());
    }
    
    /**
     * Performs the logical operation between the two boolean.
     * 
     * @param left the left boolean
     * @param right the right boolean
     * @return the result of the logical operation between the two boolean.
     */
    protected abstract boolean performOperation(boolean left, boolean right);
}
