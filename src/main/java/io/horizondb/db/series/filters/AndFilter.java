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

/**
 * <code>Filter</code> that perform an AND between two filters.
 * 
 * @author Benjamin
 *
 */
final class AndFilter<T> extends LogicalFilter<T> {
        
    /**
     * Creates a new <code>LogicalFilter</code>.
     * 
     * @param leftFilter the left filter
     * @param rightFilter the right filter
     */
    public AndFilter(Filter<T> leftFilter, 
                           Filter<T> rightFilter) {
        
        super(leftFilter, rightFilter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean performOperation(boolean left, boolean right) {
        return left && right;
    }
}
