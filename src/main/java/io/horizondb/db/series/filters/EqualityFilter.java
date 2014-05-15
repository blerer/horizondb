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

import static org.apache.commons.lang.Validate.notNull;

/**
 * Filter that will reject all the values that do not match a specific one.
 * 
 * @author Benjamin
 *
 */
final class EqualityFilter<T extends Comparable<T>> implements Filter<T> {

    /**
     * The expected value.
     */
    private final T expected;
    
    /**
     * <code>true</code> if the value submitted to this filter are never decreasing.
     */
    private final boolean valuesNeverDecrease;
    
    /**
     * <code>true</code> if this filter will always return <code>false</code> in the future.
     */
    private boolean isDone;
            
    /**
     * Creates a new <code>EqualityFilter</code> that accept values which are equals to the 
     * specified one.
     * 
     * @param expected the expected value
     */
    public EqualityFilter(T expected) {
        this(expected, false);
    }
    
    /**
     * Creates a new <code>EqualityFilter</code> that accept values which are equals to the 
     * specified one.
     * 
     * @param expected the expected value
     * @param valuesNeverDecrease <code>true</code> if the value that will be used  
     * as argument to the accept method will never decrease.
     */
    public EqualityFilter(T expected, boolean valuesNeverDecrease) {
        
        notNull(expected, "the expected parameter must not be null.");
        
        this.expected = expected;
        this.valuesNeverDecrease = valuesNeverDecrease;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(T value) throws IOException {

        if (!this.expected.equals(value)) {
        
            if (this.valuesNeverDecrease && this.expected.compareTo(value) < 0) {
                
                this.isDone = true;
            }
            
            return false;
        }
        
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDone() {
        return this.isDone;
    }
}
