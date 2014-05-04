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
import java.util.SortedSet;

import static org.apache.commons.lang.Validate.notNull;

/**
 * Filter that will reject all the values that are not within a specified set.
 * 
 * @author Benjamin
 *
 */
final class InFilter<T extends Comparable<T>> implements Filter<T> {

    /**
     * The set of values within which the values must be.
     */
    private final SortedSet<T> set;
    
    /**
     * <code>true</code> if the value submitted to this filter are never decreasing.
     */
    private final boolean valuesNeverDecrease;
    
    /**
     * <code>true</code> if this filter will always return <code>false</code> in the future.
     */
    private boolean isDone;
            
    /**
     * Creates a new <code>InFilter</code> that accept values which are within the 
     * specified set.
     * 
     * @param set the set of values being accepted
     */
    public InFilter(SortedSet<T> set) {
        this(set, false);
    }
    
    /**
     * Creates a new <code>InFilter</code> that accept values which are within the 
     * specified set.
     * 
     * @param set the set of values being accepted
     * @param valuesNeverDecrease <code>true</code> if the value that will be used to 
     * as argument to the accept method will never decrease.
     */
    public InFilter(SortedSet<T> set, boolean valuesNeverDecrease) {
        
        notNull(set, "the set parameter must not be null.");
        
        this.set = set;
        this.valuesNeverDecrease = valuesNeverDecrease;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(T value) throws IOException {

        if (!this.set.contains(value)) {
        
            if (this.valuesNeverDecrease && this.set.last().compareTo(value) < 0) {
                
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
