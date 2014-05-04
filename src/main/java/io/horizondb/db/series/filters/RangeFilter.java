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

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import static org.apache.commons.lang.Validate.notNull;

/**
 * Filter that will reject all the values that are not within a specified range.
 * 
 * @author Benjamin
 *
 */
final class RangeFilter<T extends Comparable<T>> implements Filter<T> {

    /**
     * The range within which the values must be.
     */
    private final Range<T> range;
    
    /**
     * <code>true</code> if the value submitted to this filter are never decreasing.
     */
    private final boolean valuesNeverDecrease;
    
    /**
     * <code>true</code> if this filter will always return <code>false</code> in the future.
     */
    private boolean isDone;
            
    /**
     * Creates a new <code>RangeFilter</code> that accept values which are within the 
     * specified range.
     * 
     * @param range the range
     */
    public RangeFilter(Range<T> range) {
        this(range, false);
    }
    
    /**
     * Creates a new <code>RangeFilter</code> that accept values which are within the 
     * specified range.
     * 
     * @param range the range
     * @param valuesNeverDecrease <code>true</code> if the value that will be used to 
     * as argument to the accept method will never decrease.
     */
    public RangeFilter(Range<T> range, boolean valuesNeverDecrease) {
        
        notNull(range, "the range parameter must not be null.");
        
        this.range = range;
        this.valuesNeverDecrease = valuesNeverDecrease;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(T value) throws IOException {

        if (!this.range.contains(value)) {
        
            if (this.valuesNeverDecrease 
                    && ((this.range.upperBoundType() == BoundType.CLOSED && this.range.upperEndpoint().compareTo(value) < 0)
                    || (this.range.upperBoundType() == BoundType.OPEN && this.range.upperEndpoint().compareTo(value) <= 0))) {
                
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
