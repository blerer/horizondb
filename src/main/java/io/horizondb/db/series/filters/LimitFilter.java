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

import java.io.IOException;

import io.horizondb.db.series.Filter;

/**
 * Decorating <code>Filter</code> that limit the number of results that is accepted 
 * by the decorated filter.
 * 
 * @author Benjamin
 */
final class LimitFilter<T> implements Filter<T> {

    /**
     * The offset.
     */
    private final long offset;
    
    /**
     * Maximum number of records to accept.
     */
    private final long records;
    
    /**
     * The decorated filter.
     */
    private final Filter<T> filter;

    /**
     * The number of record accepted so far.
     */
    private long counter;
        
    /**
     * Creates a <code>LimitFilter</code> that will limit the values accepted by 
     * the specified filter.
     * 
     * @param filter the decorated filter
     * @param records the number of results that should be accepted
     */
    public LimitFilter(Filter<T> filter, long records) {
        this(filter, 0, records);
    }
    
    /**
     * Creates a <code>LimitFilter</code> that will limit the values accepted by 
     * the specified filter.
     * 
     * @param filter the decorated filter
     * @param offset the results offset 
     * @param records the number of results that should be accepted
     */
    public LimitFilter(Filter<T> filter, long offset, long records) {
        this.filter = filter;
        this.offset = offset;
        this.records = records;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(T value) throws IOException {
        
        if (this.filter.accept(value)) {
            
            this.counter++;
            
            if (this.counter > this.offset && this.counter <= (this.offset + this.records)) {
             
                return true;
            }
        }
                
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDone() {
        return this.filter.isDone() || (this.counter > (this.offset + this.records));
    }
}
