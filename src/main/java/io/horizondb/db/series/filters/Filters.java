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

import java.util.SortedSet;

import com.google.common.collect.Range;

import io.horizondb.db.series.Filter;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 * Factory methods for filters 
 * 
 * @author Benjamin
 *
 */
public final class Filters {

    /**
     * The <code>NoopFilter</code> instance.
     */
    @SuppressWarnings("rawtypes")
    private static final Filter NOOP_FILTER_INSTANCE = new NoopFilter();
    
    /**
     * Converts the specified field filter into a record filter. 
     * 
     * @param definition the time series definition
     * @param fieldName the field name
     * @param filter the field filter to convert
     * @return a record filter that filter using the specified field filter
     */
    public static Filter<Record> toRecordFilter(TimeSeriesDefinition definition, 
                                                String fieldName, 
                                                Filter<Field> filter) {
        
        return new FieldRecordFilter(definition, fieldName, filter);
    }
    
    /**
     * Creates a filter that accept only fields that are equals to the specified one.
     * 
     * @param field the field to which the other fields must be compared
     * @param valuesNeverDecrease <code>true</code> if the value that will be used  
     * as argument to the accept method will never decrease.
     * @return a filter that accept only fields that are equals to the specified one.
     */
    public static Filter<Field> eq(Field field, boolean valuesNeverDecrease) {
        
        return new EqualityFilter<Field>(field, valuesNeverDecrease);
    }
    
    /**
     * Creates a filter that will reject all the field accepted by the specified filter.
     * 
     * @param filter the filter
     * @return a filter that will reject all the field accepted by the specified filter 
     */
    public static Filter<Field> not(Filter<Field> filter) {
        
        return new NotFilter<>(filter);
    }
        
    /**
     * Creates a filter that accept all the fields within the specified range.
     * 
     * @param range the accepted field range
     * @param valuesNeverDecrease <code>true</code> if the value that will be used  
     * as argument to the accept method will never decrease.
     * @return a filter that accept all the fields within the specified range.
     */
    public static Filter<Field> range(Range<Field> range, boolean valuesNeverDecrease) {
        
        return new RangeFilter<>(range, valuesNeverDecrease);
    }
    
    /**
     * Creates a filter that accept all the field within the specified set.
     * 
     * @param fields the set of fields that must be accepted.
     * @param valuesNeverDecrease <code>true</code> if the value that will be used  
     * as argument to the accept method will never decrease.
     * @return a filter that accept all the field within the specified set.
     */
    public static Filter<Field> in(SortedSet<Field> fields, boolean valuesNeverDecrease) {
    
        return new InFilter<>(fields, valuesNeverDecrease);
    }

    /**
     * Creates a filter that accept the values that are accepted by the two filters.
     * 
     * @param left the left filter
     * @param right the right filter
     * @return a filter that accept the values that are accepted by the two filters
     */
    public static <T> Filter<T> and(Filter<T> left, Filter<T> right) {
        
        return new AndFilter<>(left, right);
    }
    
    /**
     * Creates a filter that accept the values that are accepted by one of the two filters.
     * 
     * @param left the left filter
     * @param right the right filter
     * @return a filter that accept the values that are accepted by one of the two filters
     */
    public static <T> Filter<T> or(Filter<T> left, Filter<T> right) {
        
        return new OrFilter<>(left, right);
    }
    
    /**
     * Creates a filter that limit the number of values that are accepted by the specified filter. 
     * 
     * @param filter the filter to limit
     * @param offset the record offset
     * @param records the number of records that can be accepted
     * @return a filter that limit the number of values that are accepted by the specified filter
     */
    public static <T> Filter<T> limit(Filter<T> filter, long offset, long records) {
        
        return new LimitFilter<>(filter, offset, records);
    }
    
    /**
     * Returns a filter that accept everything.
     * 
     * @return a filter that accept everything.
     */
    @SuppressWarnings("unchecked")
    public static <T> Filter<T> noop() {
        return NOOP_FILTER_INSTANCE;
    }
    
    /**
     * Must not be instantiated.    
     */
    private Filters() {
        
    }
}
