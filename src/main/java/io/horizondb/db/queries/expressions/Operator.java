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
package io.horizondb.db.queries.expressions;

import io.horizondb.db.series.Filter;
import io.horizondb.model.core.Field;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import static io.horizondb.db.series.filters.Filters.eq;
import static io.horizondb.db.series.filters.Filters.not;
import static io.horizondb.db.series.filters.Filters.range;


/**
 * The comparison operators.
 * 
 * @author Benjamin
 *
 */
public enum Operator {

    /**
     * The equal operator: '=' 
     */
    EQ {
        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "=";
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RangeSet<Field> getRangeSet(Field value) {
            return ImmutableRangeSet.of(Range.closed(value, value));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Filter<Field> getFilter(Field value, boolean timestamp) {
            return eq(value, timestamp);
        }
    },
    
    /**
     * The equal operator: '=' 
     */
    NE {
        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "=";
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public RangeSet<Field> getRangeSet(Field value) {
            return ImmutableRangeSet.<Field>builder()
                                    .add(Range.closedOpen(value.minValue(), value))
                                    .add(Range.openClosed(value, value.maxValue()))
                                    .build();
                                             
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Filter<Field> getFilter(Field value, boolean timestamp) {
            return not(eq(value, timestamp));
        }
    },
    
    /**
     * The less than operator: '<' 
     */
    LT {
        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "<";
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public RangeSet<Field> getRangeSet(Field value) {
            return ImmutableRangeSet.of(Range.closedOpen(value.minValue(), value));
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public Filter<Field> getFilter(Field value, boolean timestamp) {
            return range(Range.closedOpen(value.minValue(), value), timestamp);
        }
    },
    
    /**
     * The less than or equal operator: '<='
     */
    LE {
        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "<=";
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public RangeSet<Field> getRangeSet(Field value) {
            return ImmutableRangeSet.of(Range.closed(value.minValue(), value));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Filter<Field> getFilter(Field value, boolean timestamp) {
            return range(Range.closed(value.minValue(), value), timestamp);
        }
    },
    
    /**
     * The greater than operator: '>' 
     */
    GT {        
        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return ">";
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public RangeSet<Field> getRangeSet(Field value) {
            return ImmutableRangeSet.of(Range.openClosed(value, value.maxValue()));
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public Filter<Field> getFilter(Field value, boolean timestamp) {
            return range(Range.openClosed(value, value.maxValue()), timestamp);
        }
    },
    
    /**
     * The greater than or equal operator: '>=' 
     */
    GE {       
        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return ">=";
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public RangeSet<Field> getRangeSet(Field value) {
            return ImmutableRangeSet.of(Range.closed(value, value.maxValue()));
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public Filter<Field> getFilter(Field value, boolean timestamp) {
            return range(Range.closed(value, value.maxValue()), timestamp);
        }
    };
    
    /**
     * Returns the operator associated to the specified symbol.
     * 
     * @param symbol the symbol
     * @return the operator associated to the specified symbol.
     */
    public static Operator fromSymbol(String symbol) {
        
        for (Operator operator : Operator.values()) {
            
            if (operator.toString().equals(symbol)) {
                return operator;
            }
        }
                
        throw new IllegalArgumentException("Unknown operator: " + symbol);
    }

    /**
     * Returns the range corresponding to specified field for this operator.
     * 
     * @param field the field used to create the range.
     */
    public abstract RangeSet<Field> getRangeSet(Field value);
    
    /**
     * Returns the filter corresponding to this operator.
     * 
     * @param value the value
     * @param timestamp <code>true</code> if the field is the record timestamp
     * @return the filter corresponding to this operator.
     */
    public abstract Filter<Field> getFilter(Field value, boolean timestamp);
}
