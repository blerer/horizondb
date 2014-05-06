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

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import io.horizondb.model.core.Field;


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
}
