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
package io.horizondb.db.queries;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

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
        public <C extends Comparable<C>> Range<C> toRange(C value) {
            return Range.closed(value, value);
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
        public <C extends Comparable<C>> Range<C> toRange(C value) {
            return Range.upTo(value, BoundType.OPEN);
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
        public <C extends Comparable<C>> Range<C> toRange(C value) {
            return Range.upTo(value, BoundType.CLOSED);
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
        public <C extends Comparable<C>> Range<C> toRange(C value) {
            return Range.downTo(value, BoundType.OPEN);
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
        public <C extends Comparable<C>> Range<C> toRange(C value) {
            return Range.downTo(value, BoundType.CLOSED);
        }
    };
    
    /**
     * Returns the range corresponding to this operator with the specified value.
     * 
     * @param value the value to which the field will be compared.
     * @return the range corresponding to this operator with the specified value.
     */
    public abstract <C extends Comparable<C>> Range<C> toRange(C value);
    
    public static Operator fromSymbol(String symbol) {
        
        if ("=".equals(symbol)) {
            return EQ;
        }
        
        if (">".equals(symbol)) {
            return GT;
        }
        
        if (">=".equals(symbol)) {
            return GE;
        }
        
        if ("<".equals(symbol)) {
            return LT;
        }
        
        if ("<=".equals(symbol)) {
            return LE;
        }
        
        throw new IllegalArgumentException("Unknown operator: " + symbol);
    }
}
