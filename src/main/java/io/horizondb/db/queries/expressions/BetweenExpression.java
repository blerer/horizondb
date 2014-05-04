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

import com.google.common.collect.RangeSet;

import io.horizondb.db.queries.Expression;
import io.horizondb.model.core.Field;

/**
 * A BETWEEN expression.
 * 
 * @author Benjamin
 *
 */
final class BetweenExpression implements Expression {

    /**
     * The name of the field.
     */
    private final String fieldName;
    
    /**
     * The minimum value of the closed range.
     */
    private final String min;
    
    /**
     * The maximum value of the closed range.
     */
    private final String max;
    
    /**
     * <code>true</code> if the expression is a NOT BETWEEN expression.
     */
    private final boolean notBetween;
         
    /**
     * Creates a new BETWEEN expression.
     * 
     * @param fieldName the name of the field
     * @param min the minimum value of the closed range
     * @param max the maximum value of the closed range
     */
    public BetweenExpression(String fieldName, String min, String max) {
        
        this(fieldName, min, max, false);
    }
    
    /**
     * Creates a new BETWEEN expression.
     * 
     * @param fieldName the name of the field
     * @param min the minimum value of the closed range
     * @param max the maximum value of the closed range
     * @param notBetween <code>true</code> if the expression is a NOT BETWEEN expression
     */
    public BetweenExpression(String fieldName, String min, String max, boolean notBetween) {
        
        this.fieldName = fieldName;
        this.min = min;
        this.max = max;
        this.notBetween = notBetween;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        
        StringBuilder builder = new StringBuilder().append(this.fieldName);
        
        if (this.notBetween) {
            builder.append(" NOT");
        }
        
        return builder.append(" BETWEEN ")
                      .append(this.min)
                      .append(" AND ")
                      .append(this.max)
                      .toString();
    }
}
