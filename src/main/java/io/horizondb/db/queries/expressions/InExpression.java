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

import java.util.List;

import org.apache.commons.lang.text.StrBuilder;

import io.horizondb.db.queries.Expression;

/**
 * An IN expression.
 * 
 * @author Benjamin
 *
 */
final class InExpression implements Expression {

    /**
     * The name of the field that should be compared to the values.
     */
    private final String fieldName;
    
    /**
     * The values to which must be field must be compared.
     */
    private final List<String> values;
    
    /**
     * <code>true</code> if the operator is a NOT IN operator.
     */
    private final boolean notIn;
    
    /**
     * Creates an IN expression.
     * 
     * @param fieldName the name of the field
     * @param values the values
     */
    public InExpression(String fieldName, List<String> values) {
        
        this(fieldName, values, false);
    }

    /**
     * Creates an IN expression.
     * 
     * @param fieldName the name of the field
     * @param values the values
     * @param notIn <code>true</code> if the field value must not be within the specified values
     */
    public InExpression(String fieldName, List<String> values, boolean notIn) {
        
        this.fieldName = fieldName;
        this.values = values;
        this.notIn = notIn;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        
        StrBuilder builder = new StrBuilder().append(this.fieldName)
                                             .append(' ');
        
        if (this.notIn) {
            
            builder.append("NOT ");
        }
        
        return builder.append("IN (")
                      .appendWithSeparators(this.values, ", ")
                      .append(")")
                      .toString();
    }
}
