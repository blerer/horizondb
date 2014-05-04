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

import io.horizondb.db.queries.Expression;

/**
 * A logical expression
 * 
 * @author Benjamin
 *
 */
abstract class LogicalExpression implements Expression {

    /**
     * The expression on the left hand side of the operator.
     */
    private final Expression left;
    
    /**
     * The expression on the right hand side of the operator.
     */
    private final Expression right;

    /**
     * Creates a <code>LogicalExpression</code> 
     * 
     * @param left the expression on the left hand side of the operator.
     * @param right the expression on the right hand side of the operator.
     */
    public LogicalExpression(Expression left, Expression right) {
        
        this.left = left;
        this.right = right;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new StringBuilder().append(addParenthesesIfNeeded(this.left))
                                  .append(' ')
                                  .append(getOperatorAsString())
                                  .append(' ')
                                  .append(addParenthesesIfNeeded(this.right))
                                  .toString();
    }
    
    /**
     * Returns the operator as a <code>String</code>.
     * @return the operator as a <code>String</code>.
     */
    protected abstract String getOperatorAsString();
    
    /**
     * Adds parentheses around the specified expression if needed.  
     * 
     * @param expression the expression
     * @return the expression as a <code>String</code> between parentheses if needed or the expression as a 
     * <code>String</code>.
     */
    private static CharSequence addParenthesesIfNeeded(Expression expression) {
        
        if (expression instanceof OrExpression) {
            
            return new StringBuilder().append('(')
                                      .append(expression)
                                      .append(')');
        }
        
        return expression.toString();
    }
}
